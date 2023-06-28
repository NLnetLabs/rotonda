#[cfg(test)]
mod tests {
    use crate::{
        bgp::encode::{mk_bgp_update, Announcements, Prefixes},
        comms::Gate,
        payload::{Payload, Update},
        units::rib_unit::{
            metrics::RibUnitMetrics,
            rib::{PhysicalRib, RibValue},
            status_reporter::RibUnitStatusReporter,
            unit::RibUnitRunner,
        },
    };
    use arc_swap::{ArcSwap, ArcSwapOption};
    use roto::types::{
        builtin::{BuiltinTypeValue, RawRouteWithDeltas, RotondaId, RouteStatus, UpdateMessage},
        typevalue::TypeValue,
    };
    use rotonda_store::{
        custom_alloc::Upsert, epoch, prelude::multi::PrefixStoreError, MatchOptions, MatchType,
    };
    use routecore::{addr::Prefix, bgp::message::SessionConfig};

    use std::{str::FromStr, sync::Arc, time::Instant};

    #[tokio::test]
    async fn process_non_route_update() {
        let (_gate, rib, metrics) = test_init();

        // Given an update that is not a route
        let payload = Payload::TypeValue(TypeValue::Unknown);

        // When it is processed by this unit
        let update = run_process_single_update(payload.clone(), rib.clone(), metrics).await;

        // Then it should NOT be added to the route store
        assert_eq!(rib.load().as_ref().unwrap().prefixes_count(), 0);

        // But should be output for forwarding to downstream units
        assert!(matches!(
            update,
            Some(Update::Single(Payload::TypeValue(TypeValue::Unknown)))
        ));
    }

    #[tokio::test]
    async fn process_update_single_route() {
        let (_gate, rib, metrics) = test_init();

        // Given a BGP update containing a single route announcement
        let delta_id = (RotondaId(0), 0);
        let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32)
            .unwrap()
            .into();
        let announcements =
            Announcements::from_str("e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32")
                .unwrap();
        let bgp_update_bytes = mk_bgp_update(&Prefixes::default(), &announcements, &[]);

        // When it is processed by this unit
        let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        let route = RawRouteWithDeltas::new_with_message(
            delta_id,
            prefix,
            bgp_update_msg,
            RouteStatus::InConvergence,
        );
        let payload = Payload::TypeValue(BuiltinTypeValue::Route(route).into());
        let saved_input_payload = payload.clone();
        let gate_update = run_process_single_update(payload.clone(), rib.clone(), metrics).await;

        // Then it SHOULD be added to the route store
        assert_eq!(rib.load().as_ref().unwrap().prefixes_count(), 1);

        // And SHOULD be output for forwarding to downstream units
        assert!(matches!(&gate_update, Some(Update::Single(p)) if p == &payload));

        // And the BGP UPDATE bytes should be the same for both the input and output route
        // (at the time of writing the BgpUpdateMessage PartialEq implementation only compares BgpUpdateMessage.message_id,
        // not the actual underlying message bytes)
        let Some(Update::Single(output_payload)) = &gate_update else { unreachable!() };
        assert_eq!(
            get_payload_bytes(&saved_input_payload),
            get_payload_bytes(&output_payload)
        );
    }

    #[tokio::test]
    async fn process_update_same_route_twice() {
        let (_gate, rib, metrics) = test_init();

        // Given a BGP update containing a single route announcement
        let delta_id = (RotondaId(0), 0);
        let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32)
            .unwrap()
            .into();
        let announcements =
            Announcements::from_str("e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32")
                .unwrap();
        let bgp_update_bytes = mk_bgp_update(&Prefixes::default(), &announcements, &[]);

        // When it is processed by this unit
        let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        let route = RawRouteWithDeltas::new_with_message(
            delta_id,
            prefix,
            bgp_update_msg,
            RouteStatus::InConvergence,
        );
        let payload = Payload::TypeValue(BuiltinTypeValue::Route(route).into());
        let gate_update1 =
            run_process_single_update(payload.clone(), rib.clone(), metrics.clone()).await;
        let gate_update2 = run_process_single_update(payload.clone(), rib.clone(), metrics).await;

        // Then it SHOULD be added to the route store only once
        assert_eq!(rib.load().as_ref().unwrap().prefixes_count(), 1);

        // And SHOULD be output twice for forwarding to downstream units
        assert!(matches!(&gate_update1, Some(Update::Single(p)) if p == &payload));
        assert!(matches!(&gate_update2, Some(Update::Single(p)) if p == &payload));

        // And the BGP UPDATE bytes should be the same for both the input and output route
        // (at the time of writing the BgpUpdateMessage PartialEq implementation only compares BgpUpdateMessage.message_id,
        // not the actual underlying message bytes)
        let Some(Update::Single(output_payload)) = gate_update1 else { unreachable!() };
        assert_eq!(
            get_payload_bytes(&payload),
            get_payload_bytes(&output_payload)
        );
        let Some(Update::Single(output_payload)) = gate_update2 else { unreachable!() };
        assert_eq!(
            get_payload_bytes(&payload),
            get_payload_bytes(&output_payload)
        );
    }

    #[tokio::test]
    async fn process_update_two_routes_to_the_same_prefix() {
        let (_match_result, match_result2) = {
            let (_gate, rib, metrics) = test_init();

            // Given BGP updates for two different routes to the same prefix
            let delta_id = (RotondaId(0), 0);
            let raw_prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32).unwrap();
            let prefix = raw_prefix.into();
            let announcements1 =
                Announcements::from_str("e [111,222,333] 10.0.0.1 none 127.0.0.1/32").unwrap();
            let announcements2 =
                Announcements::from_str("e [111,444,333] 10.0.0.1 none 127.0.0.1/32").unwrap();
            let bgp_update_bytes1 = mk_bgp_update(&Prefixes::default(), &announcements1, &[]);
            let bgp_update_bytes2 = mk_bgp_update(&Prefixes::default(), &announcements2, &[]);

            // When they are processed by this unit
            let mut input_payloads = vec![];
            let mut output_payloads = vec![];
            for bgp_update_bytes in [bgp_update_bytes1, bgp_update_bytes2] {
                let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
                let route = RawRouteWithDeltas::new_with_message(
                    delta_id,
                    prefix,
                    bgp_update_msg,
                    RouteStatus::InConvergence,
                );
                let payload = Payload::TypeValue(BuiltinTypeValue::Route(route).into());
                input_payloads.push(payload.clone());
                let gate_update =
                    run_process_single_update(payload, rib.clone(), metrics.clone()).await;
                let Some(Update::Single(output_payload)) = gate_update else { unreachable!() };
                output_payloads.push(output_payload);
            }

            // Then only the one common prefix SHOULD be added to the route store
            assert_eq!(rib.load().as_ref().unwrap().prefixes_count(), 1);

            // And at that prefix there should be one RibValue containing two routes
            let match_options = MatchOptions {
                match_type: MatchType::ExactMatch,
                include_all_records: true,
                include_less_specifics: true,
                include_more_specifics: true,
            };
            eprintln!("Querying store match_prefix the first time");
            let match_result = rib.load().as_ref().unwrap().match_prefix(
                &raw_prefix,
                &match_options,
                &epoch::pin(),
            );
            assert!(matches!(match_result.match_type, MatchType::ExactMatch));
            let rib_value = match_result.prefix_meta.as_ref().unwrap();
            assert_eq!(rib_value.len(), 2);

            // Check the Arc reference counts. The routes HashSet should have a strong reference count of 2 because the
            // MultiThreadedStore has the original Arc around the metadata that was inserted into the store, and it clones
            // that Arc when making a "copy" to include in the match prefix result set, thereby incrementing the strong
            // reference count. The items in the routes HashSet however should have a strong reference count of 1. This is
            // because we are accessing the one and only copy of the HashSet via the Arc and are thus seeing the actual
            // HashSet copy held by the store and thus its actual items, which have not been cloned and thus have a strong
            // reference count of 1. As we don't at this point use any Weak references to the HashSet or its items the
            // weak reference counts should be 0.
            assert_eq!(Arc::strong_count(rib_value.test_inner()), 2);
            assert_eq!(Arc::weak_count(rib_value.test_inner()), 0);
            for item in rib_value.iter() {
                assert_eq!(Arc::strong_count(item), 1);
                assert_eq!(Arc::weak_count(item), 0);
            }

            // And SHOULD be output for forwarding to downstream units
            assert_eq!(output_payloads.len(), 2);

            // And the BGP UPDATE bytes should be the same for both the input and output route
            // (at the time of writing the BgpUpdateMessage PartialEq implementation only compares BgpUpdateMessage.message_id,
            // not the actual underlying message bytes)
            for (input_payload, output_payload) in input_payloads.iter().zip(output_payloads.iter())
            {
                assert_eq!(
                    get_payload_bytes(&input_payload),
                    get_payload_bytes(&output_payload)
                );
            }

            // If we repeat the match prefix query while still holding the previous match prefix query results, we should
            // see that the routes HashSet Arc strong reference count increases from 2 to 3 while the inner items of the
            // HashSet still have a strong reference count of 1.
            eprintln!("Querying store match_prefix the second time");
            let match_result2 = rib.load().as_ref().unwrap().match_prefix(
                &raw_prefix,
                &match_options,
                &epoch::pin(),
            );
            assert!(matches!(match_result2.match_type, MatchType::ExactMatch));
            let rib_value = match_result2.prefix_meta.as_ref().unwrap();
            assert_eq!(rib_value.len(), 2);
            assert_eq!(Arc::strong_count(rib_value.test_inner()), 3);
            assert_eq!(Arc::weak_count(rib_value.test_inner()), 0);
            for item in rib_value.iter() {
                assert_eq!(Arc::strong_count(&item), 1);
                assert_eq!(Arc::weak_count(&item), 0);
            }

            eprintln!("About to drop the store by making it go out of scope");
            if Arc::try_unwrap(rib).is_err() {
                // We can't call unwrap() because Rib doesn't implement Debug.
                unreachable!();
            };

            (match_result, match_result2)
        };

        // The MultiThreadedStore has been dropped so the HashSet strong reference count should decrease from 3 to 2.
        eprintln!("Checking the reference counts of the `match_result` query result var inner metadata item");
        let rib_value = match_result2.prefix_meta.unwrap();
        // assert_eq!(Arc::strong_count(&rib_value.per_prefix_items), 2); // TODO: MultiThreadedStore doesn't cleanup on drop...
        assert_eq!(Arc::weak_count(rib_value.test_inner()), 0);
        for item in rib_value.iter() {
            assert_eq!(Arc::strong_count(&item), 1);
            assert_eq!(Arc::weak_count(&item), 0);
        }
    }

    #[tokio::test]
    async fn process_update_two_routes_to_different_prefixes() {
        let (_gate, rib, metrics) = test_init();

        // Given BGP updates for two different routes to two different prefixes
        let delta_id = (RotondaId(0), 0);
        let raw_prefix1 = Prefix::new("127.0.0.1".parse().unwrap(), 32).unwrap();
        let raw_prefix2 = Prefix::new("127.0.0.2".parse().unwrap(), 32).unwrap();
        let prefix1 = raw_prefix1.into();
        let prefix2 = raw_prefix2.into();
        let announcements1 =
            Announcements::from_str("e [111,222,333] 10.0.0.1 none 127.0.0.1/32").unwrap();
        let announcements2 =
            Announcements::from_str("e [111,444,333] 10.0.0.1 none 127.0.0.2/32").unwrap();
        let bgp_update_bytes1 = mk_bgp_update(&Prefixes::default(), &announcements1, &[]);
        let bgp_update_bytes2 = mk_bgp_update(&Prefixes::default(), &announcements2, &[]);

        // When they are processed by this unit
        let mut input_payloads = vec![];
        let mut output_payloads = vec![];
        for (prefix, bgp_update_bytes) in
            [(prefix1, bgp_update_bytes1), (prefix2, bgp_update_bytes2)]
        {
            let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
            let route = RawRouteWithDeltas::new_with_message(
                delta_id,
                prefix,
                bgp_update_msg,
                RouteStatus::InConvergence,
            );
            let payload = Payload::TypeValue(BuiltinTypeValue::Route(route).into());
            input_payloads.push(payload.clone());
            let gate_update =
                run_process_single_update(payload, rib.clone(), metrics.clone()).await;
            let Some(Update::Single(output_payload)) = gate_update else { unreachable!() };
            output_payloads.push(output_payload);
        }

        // Then two separate prefixes SHOULD be added to the route store
        assert_eq!(rib.load().as_ref().unwrap().prefixes_count(), 2);

        // And at that prefix there should be two RibValues
        let match_options = MatchOptions {
            match_type: MatchType::ExactMatch,
            include_all_records: true,
            include_less_specifics: true,
            include_more_specifics: true,
        };

        for prefix in [raw_prefix1, raw_prefix2] {
            let match_result =
                rib.load()
                    .as_ref()
                    .unwrap()
                    .match_prefix(&prefix, &match_options, &epoch::pin());
            assert!(matches!(match_result.match_type, MatchType::ExactMatch));
            let rib_value = match_result.prefix_meta.unwrap(); // TODO: Why do we get the actual value out of the store here and not an Arc?
            assert_eq!(rib_value.len(), 1);
        }

        // And SHOULD be output for forwarding to downstream units
        assert_eq!(output_payloads.len(), 2);

        // And the BGP UPDATE bytes should be the same for both the input and output route
        // (at the time of writing the BgpUpdateMessage PartialEq implementation only compares BgpUpdateMessage.message_id,
        // not the actual underlying message bytes)
        for (input_payload, output_payload) in input_payloads.iter().zip(output_payloads.iter()) {
            assert_eq!(
                get_payload_bytes(&input_payload),
                get_payload_bytes(&output_payload)
            );
        }
    }

    // --- Test helpers ------------------------------------------------------

    fn test_init() -> (
        Arc<Gate>,
        Arc<ArcSwapOption<PhysicalRib>>,
        Arc<RibUnitMetrics>,
    ) {
        let (gate, _gate_agent) = Gate::new(0);
        let gate = Arc::new(gate);
        let physical_rib = PhysicalRib::new("test-rib");

        let rib = Arc::new(ArcSwapOption::from_pointee(physical_rib));
        let metrics = Arc::new(RibUnitMetrics::new(&gate));
        (gate, rib, metrics)
    }

    fn get_payload_bytes(payload: &Payload) -> Option<&[u8]> {
        if let Payload::TypeValue(TypeValue::Builtin(BuiltinTypeValue::Route(route))) = payload {
            Some(route.raw_message.raw_message().0.as_ref())
        } else {
            None
        }
    }

    async fn run_process_single_update(
        payload: Payload,
        rib: Arc<ArcSwapOption<PhysicalRib>>,
        metrics: Arc<RibUnitMetrics>,
    ) -> Option<Update> {
        let status_reporter = RibUnitStatusReporter::new("test metrics", metrics.clone());
        let status_reporter = Arc::new(status_reporter);

        fn insert(
            prefix: &Prefix,
            rib_value: RibValue,
            store: &PhysicalRib,
        ) -> Result<(Upsert, u32), PrefixStoreError> {
            eprintln!("Inserting {prefix} into the store");
            let res = store.insert(prefix, rib_value);
            eprintln!("Insert complete");
            res
        }

        let roto_source = Arc::new(ArcSwap::from_pointee((Instant::now(), String::new())));

        RibUnitRunner::process_update_single(
            payload,
            rib.clone(),
            insert,
            roto_source,
            status_reporter,
        )
        .await
    }
}
