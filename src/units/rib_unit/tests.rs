use crate::units::RibType;
use crate::{
    bgp::encode::{mk_bgp_update, Announcements, Prefixes},
    payload::{Payload, Update},
    units::rib_unit::unit::RibUnitRunner,
};
use roto::types::{
    builtin::{
        BgpUpdateMessage, BuiltinTypeValue, RawRouteWithDeltas, RotondaId,
        RouteStatus, UpdateMessage,
    },
    typevalue::TypeValue,
};
use rotonda_store::{epoch, MatchOptions, MatchType};
use routecore::{addr::Prefix, bgp::message::SessionConfig};

use std::sync::atomic::Ordering;
use std::{str::FromStr, sync::Arc};

#[tokio::test]
async fn process_non_route_update() {
    let (runner, _) = RibUnitRunner::mock("", RibType::Physical);

    // Given an update that is not a route
    let update = Update::from(Payload::from(TypeValue::Unknown));

    // When it is processed by this unit it should not be filtered
    assert!(!is_filtered(&runner, update).await);

    // And it should NOT be added to the route store
    assert_eq!(runner.rib().store().unwrap().prefixes_count(), 0);
}

#[tokio::test]
async fn process_update_single_route() {
    let (runner, _) = RibUnitRunner::mock("", RibType::Physical);

    // Given a BGP update containing a single route announcement
    let delta_id = (RotondaId(0), 0);
    let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32)
        .unwrap()
        .into();
    let announcements = Announcements::from_str(
        "e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32",
    )
    .unwrap();
    let bgp_update_bytes =
        mk_bgp_update(&Prefixes::default(), &announcements, &[]);

    let roto_update_msg =
        UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
    let bgp_update_msg =
        Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
    let route = RawRouteWithDeltas::new_with_message_ref(
        delta_id,
        prefix,
        &bgp_update_msg,
        RouteStatus::InConvergence,
    );
    let update = Update::from(Payload::from(TypeValue::from(
        BuiltinTypeValue::Route(route),
    )));

    // When it is processed by this unit it should not be filtered
    assert!(!is_filtered(&runner, update).await);

    // And it should be added to the route store
    assert_eq!(runner.rib().store().unwrap().prefixes_count(), 1);
}

#[tokio::test]
async fn process_update_same_route_twice() {
    let (runner, _) = RibUnitRunner::mock("", RibType::Physical);

    // Given a BGP update containing a single route announcement
    let delta_id = (RotondaId(0), 0);
    let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32)
        .unwrap()
        .into();
    let announcements = Announcements::from_str(
        "e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32",
    )
    .unwrap();
    let bgp_update_bytes =
        mk_bgp_update(&Prefixes::default(), &announcements, &[]);

    let roto_update_msg =
        UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
    let bgp_update_msg =
        Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
    let route = RawRouteWithDeltas::new_with_message_ref(
        delta_id,
        prefix,
        &bgp_update_msg,
        RouteStatus::InConvergence,
    );
    let update = Update::from(Payload::from(TypeValue::from(
        BuiltinTypeValue::Route(route),
    )));

    // When it is processed by this unit it should not be filtered
    assert!(!is_filtered(&runner, update.clone()).await);

    // And it should be added to the route store
    assert_eq!(runner.rib().store().unwrap().prefixes_count(), 1);

    // When it is processed by this unit again it should not be filtered
    assert!(!is_filtered(&runner, update).await);

    // And it should NOT be added again to the route store
    assert_eq!(runner.rib().store().unwrap().prefixes_count(), 1);
}

#[tokio::test]
async fn process_update_two_routes_to_the_same_prefix() {
    #[rustfmt::skip]
    let (_match_result, match_result2) = {
        let (runner, _) = RibUnitRunner::mock("", RibType::Physical);

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
        for bgp_update_bytes in [bgp_update_bytes1, bgp_update_bytes2] {
            let roto_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
            let bgp_update_msg = Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
            let route = RawRouteWithDeltas::new_with_message_ref(
                delta_id,
                prefix,
                &bgp_update_msg,
                RouteStatus::InConvergence,
            );

            // When it is processed by this unit it should not be filtered
            let update = Update::from(Payload::from(TypeValue::from(BuiltinTypeValue::Route(
                route,
            ))));
            assert!(!is_filtered(&runner, update.clone()).await);
        }

        // Then only the one common prefix SHOULD be added to the route store
        assert_eq!(runner.rib().store().unwrap().prefixes_count(), 1);

        // And at that prefix there should be one RibValue containing two routes
        let match_options = MatchOptions {
            match_type: MatchType::ExactMatch,
            include_all_records: true,
            include_less_specifics: true,
            include_more_specifics: true,
        };
        eprintln!("Querying store match_prefix the first time");
        let match_result = runner.rib().store().unwrap().match_prefix(
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

        // If we repeat the match prefix query while still holding the previous match prefix query results, we should
        // see that the routes HashSet Arc strong reference count increases from 2 to 3 while the inner items of the
        // HashSet still have a strong reference count of 1.
        eprintln!("Querying store match_prefix the second time");
        let match_result2 = runner.rib().store().unwrap().match_prefix(
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
            assert_eq!(Arc::strong_count(item), 1);
            assert_eq!(Arc::weak_count(item), 0);
        }

        (match_result, match_result2)
    };

    // The MultiThreadedStore has been dropped so the HashSet strong reference count should decrease from 3 to 2.
    eprintln!(
        "Checking the reference counts of the `match_result` query result var inner metadata item"
    );
    let rib_value = match_result2.prefix_meta.unwrap();
    // assert_eq!(Arc::strong_count(&rib_value.per_prefix_items), 2); // TODO: MultiThreadedStore doesn't cleanup on drop...
    assert_eq!(Arc::weak_count(rib_value.test_inner()), 0);
    for item in rib_value.iter() {
        assert_eq!(Arc::strong_count(item), 1);
        assert_eq!(Arc::weak_count(item), 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn process_update_two_routes_to_different_prefixes() {
    let (runner, _) = RibUnitRunner::mock("", RibType::Physical);

    // Given BGP updates for two different routes to two different prefixes
    let delta_id = (RotondaId(0), 0);
    let raw_prefix1 = Prefix::new("127.0.0.1".parse().unwrap(), 32).unwrap();
    let raw_prefix2 = Prefix::new("127.0.0.2".parse().unwrap(), 32).unwrap();
    let prefix1 = raw_prefix1.into();
    let prefix2 = raw_prefix2.into();
    let announcements1 =
        Announcements::from_str("e [111,222,333] 10.0.0.1 none 127.0.0.1/32")
            .unwrap();
    let announcements2 =
        Announcements::from_str("e [111,444,333] 10.0.0.1 none 127.0.0.2/32")
            .unwrap();
    let bgp_update_bytes1 =
        mk_bgp_update(&Prefixes::default(), &announcements1, &[]);
    let bgp_update_bytes2 =
        mk_bgp_update(&Prefixes::default(), &announcements2, &[]);

    // When they are processed by this unit
    for (prefix, bgp_update_bytes) in
        [(prefix1, bgp_update_bytes1), (prefix2, bgp_update_bytes2)]
    {
        let roto_update_msg =
            UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        let bgp_update_msg =
            Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
        let route = RawRouteWithDeltas::new_with_message_ref(
            delta_id,
            prefix,
            &bgp_update_msg,
            RouteStatus::InConvergence,
        );

        // When it is processed by this unit it should not be filtered
        let update = Update::from(Payload::from(TypeValue::from(
            BuiltinTypeValue::Route(route),
        )));
        assert!(!is_filtered(&runner, update.clone()).await);
    }

    // Then two separate prefixes SHOULD be added to the route store
    assert_eq!(runner.rib().store().unwrap().prefixes_count(), 2);

    // And at that prefix there should be two RibValues
    let match_options = MatchOptions {
        match_type: MatchType::ExactMatch,
        include_all_records: true,
        include_less_specifics: true,
        include_more_specifics: true,
    };

    for prefix in [raw_prefix1, raw_prefix2] {
        let match_result = runner.rib().store().unwrap().match_prefix(
            &prefix,
            &match_options,
            &epoch::pin(),
        );
        assert!(matches!(match_result.match_type, MatchType::ExactMatch));
        let rib_value = match_result.prefix_meta.unwrap(); // TODO: Why do we get the actual value out of the store here and not an Arc?
        assert_eq!(rib_value.len(), 1);
    }
}

// --- Test helpers ------------------------------------------------------

async fn is_filtered(runner: &RibUnitRunner, update: Update) -> bool {
    runner
        .process_update(update, |pfx, meta, store| store.insert(pfx, meta))
        .await
        .unwrap();
    let gate_metrics = runner.gate().metrics();
    let num_dropped_updates =
        gate_metrics.num_dropped_updates.load(Ordering::SeqCst);
    let num_updates = gate_metrics.num_updates.load(Ordering::SeqCst);
    num_dropped_updates == 0 && num_updates == 0
}
