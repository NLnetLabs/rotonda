use std::{ops::Deref, str::FromStr, sync::Arc};

use arc_swap::ArcSwap;
use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
use hyper::{body::Bytes, Body, Request, StatusCode};
use roto::types::builtin::{RawRouteWithDeltas, RotondaId, RouteStatus};
use rotonda_store::MultiThreadedStore;
use routecore::{
    addr::Prefix,
    bgp::{
        communities::{
            Community, ExtendedCommunity, LargeCommunity, ParseError, StandardCommunity, Wellknown,
        },
        message::SessionConfig,
    },
};
use serde_json::{json, Value};

use crate::{
    bgp::{
        encode::{mk_bgp_update, Announcements, Prefixes},
        raw::communities::{extended::*, large::*, standard::*},
    },
    common::{frim::FrimMap, json::EasilyExtendedJSONObject},
    http::ProcessRequest,
    units::{
        rib_unit::{
            rib_value::{RibValue, RouteWithUserDefinedHash},
            unit::{MoreSpecifics, QueryLimits},
        },
        RibType,
    },
};

use super::PrefixesApi;

#[tokio::test]
async fn error_with_garbage_prefix() {
    let store = mk_store();
    assert_query_eq(
        store.clone(),
        "/prefixes/not_a_valid_prefix",
        StatusCode::BAD_REQUEST,
        None,
    )
    .await;
}

#[tokio::test]
async fn error_with_prefix_host_bits_set_ipv4() {
    let store = mk_store();
    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/24", // that should have been 1.2.3.4/32 or 1.2.3.0/24
        StatusCode::BAD_REQUEST,
        None,
    )
    .await;
}

#[tokio::test]
async fn no_match_when_store_is_empty() {
    let store = mk_store();

    assert_query_eq(
        store,
        "/prefixes/1.2.3.4/32",
        StatusCode::OK,
        Some(json!({
            "data": [],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn exact_match_ipv4() {
    let store = mk_store();

    insert_withdrawal(store.clone(), "1.2.3.4/32", 1);

    assert_query_eq(
        store,
        "/prefixes/1.2.3.4/32",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_withdrawn_prefix("1.2.3.4/32", 1)],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn exact_match_with_more_and_less_specifics_ipv4() {
    let store = mk_store();

    insert_withdrawal(store.clone(), "1.2.0.0/16", 1);
    insert_withdrawal(store.clone(), "1.2.3.0/24", 2);
    insert_withdrawal(store.clone(), "1.2.3.4/32", 3);

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.0/24",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_withdrawn_prefix("1.2.3.0/24", 2)],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.0/24?include=lessSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_withdrawn_prefix("1.2.3.0/24", 2)],
            "included": {
                "lessSpecifics": [mk_response_withdrawn_prefix("1.2.0.0/16", 1)]
            }
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.0/24?include=moreSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_withdrawn_prefix("1.2.3.0/24", 2)],
            "included": {
                "moreSpecifics": [mk_response_withdrawn_prefix("1.2.3.4/32", 3)]
            }
        })),
    )
    .await;

    assert_query_eq(
        store,
        "/prefixes/1.2.3.0/24?include=lessSpecifics,moreSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_withdrawn_prefix("1.2.3.0/24", 2)],
            "included": {
                "lessSpecifics": [mk_response_withdrawn_prefix("1.2.0.0/16", 1)],
                "moreSpecifics": [mk_response_withdrawn_prefix("1.2.3.4/32", 3)]
            }
        })),
    )
    .await;
}

#[tokio::test]
async fn issue_79_exact_match() {
    let store = mk_store();

    insert_withdrawal(store.clone(), "8.8.8.0/24", 1);
    insert_withdrawal(store.clone(), "8.0.0.0/12", 2);
    insert_withdrawal(store.clone(), "8.0.0.0/9", 3);

    assert_query_eq(
        store.clone(),
        "/prefixes/8.8.8.0/24",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_withdrawn_prefix("8.8.8.0/24", 1)],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/8.8.8.8/32?include=lessSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [],
            "included": {
                "lessSpecifics": [
                    mk_response_withdrawn_prefix("8.8.8.0/24", 1),
                    mk_response_withdrawn_prefix("8.0.0.0/12", 2),
                    mk_response_withdrawn_prefix("8.0.0.0/9", 3)
                ]
            }
        })),
    )
    .await;
}

#[tokio::test]
async fn prefix_normalization_ipv6() {
    let store = mk_store();

    insert_withdrawal(store.clone(), "2001:DB8:2222::/48", 1);
    insert_withdrawal(store.clone(), "2001:DB8:2222:0000::/64", 2);
    insert_withdrawal(store.clone(), "2001:DB8:2222:0001::/64", 3);

    assert_query_eq(
        store,
        "/prefixes/2001:DB8:2222::/48?include=moreSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_withdrawn_prefix("2001:db8:2222::/48", 1)], // note: DB8 was queried but db8 was reported
            "included": {
                "moreSpecifics": [
                    mk_response_withdrawn_prefix("2001:db8:2222::/64", 2),   // note: 2222:0000:: compressed to 2222::
                    mk_response_withdrawn_prefix("2001:db8:2222:1::/64", 3)  // note: 2222:0001:: compressed to 2222:1::
                ]
            }
        })),
    )
    .await;
}

#[tokio::test]
async fn error_with_too_short_more_specifics_ipv4() {
    let store = mk_store();

    insert_withdrawal(store.clone(), "128.168.0.0/16", 1);
    insert_withdrawal(store.clone(), "128.168.20.16/28", 2);

    // Test with a prefix whose first bit is the only bit set. 0x128 is 10000000 in binary. If we were to test
    // with any pattern of bits with a bit set further to the right, as the /N mask value increases the boundary
    // between net bits and host bits shifts so that the number of net bits decreases and the number of host bits
    // increases, to the point where the bit set in the query becomes treated as a host bit and thus causes the
    // query to fail with HTTP 400 Bad Request: The host portion of the address has non-zero bits set.
    for n in MoreSpecifics::default_shortest_prefix_ipv4()..=32 {
        assert_query_eq(
            store.clone(),
            &format!("/prefixes/128.0.0.0/{}?include=moreSpecifics", n),
            StatusCode::OK,
            None,
        )
        .await;
    }

    for n in 0..MoreSpecifics::default_shortest_prefix_ipv4() {
        assert_query_eq(
            store.clone(),
            &format!("/prefixes/128.0.0.0/{}?include=moreSpecifics", n),
            StatusCode::BAD_REQUEST,
            None,
        )
        .await;
    }
}

#[tokio::test]
async fn error_with_too_short_more_specifics_ipv6() {
    let store = mk_store();

    insert_withdrawal(store.clone(), "2001:DB8:2222::/48", 1);
    insert_withdrawal(store.clone(), "2001:DB8:2222:0000::/64", 2);
    insert_withdrawal(store.clone(), "2001:DB8:2222:0001::/64", 3);

    // Test with a prefix whose first bit is the only bit set. 0x8000 is 1000000000000000 in binary. If we were to
    // test with any pattern of bits with a bit set further to the right, as the /N mask value increases the
    // boundary between net bits and host bits shifts so that the number of net bits decreases and the number of
    // host bits increases, to the point where the bit set in the query becomes treated as a host bit and thus
    // causes the query to fail with HTTP 400 Bad Request: The host portion of the address has non-zero bits set.
    for n in MoreSpecifics::default_shortest_prefix_ipv6()..=128 {
        assert_query_eq(
            store.clone(),
            &format!("/prefixes/8000::/{}?include=moreSpecifics", n),
            StatusCode::OK,
            None,
        )
        .await;
    }

    for n in 0..MoreSpecifics::default_shortest_prefix_ipv6() {
        assert_query_eq(
            store.clone(),
            &format!("/prefixes/8000::/{}?include=moreSpecifics", n),
            StatusCode::BAD_REQUEST,
            None,
        )
        .await;
    }
}

#[tokio::test]
async fn exact_match_ipv4_with_normal_communities() {
    let store = mk_store();

    #[rustfmt::skip]
    let normal_communities: Vec<StandardCommunity> = vec![
        sample_reserved_standard_community(),
        sample_private_community(),
        well_known_rfc1997_no_export_community(),
        well_known_rfc7999_blackhole_community(),
        [0xFF, 0xFF, 0xFF, 0x05].into()
    ];

    insert_announcement(
        store.clone(),
        "1.2.3.4/32",
        1,
        &[18],
        "127.0.0.1",
        &normal_communities, // PrePolicy,
    );

    let expected_communities = json!([
        {
            "rawFields": ["0x00000000"],
            "type": "standard",
            "parsed": {
                "value": { "type": "reserved" }
            }
        },
        {
            "rawFields": ["0x0001", "0x0000"],
            "type": "standard",
            "parsed": {
                "value": { "type": "private", "asn": "AS1", "tag": 0 }
            }
        },
        {
            "rawFields": ["0xFFFFFF01"],
            "type": "standard",
            "parsed": {
                "value": { "type": "well-known", "attribute": "NO_EXPORT" }
            }
        },
        {
            "rawFields": ["0xFFFF029A"],
            "type": "standard",
            "parsed": {
                "value": { "type": "well-known", "attribute": "BLACKHOLE" }
            }
        },
        {
            "rawFields": ["0xFFFFFF05"],
            "type": "standard",
            "parsed": {
                "value": { "type": "well-known-unrecognised" }
            }
        }
    ]);

    assert_query_eq(
        store,
        "/prefixes/1.2.3.4/32?details=communities",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.4/32", 1, &[18], Some(expected_communities), "Pre")],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn exact_match_ipv4_with_extended_communities() {
    let store = mk_store();

    #[rustfmt::skip]
    let extended_communities: Vec<ExtendedCommunity> = vec![
        sample_as2_specific_route_target_extended_community(),
        sample_ipv4_address_specific_route_target_extended_community(),
        sample_unrecgonised_extended_community(),
    ];

    insert_announcement(
        store.clone(),
        "1.2.3.4/32",
        1,
        &[18],
        "127.0.0.1",
        &extended_communities,
        // PrePolicy,
    );

    let expected_communities = json!([
        {
            "rawFields": ["0x00", "0x02", "0x0022", "0x0000D508"],
            "type": "extended",
            "parsed": {
                "type": "as2-specific",
                "rfc7153SubType": "route-target",
                "transitive": true,
                "globalAdmin": { "type": "asn", "value": "AS34" },
                "localAdmin": 54536
            }
        },
        {
            "rawFields": ["0x01", "0x02", "0xC12A000A", "0xD508"],
            "type": "extended",
            "parsed": {
              "type": "ipv4-address-specific",
              "rfc7153SubType": "route-target",
              "transitive": true,
              "globalAdmin": { "type": "ipv4-address", "value": "193.42.0.10" },
              "localAdmin": 54536
            }
        },
        {
            "rawFields": ["0x02", "0x02", "0x00", "0x22", "0x00", "0x00", "0xD5", "0x08"],
            "type": "extended",
            "parsed": {
              "type": "unrecognised",
              "transitive": true,
            }
        }
    ]);

    assert_query_eq(
        store,
        "/prefixes/1.2.3.4/32?details=communities",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.4/32", 1, &[18], Some(expected_communities), "Pre")],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn exact_match_ipv4_with_large_communities() {
    let store = mk_store();

    insert_announcement(
        store.clone(),
        "1.2.3.4/32",
        1,
        &[18],
        "127.0.0.1",
        &[sample_large_community()],
        // PrePolicy,
    );

    let expected_communities = json!([
        {
            "type": "large",
            "rawFields": ["0x0022", "0x0100", "0x0200"],
            "parsed": {
              "globalAdmin": { "type": "asn", "value": "AS34" },
              "localDataPart1": 256,
              "localDataPart2": 512
            }
        }
    ]);

    assert_query_eq(
        store,
        "/prefixes/1.2.3.4/32?details=communities",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.4/32", 1, &[18], Some(expected_communities), "Pre")],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn select_and_discard() {
    fn insert_announcement_helper<C: Into<Community>>(
        store: Arc<Option<MultiThreadedStore<RibValue>>>,
        router_n: u8,
        as_path: &[u32],
        community: Option<C>,
        // rib_name: RoutingInformationBase,
    ) {
        insert_announcement(
            store,
            "1.2.3.4/32",
            router_n,
            as_path,
            "127.0.0.1",
            &community.map_or(vec![], |v| vec![v.into()]),
        );
    }

    let store = mk_store();

    insert_announcement_helper(
        store.clone(),
        1,
        &[18, 19, 20],
        Option::<StandardCommunity>::None,
    ); //, PrePolicy);
    insert_announcement_helper(
        store.clone(),
        2,
        &[19, 20, 21],
        Some(Wellknown::Blackhole),
        // PostPolicy,
    );
    insert_announcement_helper(
        store.clone(),
        3,
        &[19, 20, 21],
        Some(Wellknown::NoExport),
        // PrePolicy,
    );
    insert_announcement_helper(
        store.clone(),
        4,
        &[20, 21, 22],
        Some(Wellknown::NoAdvertise),
        // PostPolicy,
    );

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?select[as_path]=19,20,21&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix("1.2.3.4/32", 2, &[19, 20, 21], None, "Post"),
                mk_response_announced_prefix("1.2.3.4/32", 3, &[19, 20, 21], None, "Pre")
            ],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?select[routing_information_base_name]=Pre-Policy-RIB&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix("1.2.3.4/32", 1, &[18, 19, 20], None, "Pre"),
                mk_response_announced_prefix("1.2.3.4/32", 3, &[19, 20, 21], None, "Pre")
            ],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?select[routing_information_base_name]=Post-Policy-RIB&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix("1.2.3.4/32", 2, &[19, 20, 21], None, "Post"),
                mk_response_announced_prefix("1.2.3.4/32", 4, &[20, 21, 22], None, "Post")
            ],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?select[source_as]=10003&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.4/32", 3, &[19, 20, 21], None, "Pre")],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?select[community]=BLACKHOLE&select[as_path]=19,20,21&filter_op=all",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.4/32", 2, &[19, 20, 21], None, "Post")],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?discard[as_path]=19,20,21&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix("1.2.3.4/32", 1, &[18, 19, 20], None, "Pre"),
                mk_response_announced_prefix("1.2.3.4/32", 4, &[20, 21, 22], None, "Post")
            ],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?discard[as_path]=18,19,20&discard[as_path]=19,20,21&filter_op=any&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.4/32", 4, &[20, 21, 22], None, "Post")],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?discard[community]=BLACKHOLE&discard[as_path]=19,20,21&filter_op=all&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix("1.2.3.4/32", 1, &[18, 19, 20], None, "Pre"),
                mk_response_announced_prefix("1.2.3.4/32", 3, &[19, 20, 21], None, "Pre"),
                mk_response_announced_prefix("1.2.3.4/32", 4, &[20, 21, 22], None, "Post")
            ],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?select[as_path]=19,20,21&discard[community]=BLACKHOLE&discard[community]=NO_EXPORT&filter_op=all&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix("1.2.3.4/32", 2, &[19, 20, 21], None, "Post"),
                mk_response_announced_prefix("1.2.3.4/32", 3, &[19, 20, 21], None, "Pre")
            ],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        store.clone(),
        "/prefixes/1.2.3.4/32?select[as_path]=19,20,21&discard[community]=BLACKHOLE&discard[community]=NO_EXPORT&sort=router_id",
        StatusCode::OK,
        Some(json!({
            "data": [],
            "included": {}
        })),
    )
    .await;

    // The default filter operation should be "any"
    for query_extra in &["&filter_op=any", ""] {
        let query = format!(
            "/prefixes/1.2.3.4/32?select[community]=NO_ADVERTISE&select[as_path]=19,20,21{}&sort=router_id",
            query_extra
        );

        assert_query_eq(
            store.clone(),
            &query,
            StatusCode::OK,
            Some(json!({
                "data": [
                    mk_response_announced_prefix("1.2.3.4/32", 2, &[19, 20, 21], None, "Post"),
                    mk_response_announced_prefix("1.2.3.4/32", 3, &[19, 20, 21], None, "Pre"),
                    mk_response_announced_prefix("1.2.3.4/32", 4, &[20, 21, 22], None, "Post")
                ],
                "included": {}
            })),
        )
        .await;
    }
}

/// --- Helper functions ------------------------------------------------------------------------------------------

async fn do_query(
    store: Arc<Option<MultiThreadedStore<RibValue>>>,
    query: &str,
    expected_status_code: StatusCode,
    expected_body: Option<Value>,
) -> Result<Bytes, (String, Bytes, StatusCode)> {
    let http_api_path = Arc::new("/prefixes/".to_string());
    let query_limits = Arc::new(ArcSwap::from_pointee(QueryLimits::default()));
    let proc = PrefixesApi::new(
        store,
        http_api_path,
        query_limits,
        RibType::Physical,
        None,
        Arc::new(FrimMap::default()),
    );

    let request = Request::builder()
        .method("GET")
        .uri(&format!("https://localhost:8080{}", query))
        .body(Body::empty())
        .unwrap();

    let res = proc.process_request(&request).await;
    if res.is_none() {
        let err = "The HTTP processor did not accept the request";
        return Err((err.to_string(), Bytes::default(), StatusCode::default()));
    }

    let res = res.unwrap();
    let actual_status_code = res.status();
    let bytes = hyper::body::to_bytes(res.into_body()).await.unwrap();

    if actual_status_code != expected_status_code {
        let err = format!(
            "Expected status code {} but received {}",
            expected_status_code, actual_status_code
        );
        return Err((err, bytes, actual_status_code));
    }

    if let Some(expected_body) = expected_body {
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        let config = Config::new(CompareMode::Strict);
        match assert_json_matches_no_panic(&v, &expected_body, config) {
            Ok(_) => Ok(bytes),
            Err(err) => Err((err, bytes, actual_status_code)),
        }
    } else {
        Ok(bytes)
    }
}

async fn assert_query_eq(
    store: Arc<Option<MultiThreadedStore<RibValue>>>,
    query: &str,
    expected_status_code: StatusCode,
    expected_body: Option<Value>,
) {
    match do_query(store, query, expected_status_code, expected_body).await {
        Ok(bytes) => {
            if expected_status_code == 200 {
                assert_valid_against_schema(&bytes);
            }
        }

        Err((err, bytes, status_code)) => {
            eprintln!(
                "Query: {}\nResponse Code: {}\nResponse Body: {}",
                query,
                status_code,
                std::str::from_utf8(&bytes).unwrap()
            );
            panic!("{}", err);
        }
    }
}

fn mk_store() -> Arc<Option<MultiThreadedStore<RibValue>>> {
    Arc::new(Some(MultiThreadedStore::<RibValue>::new().unwrap()))
}

fn insert_routes(store: Arc<Option<MultiThreadedStore<RibValue>>>, announcements: Announcements) {
    let bgp_update_bytes = mk_bgp_update(&Prefixes::default(), &announcements, &[]);
    let delta_id = (RotondaId(0), 0); // TODO
    if let Announcements::Some {
        origin: _,
        as_path: _,
        next_hop: _,
        communities: _,
        prefixes,
    } = announcements
    {
        for prefix in prefixes.iter() {
            let roto_update_msg = roto::types::builtin::UpdateMessage::new(
                bgp_update_bytes.clone(),
                SessionConfig::modern(),
            );
            let raw_route = RawRouteWithDeltas::new_with_message(
                delta_id,
                (*prefix).into(),
                roto_update_msg,
                RouteStatus::InConvergence,
            );
            let rib_value = RouteWithUserDefinedHash::new(raw_route, 1).into();
            store
                .deref()
                .as_ref()
                .unwrap()
                .insert(&prefix, rib_value)
                .unwrap();
        }
    }
}

fn insert_withdrawal(store: Arc<Option<MultiThreadedStore<RibValue>>>, withdrawals: &str, n: u8) {
    let prefixes = Prefixes::from_str(withdrawals).unwrap();
    let bgp_update_bytes = mk_bgp_update(&prefixes, &Announcements::None, &[]);
    let delta_id = (RotondaId(0), 0); // TODO

    for prefix in prefixes.iter() {
        let roto_update_msg = roto::types::builtin::UpdateMessage::new(
            bgp_update_bytes.clone(),
            SessionConfig::modern_addpath(),
        );
        let raw_route = RawRouteWithDeltas::new_with_message(
            delta_id,
            (*prefix).into(),
            roto_update_msg,
            RouteStatus::Withdrawn,
        );
        let rib_value = RouteWithUserDefinedHash::new(raw_route, 1).into();
        store
            .deref()
            .as_ref()
            .unwrap()
            .insert(&prefix, rib_value)
            .unwrap();
    }
}

fn insert_announcement<C: Into<Community> + Copy>(
    store: Arc<Option<MultiThreadedStore<RibValue>>>,
    prefix: &str,
    n: u8,
    as_path: &[u32],
    next_hop: &str,
    communities: &[C],
) {
    let communities = if communities.is_empty() {
        "none".to_string()
    } else {
        communities
            .iter()
            .map(|&c| {
                let c: Community = c.into();
                format!("{c}")
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    let as_path = format!(
        "[{}]",
        as_path
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    insert_routes(
        store,
        Announcements::from_str(&format!("e {} {next_hop} {communities} {prefix}", as_path))
            .unwrap(),
    );
}

fn mk_response_withdrawn_prefix(prefix: &str, n: u8) -> Value {
    json!({
        "prefix": prefix,
        "router": format!("router{}", n),
        "sourceAs": format!("AS1000{}", n),
        "routingInformationBaseName": "Pre-Policy-RIB",
        "asPath": [],
        "neighbor": format!("1.1.1.{}", n)
    })
}

fn mk_response_announced_prefix(
    prefix: &str,
    router_n: u8,
    as_path: &[u32],
    communities: Option<Value>,
    rib_name: &str,
) -> Value {
    let mut prefix_object = json!({
        "prefix": prefix,
        "router": format!("router{}", router_n),
        "sourceAs": format!("AS1000{}", router_n),
        "routingInformationBaseName": format!("{}-Policy-RIB", rib_name),
        "asPath": as_path.iter().map(|asn| format!("AS{}", asn)).collect::<Vec<String>>(),
        "neighbor": format!("1.1.1.{}", router_n)
    });

    if let Some(communities) = communities {
        prefix_object.insert("communities", communities);
    }

    prefix_object
}

fn assert_valid_against_schema(response: &[u8]) {
    // use jsonschema::{Draft, JSONSchema};

    // const SCHEMA_BYTES: &[u8; 18174] = include_bytes!("../../../../doc/api/prefixes.schema.json");

    // let schema = serde_json::from_slice(SCHEMA_BYTES).unwrap();
    // let instance = serde_json::from_slice(response).unwrap();
    // let compiled = JSONSchema::options()
    //     .with_draft(Draft::Draft202012)
    //     .compile(&schema)
    //     .unwrap();
    // let result = compiled.validate(&instance);
    // if let Err(errors) = result {
    //     eprintln!("JSON validation failure:");
    //     for error in errors {
    //         eprintln!("Validation error: {}", error);
    //         eprintln!("Instance path: {}", error.instance_path);
    //     }
    //     panic!("JSON validation failure");
    // }
    todo!()
}

#[derive(Debug, Default)]
struct RawCommunityTuple(
    Vec<StandardCommunity>,
    Vec<ExtendedCommunity>,
    Vec<LargeCommunity>,
);

impl FromStr for RawCommunityTuple {
    type Err = ParseError;

    fn from_str(communities: &str) -> Result<Self, Self::Err> {
        let mut normal = vec![];
        let mut extended = vec![];
        let mut large = vec![];

        for community in communities.split(',') {
            let community = Community::from_str(community)?;
            match community {
                Community::Standard(c) => normal.push(c),
                Community::Extended(c) => extended.push(c),
                Community::Ipv6Extended(_) => { /* TODO */ }
                Community::Large(c) => large.push(c),
            };
        }

        Ok(RawCommunityTuple(normal, extended, large))
    }
}

impl RawCommunityTuple {
    pub fn as_tuple(
        self,
    ) -> (
        Vec<StandardCommunity>,
        Vec<ExtendedCommunity>,
        Vec<LargeCommunity>,
    ) {
        (self.0, self.1, self.2)
    }
}
