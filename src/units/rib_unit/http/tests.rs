use std::{str::FromStr, sync::Arc};

use arc_swap::{ArcSwap, ArcSwapOption};
use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
use hyper::{body::Bytes, Body, Request, StatusCode};
use roto::types::builtin::{BgpUpdateMessage, RawRouteWithDeltas, RotondaId, RouteStatus};
use routecore::{
    asn::Asn,
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
            rib::{PhysicalRib, PreHashedTypeValue},
            unit::{MoreSpecifics, QueryLimits},
        },
        RibType,
    },
};

use super::PrefixesApi;

#[tokio::test]
async fn error_with_garbage_prefix() {
    let rib = mk_rib();
    assert_query_eq(
        rib.clone(),
        "/prefixes/not_a_valid_prefix",
        StatusCode::BAD_REQUEST,
        None,
    )
    .await;
}

#[tokio::test]
async fn error_with_prefix_host_bits_set_ipv4() {
    let rib = mk_rib();
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/24", // that should have been 1.2.3.4/32 or 1.2.3.0/24
        StatusCode::BAD_REQUEST,
        None,
    )
    .await;
}

#[tokio::test]
async fn no_match_when_rib_is_empty() {
    let rib = mk_rib();

    assert_query_eq(
        rib,
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
    let rib = mk_rib();

    insert_announcement(rib.clone(), "1.2.3.4/32", 1);

    assert_query_eq(
        rib,
        "/prefixes/1.2.3.4/32",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.4/32", 1)],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn exact_match_with_more_and_less_specifics_ipv4() {
    let rib = mk_rib();

    insert_announcement(rib.clone(), "1.2.0.0/16", 1);
    insert_announcement(rib.clone(), "1.2.3.0/24", 2);
    insert_announcement(rib.clone(), "1.2.3.4/32", 3);

    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.0/24",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.0/24", 2)],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.0/24?include=lessSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.0/24", 2)],
            "included": {
                "lessSpecifics": [mk_response_announced_prefix("1.2.0.0/16", 1)]
            }
        })),
    )
    .await;

    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.0/24?include=moreSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.0/24", 2)],
            "included": {
                "moreSpecifics": [mk_response_announced_prefix("1.2.3.4/32", 3)]
            }
        })),
    )
    .await;

    assert_query_eq(
        rib,
        "/prefixes/1.2.3.0/24?include=lessSpecifics,moreSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("1.2.3.0/24", 2)],
            "included": {
                "lessSpecifics": [mk_response_announced_prefix("1.2.0.0/16", 1)],
                "moreSpecifics": [mk_response_announced_prefix("1.2.3.4/32", 3)]
            }
        })),
    )
    .await;
}

#[tokio::test]
async fn issue_79_exact_match() {
    let rib = mk_rib();

    insert_announcement(rib.clone(), "8.8.8.0/24", 1);
    insert_announcement(rib.clone(), "8.0.0.0/12", 2);
    insert_announcement(rib.clone(), "8.0.0.0/9", 3);

    assert_query_eq(
        rib.clone(),
        "/prefixes/8.8.8.0/24",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("8.8.8.0/24", 1)],
            "included": {}
        })),
    )
    .await;

    assert_query_eq(
        rib.clone(),
        "/prefixes/8.8.8.8/32?include=lessSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [],
            "included": {
                "lessSpecifics": [
                    mk_response_announced_prefix("8.8.8.0/24", 1),
                    mk_response_announced_prefix("8.0.0.0/12", 2),
                    mk_response_announced_prefix("8.0.0.0/9", 3)
                ]
            }
        })),
    )
    .await;
}

#[tokio::test]
async fn prefix_normalization_ipv6() {
    let rib = mk_rib();

    insert_announcement(rib.clone(), "2001:DB8:2222::/48", 1);
    insert_announcement(rib.clone(), "2001:DB8:2222:0000::/64", 2);
    insert_announcement(rib.clone(), "2001:DB8:2222:0001::/64", 3);

    assert_query_eq(
        rib,
        "/prefixes/2001:DB8:2222::/48?include=moreSpecifics",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix("2001:db8:2222::/48", 1)], // note: DB8 was queried but db8 was reported
            "included": {
                "moreSpecifics": [
                    mk_response_announced_prefix("2001:db8:2222::/64", 2),   // note: 2222:0000:: compressed to 2222::
                    mk_response_announced_prefix("2001:db8:2222:1::/64", 3)  // note: 2222:0001:: compressed to 2222:1::
                ]
            }
        })),
    )
    .await;
}

#[tokio::test]
async fn error_with_too_short_more_specifics_ipv4() {
    let rib = mk_rib();

    insert_announcement(rib.clone(), "128.168.0.0/16", 1);
    insert_announcement(rib.clone(), "128.168.20.16/28", 2);

    // Test with a prefix whose first bit is the only bit set. 0x128 is 10000000 in binary. If we were to test
    // with any pattern of bits with a bit set further to the right, as the /N mask value increases the boundary
    // between net bits and host bits shifts so that the number of net bits decreases and the number of host bits
    // increases, to the point where the bit set in the query becomes treated as a host bit and thus causes the
    // query to fail with HTTP 400 Bad Request: The host portion of the address has non-zero bits set.
    for n in MoreSpecifics::default_shortest_prefix_ipv4()..=32 {
        assert_query_eq(
            rib.clone(),
            &format!("/prefixes/128.0.0.0/{}?include=moreSpecifics", n),
            StatusCode::OK,
            None,
        )
        .await;
    }

    for n in 0..MoreSpecifics::default_shortest_prefix_ipv4() {
        assert_query_eq(
            rib.clone(),
            &format!("/prefixes/128.0.0.0/{}?include=moreSpecifics", n),
            StatusCode::BAD_REQUEST,
            None,
        )
        .await;
    }
}

#[tokio::test]
async fn error_with_too_short_more_specifics_ipv6() {
    let rib = mk_rib();

    insert_announcement(rib.clone(), "2001:DB8:2222::/48", 1);
    insert_announcement(rib.clone(), "2001:DB8:2222:0000::/64", 2);
    insert_announcement(rib.clone(), "2001:DB8:2222:0001::/64", 3);

    // Test with a prefix whose first bit is the only bit set. 0x8000 is 1000000000000000 in binary. If we were to
    // test with any pattern of bits with a bit set further to the right, as the /N mask value increases the
    // boundary between net bits and host bits shifts so that the number of net bits decreases and the number of
    // host bits increases, to the point where the bit set in the query becomes treated as a host bit and thus
    // causes the query to fail with HTTP 400 Bad Request: The host portion of the address has non-zero bits set.
    for n in MoreSpecifics::default_shortest_prefix_ipv6()..=128 {
        assert_query_eq(
            rib.clone(),
            &format!("/prefixes/8000::/{}?include=moreSpecifics", n),
            StatusCode::OK,
            None,
        )
        .await;
    }

    for n in 0..MoreSpecifics::default_shortest_prefix_ipv6() {
        assert_query_eq(
            rib.clone(),
            &format!("/prefixes/8000::/{}?include=moreSpecifics", n),
            StatusCode::BAD_REQUEST,
            None,
        )
        .await;
    }
}

#[tokio::test]
async fn exact_match_ipv4_with_normal_communities() {
    let rib = mk_rib();

    #[rustfmt::skip]
    let normal_communities: Vec<StandardCommunity> = vec![
        sample_reserved_standard_community(),
        sample_private_community(),
        well_known_rfc1997_no_export_community(),
        well_known_rfc7999_blackhole_community(),
        [0xFF, 0xFF, 0xFF, 0x05].into()
    ];

    insert_announcement_full(
        rib.clone(),
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
        rib,
        "/prefixes/1.2.3.4/32?details=communities",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix_full("1.2.3.4/32", 1, &[18], Some(expected_communities))],//, "Pre")],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn exact_match_ipv4_with_extended_communities() {
    let rib = mk_rib();

    #[rustfmt::skip]
    let extended_communities: Vec<ExtendedCommunity> = vec![
        sample_as2_specific_route_target_extended_community(),
        sample_ipv4_address_specific_route_target_extended_community(),
        sample_unrecgonised_extended_community(),
    ];

    insert_announcement_full(
        rib.clone(),
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
        rib,
        "/prefixes/1.2.3.4/32?details=communities",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix_full("1.2.3.4/32", 1, &[18], Some(expected_communities))],//, "Pre")],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn exact_match_ipv4_with_large_communities() {
    let rib = mk_rib();

    insert_announcement_full(
        rib.clone(),
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
        rib,
        "/prefixes/1.2.3.4/32?details=communities",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix_full("1.2.3.4/32", 1, &[18], Some(expected_communities))],//, "Pre")],
            "included": {}
        })),
    )
    .await;
}

#[tokio::test]
async fn select_and_discard() {
    fn insert_announcement_helper<C: Into<Community>>(
        rib: Arc<ArcSwapOption<PhysicalRib>>,
        router_n: u8,
        as_path: &[u32],
        community: Option<C>,
    ) {
        insert_announcement_full(
            rib,
            "1.2.3.4/32",
            router_n,
            as_path,
            "127.0.0.1",
            &community.map_or(vec![], |v| vec![v.into()]),
        );
    }

    // Populate the RIB
    let rib = mk_rib();

    insert_announcement_helper(rib.clone(), 1, &[19, 20, 21], Some(Wellknown::Blackhole));
    insert_announcement_helper(
        rib.clone(),
        2,
        &[19, 20, 21],
        Option::<StandardCommunity>::None,
    );
    insert_announcement_helper(
        rib.clone(),
        2,
        &[20, 21, 22],
        Option::<StandardCommunity>::None,
    );
    insert_announcement_helper(rib.clone(), 2, &[1, 2, 3], Some(Wellknown::NoExport));
    insert_announcement_helper(rib.clone(), 3, &[21, 22, 23], Some(Wellknown::Blackhole));

    let blackhole_json = json!([
        {
            "rawFields": ["0xFFFF029A"],
            "type": "standard",
            "parsed": {
                "value": { "type": "well-known", "attribute": "BLACKHOLE" }
            }
        }
    ]);

    let no_export_json = json!([
        {
            "rawFields": ["0xFFFFFF01"],
            "type": "standard",
            "parsed": {
                "value": { "type": "well-known", "attribute": "NO_EXPORT" }
            }
        }
    ]);

    // Query the RIB

    // -- Select by AS PATH
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?select[as_path]=19,20,21&sort=/route/router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix_full("1.2.3.4/32", 1, &[19, 20, 21], Some(blackhole_json.clone())),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[19, 20, 21], None),
            ],
            "included": {}
        })),
    )
    .await;

    // -- Select by peer AS, using a secondary sort to make the order stable for our test
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?select[peer_as]=1002&sort=/route/router_id,/route/as_path",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[1, 2, 3], Some(no_export_json.clone())),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[19, 20, 21], None),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[20, 21, 22], None),
            ],
            "included": {}
        })),
    )
    .await;

    // -- Select by community
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?select[community]=BLACKHOLE&sort=/route/router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix_full("1.2.3.4/32", 1, &[19, 20, 21], Some(blackhole_json.clone())),
                mk_response_announced_prefix_full("1.2.3.4/32", 3, &[21, 22, 23], Some(blackhole_json.clone())),
            ],
            "included": {}
        })),
    )
    .await;

    // -- Discard by AS path
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?discard[as_path]=19,20,21&sort=/route/as_path",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[1, 2, 3], Some(no_export_json.clone())),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[20, 21, 22], None),
                mk_response_announced_prefix_full("1.2.3.4/32", 3, &[21, 22, 23], Some(blackhole_json.clone())),
            ],
            "included": {}
        })),
    )
    .await;

    // -- Discard by either of two AS paths.
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?discard[as_path]=19,20,21&discard[as_path]=1,2,3&filter_op=any&sort=/route/router_id",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[20, 21, 22], None),
                mk_response_announced_prefix_full("1.2.3.4/32", 3, &[21, 22, 23], Some(blackhole_json.clone())),
            ],
            "included": {}
        })),
    )
    .await;

    // -- Discard by both community and AS path
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?discard[community]=BLACKHOLE&discard[as_path]=19,20,21&filter_op=all&sort=/route/as_path",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[1, 2, 3], Some(no_export_json.clone())),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[19, 20, 21], None),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[20, 21, 22], None),
                mk_response_announced_prefix_full("1.2.3.4/32", 3, &[21, 22, 23], Some(blackhole_json.clone())),
            ],
            "included": {}
        })),
    )
    .await;

    // -- Select by peer AS AND discard if BOTH given communities match
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?select[peer_as]=1002&discard[community]=BLACKHOLE&discard[community]=NO_EXPORT&filter_op=all&sort=/route/as_path",
        StatusCode::OK,
        Some(json!({
            "data": [
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[1, 2, 3], Some(no_export_json.clone())),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[19, 20, 21], None),
                mk_response_announced_prefix_full("1.2.3.4/32", 2, &[20, 21, 22], None),
            ],
            "included": {}
        })),
    )
    .await;

    // -- Select by AS path, or discard if either given community matches
    // -- Discard overrides select
    assert_query_eq(
        rib.clone(),
        "/prefixes/1.2.3.4/32?select[as_path]=19,20,21&discard[community]=BLACKHOLE&discard[community]=NO_EXPORT&sort=/route/router_id",
        StatusCode::OK,
        Some(json!({
            "data": [mk_response_announced_prefix_full("1.2.3.4/32", 2, &[19, 20, 21], None)],
            "included": {}
        })),
    )
    .await;

    // The default filter operation should be "any"
    for query_extra in &["&filter_op=any", ""] {
        let query = format!(
            "/prefixes/1.2.3.4/32?select[community]=NO_EXPORT&select[peer_as]=1001{}&sort=/route/router_id",
            query_extra
        );

        assert_query_eq(
            rib.clone(),
            &query,
            StatusCode::OK,
            Some(json!({
                "data": [
                    mk_response_announced_prefix_full("1.2.3.4/32", 1, &[19, 20, 21], Some(blackhole_json.clone())),
                    mk_response_announced_prefix_full("1.2.3.4/32", 2, &[1, 2, 3], Some(no_export_json.clone())),
                ],
                "included": {}
            })),
        )
        .await;
    }
}

/// --- Helper functions ------------------------------------------------------------------------------------------

async fn do_query(
    rib: Arc<ArcSwapOption<PhysicalRib>>,
    query: &str,
    expected_status_code: StatusCode,
    expected_body: Option<Value>,
) -> Result<Bytes, (String, Bytes, StatusCode)> {
    let http_api_path = Arc::new("/prefixes/".to_string());
    let query_limits = Arc::new(ArcSwap::from_pointee(QueryLimits::default()));
    let proc = PrefixesApi::new(
        rib,
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
            Err(err) => {
                let err = format!(
                    "JSON difference detected (lhs=actual, rhs=expected): {}",
                    err
                );
                Err((err, bytes, actual_status_code))
            }
        }
    } else {
        Ok(bytes)
    }
}

async fn assert_query_eq(
    rib: Arc<ArcSwapOption<PhysicalRib>>,
    query: &str,
    expected_status_code: StatusCode,
    expected_body: Option<Value>,
) {
    match do_query(rib, query, expected_status_code, expected_body).await {
        Ok(_bytes) => {
            // if expected_status_code == 200 {
            //     assert_valid_against_schema(&bytes);
            // }
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

fn mk_rib() -> Arc<ArcSwapOption<PhysicalRib>> {
    let physical_rib = PhysicalRib::default();
    Arc::new(ArcSwapOption::from_pointee(physical_rib))
}

fn insert_routes(rib: Arc<ArcSwapOption<PhysicalRib>>, n: u8, announcements: Announcements) {
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
        let loaded_rib = rib.load();
        let rib = loaded_rib.as_ref().unwrap();

        for prefix in prefixes.iter() {
            let roto_update_msg = roto::types::builtin::UpdateMessage::new(
                bgp_update_bytes.clone(),
                SessionConfig::modern(),
            );
            let bgp_update_msg = Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
            let raw_route = RawRouteWithDeltas::new_with_message_ref(
                delta_id,
                (*prefix).into(),
                &bgp_update_msg,
                RouteStatus::InConvergence,
            )
            .with_peer_ip(format!("192.168.0.{n}").parse().unwrap())
            .with_peer_asn(Asn::from_u32(1000 + (n as u32)))
            .with_router_id(Arc::new(format!("router{n}")));

            rib.insert(&prefix, raw_route).unwrap();
        }
    }
}

fn insert_announcement(rib: Arc<ArcSwapOption<PhysicalRib>>, prefix: &str, n: u8) {
    insert_announcement_full(
        rib,
        prefix,
        n,
        &[123, 456],
        "127.0.0.1",
        &[] as &[Community],
    );
}

fn insert_announcement_full<C: Into<Community> + Copy>(
    rib: Arc<ArcSwapOption<PhysicalRib>>,
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
        rib,
        n,
        Announcements::from_str(&format!("e {} {next_hop} {communities} {prefix}", as_path))
            .unwrap(),
    );
}

fn mk_response_announced_prefix(prefix: &str, router_n: u8) -> Value {
    mk_response_announced_prefix_full(prefix, router_n, &[123, 456], None)
}

fn mk_response_announced_prefix_full(
    prefix: &str,
    router_n: u8,
    as_path: &[u32],
    communities: Option<Value>,
) -> Value {
    let mut route_object = json!({
        "prefix": prefix,
        "as_path": as_path.iter().map(|asn| format!("AS{}", asn)).collect::<Vec<String>>(),
        "next_hop": {
            "Ipv4": "127.0.0.1",
        },
        "atomic_aggregate": false,
        "origin_type": "Egp",
        "peer_ip": format!("192.168.0.{router_n}"),
        "peer_asn": 1000 + (router_n as u32),
        "router_id": format!("router{}", router_n),
    });

    if let Some(communities) = communities {
        route_object.insert("communities", communities);
    }

    let prefix_object = json!({
        "route": route_object,
        "status": "InConvergence",
        "route_id": [ 0, 0 ] // TODO: remove this unused field from the underlying data structures
    });

    prefix_object
}

// fn assert_valid_against_schema(response: &[u8]) {
//     use jsonschema::{Draft, JSONSchema};

//     const SCHEMA_BYTES: &[u8; 18174] = include_bytes!("../../../../doc/api/prefixes.schema.json");

//     let schema = serde_json::from_slice(SCHEMA_BYTES).unwrap();
//     let instance = serde_json::from_slice(response).unwrap();
//     let compiled = JSONSchema::options()
//         .with_draft(Draft::Draft202012)
//         .compile(&schema)
//         .unwrap();
//     let result = compiled.validate(&instance);
//     if let Err(errors) = result {
//         eprintln!("JSON validation failure:");
//         for error in errors {
//             eprintln!("Validation error: {}", error);
//             eprintln!("Instance path: {}", error.instance_path);
//         }
//         panic!("JSON validation failure");
//     }
// }

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
