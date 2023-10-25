use std::{cmp::Ordering, sync::Arc};

use hyper::{Body, Response};

use roto::types::{
    builtin::BuiltinTypeValue, collections::ElementTypeValue,
    typevalue::TypeValue,
};
use rotonda_store::prelude::Prefix;

use routecore::{
    asn::Asn,
    bgp::{aspath::Hop, communities::Community},
};
use serde_json::{json, Value};

use crate::{
    common::json::EasilyExtendedJSONObject,
    units::rib_unit::rib::{PreHashedTypeValue, RibValue},
};

use super::{
    types::{
        Details, Filter, FilterKind, FilterOp, Filters, Includes, SortKey,
    },
    PrefixesApi,
};

impl PrefixesApi {
    pub fn mk_json_response(
        res: rotonda_store::QueryResult<RibValue>,
        includes: Includes,
        details_cfg: Details,
        filters_cfg: Filters,
        sort_cfg: SortKey,
    ) -> Response<Body> {
        let mut out_prefixes = Vec::new();
        let mut out_less_specifics = Vec::new();
        let mut out_more_specifics = Vec::new();

        if let Some(prefix) = res.prefix {
            if let Some(routes_per_router) = res.prefix_meta {
                Self::prefixes_as_json(
                    &prefix,
                    &routes_per_router,
                    &details_cfg,
                    &filters_cfg,
                    &sort_cfg,
                    &mut out_prefixes,
                );
            }
        }

        if includes.less_specifics {
            if let Some(less_specifics) = res.less_specifics {
                for record in less_specifics.iter() {
                    Self::prefixes_as_json(
                        &record.prefix,
                        &record.meta,
                        &details_cfg,
                        &filters_cfg,
                        &sort_cfg,
                        &mut out_less_specifics,
                    );
                }
            }
        }

        if includes.more_specifics {
            if let Some(more_specifics) = res.more_specifics {
                for record in more_specifics.iter() {
                    Self::prefixes_as_json(
                        &record.prefix,
                        &record.meta,
                        &details_cfg,
                        &filters_cfg,
                        &sort_cfg,
                        &mut out_more_specifics,
                    );
                }
            }
        }

        let mut out_included = json!({});
        if includes.less_specifics {
            out_included.insert("lessSpecifics", json!(out_less_specifics));
        }
        if includes.more_specifics {
            out_included.insert("moreSpecifics", json!(out_more_specifics));
        }

        let response = json!({
            "data": out_prefixes,
            "included": out_included,
        });

        Response::builder()
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string_pretty(&response).unwrap(),
            ))
            .unwrap()
    }

    fn prefixes_as_json(
        query_prefix: &Prefix,
        rib_value: &RibValue,
        details_cfg: &Details,
        filter_cfg: &Filters,
        sort_cfg: &SortKey,
        result_prefixes: &mut Vec<Value>,
    ) {
        let mut sortable_results = rib_value
            .iter()
            .filter(|&item| Self::include_item_in_results(filter_cfg, item))
            .map(|item| Self::mk_result(query_prefix, item, details_cfg))
            .collect::<Vec<Value>>();

        Self::sort_results(sort_cfg, &mut sortable_results);

        result_prefixes.extend(sortable_results);
    }

    fn sort_results(sort_cfg: &SortKey, sortable_results: &mut [Value]) {
        match sort_cfg {
            SortKey::None => {}

            SortKey::Some(json_pointers) => {
                let json_pointers =
                    json_pointers.split(',').collect::<Vec<_>>();
                sortable_results.sort_by(|a, b| {
                    let mut last_res = Ordering::Equal;

                    for json_pointer in &json_pointers {
                        let lhs = a.pointer(json_pointer);
                        let rhs = b.pointer(json_pointer);
                        last_res = match (lhs, rhs) {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => Ordering::Less,
                            (Some(_), None) => Ordering::Greater,
                            (Some(lhs), Some(rhs)) => {
                                // serde_json::Value doesn't implement PartialOrd
                                // so we can't call cmp
                                match Self::cmp_json_values(lhs, rhs) {
                                    Ordering::Less => Ordering::Less,
                                    Ordering::Equal => {
                                        // Try to break the tie using the next sort key, if one exists
                                        continue;
                                    }
                                    Ordering::Greater => Ordering::Greater,
                                }
                            }
                        };
                        break;
                    }

                    last_res
                })
            }
        };
    }

    // Cmp for serde_json::Value for sorting. Not implemented for object types.
    fn cmp_json_values(lhs: &Value, rhs: &Value) -> Ordering {
        if lhs == rhs {
            Ordering::Equal
        } else {
            // When comparing we expect to compare values of the same type as we are selecting the same field from
            // multiple results in the data and sorting by them.
            match (lhs, rhs) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Bool(v1), Value::Bool(v2)) => v1.cmp(v2),
                (Value::Number(v1), Value::Number(v2)) => {
                    if v1.is_f64() {
                        v1.as_f64().partial_cmp(&v2.as_f64()).unwrap()
                    } else if v1.is_i64() {
                        v1.as_i64().cmp(&v2.as_i64())
                    } else if v1.is_u64() {
                        v1.as_u64().cmp(&v2.as_u64())
                    } else {
                        unreachable!()
                    }
                }
                (Value::String(v1), Value::String(v2)) => v1.cmp(v2),
                (Value::Array(v1), Value::Array(v2)) => {
                    let mut it1 = v1.iter();
                    let mut it2 = v2.iter();
                    // Do lexicographic compare because serde_json::Value doesn't implement the Ord trait so we can't
                    // just compare the two Vec<Value> instances.
                    loop {
                        match (it1.next(), it2.next()) {
                            (None, None) => return Ordering::Equal,
                            (None, Some(_)) => return Ordering::Less,
                            (Some(_), None) => return Ordering::Greater,
                            (Some(lhs), Some(rhs)) => {
                                match Self::cmp_json_values(lhs, rhs) {
                                    Ordering::Less => return Ordering::Less,
                                    Ordering::Greater => {
                                        return Ordering::Greater
                                    }
                                    Ordering::Equal => continue,
                                }
                            }
                        }
                    }
                }
                _ => Ordering::Less,
            }
        }
    }

    fn mk_result(
        _query_prefix: &Prefix,
        route: &Arc<PreHashedTypeValue>,
        details_cfg: &Details,
    ) -> Value {
        // TODO: Honor details_cfg
        serde_json::to_value(route).unwrap()
    }

    fn include_item_in_results(
        filter_cfg: &Filters,
        item: &Arc<PreHashedTypeValue>,
    ) -> bool {
        let no_selects = filter_cfg.selects().is_empty();
        let no_discards = filter_cfg.discards().is_empty();

        if no_selects && no_discards {
            return true;
        }

        let matches = |filter: &Filter| match filter.kind() {
            FilterKind::AsPath(filter_as_path) => {
                Self::match_as_path(item, filter_as_path)
            }

            FilterKind::Community(community) => {
                Self::match_community(item, community)
            }

            FilterKind::PeerAs(peer_as) => Self::match_peer_as(item, peer_as),
        };

        let mut discards = filter_cfg.discards().iter();
        let mut selects = filter_cfg.selects().iter();

        match filter_cfg.op() {
            FilterOp::Any => {
                (no_selects || selects.any(matches))
                    && (no_discards || !discards.any(matches))
            }
            FilterOp::All => {
                (no_selects || selects.all(matches))
                    && (no_discards || !discards.all(matches))
            }
        }
    }

    fn match_as_path(
        item: &Arc<PreHashedTypeValue>,
        filter_as_path: &[Asn],
    ) -> bool {
        match ***item {
            TypeValue::Builtin(BuiltinTypeValue::Route(ref route)) => {
                let mut route = route.clone();
                let attrs = route.get_latest_attrs();
                let as_path = attrs.as_path.as_routecore_hops_vec();
                let mut actual_it = as_path.iter();
                let mut wanted_it = filter_as_path.iter();

                loop {
                    match (actual_it.next(), wanted_it.next()) {
                        (Some(Hop::Asn(actual_asn)), Some(wanted_asn))
                            if actual_asn == wanted_asn =>
                        {
                            continue
                        }

                        (None, None) => return true,

                        _ => return false,
                    }
                }
            }
            _ => false,
        }
    }

    fn match_community(
        item: &Arc<PreHashedTypeValue>,
        community: &Community,
    ) -> bool {
        let wanted_c = BuiltinTypeValue::try_from(*community).unwrap();
        match ***item {
            TypeValue::Builtin(BuiltinTypeValue::Route(ref route)) => {
                let mut route = route.clone();
                let attrs = route.get_latest_attrs();
                attrs.communities.as_vec().iter().any(|item| {
                    matches!(
                        item,
                        ElementTypeValue::Primitive(TypeValue::Builtin(possible_c)) if *possible_c == wanted_c
                    )
                })
            }
            _ => false,
        }
    }

    fn match_peer_as(item: &Arc<PreHashedTypeValue>, peer_as: &Asn) -> bool {
        match ***item {
            TypeValue::Builtin(BuiltinTypeValue::Route(ref route)) => {
                route.peer_asn() == Some(*peer_as)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use roto::types::builtin::{
        BgpUpdateMessage, RawRouteWithDeltas, RotondaId, RouteStatus,
    };
    use routecore::bgp::message::SessionConfig;

    use crate::bgp::encode::{mk_bgp_update, Announcements, Prefixes};

    use super::*;

    #[test]
    fn test_mk_result() {
        let announcements = Announcements::from_str(
            "e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32",
        )
        .unwrap();
        let bgp_update_bytes =
            mk_bgp_update(&Prefixes::default(), &announcements, &[]);

        let delta_id = (RotondaId(0), 0); // TODO
        let prefix =
            routecore::addr::Prefix::from_str("192.168.0.1/32").unwrap();
        let roto_update_msg = roto::types::builtin::UpdateMessage::new(
            bgp_update_bytes,
            SessionConfig::modern(),
        );
        let bgp_update_msg =
            Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
        let raw_route = RawRouteWithDeltas::new_with_message_ref(
            delta_id,
            prefix.into(),
            &bgp_update_msg,
            RouteStatus::InConvergence,
        );

        let details = Details::default();
        let route = PreHashedTypeValue::new(raw_route.into(), 1);
        let json_out =
            PrefixesApi::mk_result(&prefix, &Arc::new(route), &details);
        println!("{}", json_out);
    }
}
