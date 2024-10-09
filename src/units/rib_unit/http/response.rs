use std::{cmp::Ordering, sync::Arc};

use hyper::{Body, Response};

use log::{debug, error};
//use roto::types::{
//    builtin::{BuiltinTypeValue, PrefixRoute}, collections::ElementTypeValue,
//    typevalue::TypeValue,
//};
use rotonda_store::{prelude::{multi::RouteStatus, Prefix}, PublicRecord};
use inetnum::asn::Asn;

use routecore::bgp::{
    aspath::{AsPath, Hop, HopPath}, communities::{
        Community as CommunityEnum, HumanReadableCommunity as Community
    }, nlri::afisafi::{AfiSafiNlri, IsPrefix}, path_attributes::FromAttribute, workshop::route::RouteWorkshop
};

use serde_json::{json, Value};

use crate::{
    common::json::EasilyExtendedJSONObject, ingress::{self, IngressId, IngressInfo}, payload::{RotondaPaMap, RotondaRoute},// units::rib_unit::rib::RibValue
};

use super::{
    types::{
        Details, Filter, FilterKind, FilterOp, Filters, Includes, SortKey,
    },
    PrefixesApi,
};

impl PrefixesApi {
    pub fn mk_json_response(
        //res: rotonda_store::QueryResult<RotondaRoute>,
        res: rotonda_store::QueryResult<RotondaPaMap>,
        includes: Includes,
        details_cfg: Details,
        filters_cfg: Filters,
        sort_cfg: SortKey,
        ingress_register: &Arc<ingress::Register>,
    ) -> Response<Body> {
        let mut out_prefixes = Vec::new();
        let mut out_less_specifics = Vec::new();
        let mut out_more_specifics = Vec::new();

        //debug!("creating response for {:#?}", res);

        

        if let Some(prefix) = res.prefix {
            // now a vec instead of an option
            //if let Some(routes_per_router) = res.prefix_meta {
            for public_record in res.prefix_meta {
                Self::prefixes_as_json(
                    &prefix,
                    &public_record,
                    &details_cfg,
                    &filters_cfg,
                    &sort_cfg,
                    &mut out_prefixes,
                    &ingress_register,
                );
            }
        }

        if includes.less_specifics {
            if let Some(less_specifics) = res.less_specifics {
                for record in less_specifics.iter() {
                    for pub_record in record.meta {
                        Self::prefixes_as_json(
                            &record.prefix,
                            &pub_record,
                            &details_cfg,
                            &filters_cfg,
                            &sort_cfg,
                            &mut out_less_specifics,
                            &ingress_register,
                        );
                    }
                }
            }
        }

        if includes.more_specifics {
            if let Some(more_specifics) = res.more_specifics {
                for record in more_specifics.iter() {
                    for pub_record in record.meta {
                        Self::prefixes_as_json(
                            &record.prefix,
                            &pub_record,
                            &details_cfg,
                            &filters_cfg,
                            &sort_cfg,
                            &mut out_more_specifics,
                            &ingress_register,
                        );
                    }
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
        //rib_value: &RibValue, // RibValue is basically PrefixRoute now
        //record: &PublicRecord<RotondaRoute>,
        record: &PublicRecord<RotondaPaMap>,
        details_cfg: &Details,
        filter_cfg: &Filters,
        sort_cfg: &SortKey,
        result_prefixes: &mut Vec<Value>,
        ingress_register: &Arc<ingress::Register>,
    ) {

        let ingress_id = record.multi_uniq_id;
        let status = record.status;

        let ingress_info = ingress_register.get(ingress_id);

        //let mut sortable_results = Some(Arc::new(TypeValue::from(rib_value.clone())))
        let mut sortable_results = Some(record.meta.clone())
            .iter()
            .filter(|&item| Self::include_item_in_results(filter_cfg, item, &ingress_info))
            .map(|item| Self::mk_result(query_prefix, item, ingress_id, status, details_cfg, &ingress_info))
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
        query_prefix: &Prefix,
        route: &RotondaPaMap,
        ingress_id: IngressId,
        status: RouteStatus,
        _details_cfg: &Details,
        ingress_info: &Option<ingress::IngressInfo>,
    ) -> Value {
        // TODO: Honor details_cfg

        // XXX LH: this is yet another place where a prefix is expected, i.e.
        // things are limited in terms of supported afisafis.

        serde_json::to_value(json!({
            "ingress_id": ingress_id,
            "ingress_info": ingress_info,
            "prefix": query_prefix,
            "status": status.to_string(),
            // "next_hop": nh,
            "attributes": route.0.iter()
                .map(|a| a.ok())
                .flatten()
                .map(|a| a.to_owned().ok())
                .collect::<Vec<_>>()

        })).unwrap()
    }

    fn include_item_in_results(
        filter_cfg: &Filters,
        //item: &Arc<TypeValue>,
        //item: &PrefixRoute,
        //item: &RotondaRoute,
        item: &RotondaPaMap,
        ingress_info: &Option<IngressInfo>,
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

            //FilterKind::PeerAs(peer_as) => Self::match_peer_as(item, *peer_as),

            // NB: we don't have a Provenance available here, but we should
            // have a remote ASN in the ingress::Register for the
            // MUI/ingress_id stored in the prefix store.
            FilterKind::PeerAs(peer_as) => Self::match_peer_as(item, *peer_as, ingress_info),
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
        //item: &Arc<TypeValue>,
        //item: &PrefixRoute,
        //item: &RotondaRoute,
        item: &RotondaPaMap,
        filter_as_path: &[Asn],
    ) -> bool {
            let as_path = item.0.get::<HopPath>();
            let as_path = if let Some(as_path) = as_path {
                debug!("trying to match as_path {:?}", as_path);
                as_path
            } else {
                debug!("Ignoring AS path matching for {:?} with {:?}", item, filter_as_path);
                return false;
            };
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
            /*
        match item {
            TypeValue::Builtin(BuiltinTypeValue::PrefixRoute(ref route)) => {
                //let mut route = route.clone();
                // let attrs = if let Ok(attrs) = route.0.attributes().get_attrs() {
                //     attrs
                // } else {
                //     debug!("Ignoring AS path matching for {:?} with {:?}", route, filter_as_path);
                //     return false;
                // };
                let as_path = if let  Some(as_path) = route.get_attr::<HopPath>() {
                    as_path
                } else {
                    debug!("Ignoring AS path matching for {:?} with {:?}", route, filter_as_path);
                    return false;
                };
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
            */
    }

    fn match_community(
        //item: &Arc<TypeValue>,
        //item: &PrefixRoute,
        //item: &RotondaRoute,
        item: &RotondaPaMap,
        community: &Community,
    ) -> bool {

        #[allow(unused_variables)] // false positive
        let wanted_c = community.0;
        debug!("in match_community, wanted_c {:?}", &wanted_c);


        if let Some(communities) = item.0.get::<Vec<CommunityEnum>>() {
            #[allow(unused_variables)] // false positive
            communities.iter().any(|item| {
                //let match_res = matches!(
                //    item,
                //    //ElementTypeValue::Primitive(TypeValue::Builtin(possible_c)) if *possible_c == wanted_c
                //    wanted_c
                //);
                let match_res = item == &wanted_c;
                debug!("does {:?} match? {}", &item, match_res);
                match_res
            })
        } else {
            debug!("Ignoring community matching for {:?} with {:?}", item, community);
            false
        }

        /*

        //let wanted_c = BuiltinTypeValue::try_from(*community).unwrap();
        match **item {
            TypeValue::Builtin(BuiltinTypeValue::PrefixRoute(ref route)) => {
                //let mut route = route.clone();
                //if let Ok(attrs) = route.get_attrs() {
                if let Some(communities) = route.get_attr::<Vec<CommunityEnum>>() {
                    #[allow(unused_variables)] // false positive
                    communities.iter().any(|item| {
                        matches!(
                            item,
                            //ElementTypeValue::Primitive(TypeValue::Builtin(possible_c)) if *possible_c == wanted_c
                            community
                        )
                    })
                } else {
                    debug!("Ignoring community matching for {:?} with {:?}", route, community);
                    false
                }
            }
            _ => false,
        }
    }
        */
    }

    //fn match_peer_as(item: &Arc<TypeValue>, peer_asn: Asn) -> bool {
    fn match_peer_as(
        //_item: &PrefixRoute,
        //_item: &RotondaRoute,
        _item: &RotondaPaMap,
        peer_asn: Asn,
        ingress_info: &Option<IngressInfo>,
        ) -> bool {
        // FIXME we need the peer ASN here, which is not part of the TypeValue
        // (anymore).
        // We'll need to store (parts of) the provenance in the store or some
        // such.
        //item.as_ref().provenance().peer_asn == peer_asn

        // possible workaround: get the neighbour AS from the HopPath in the
        // RouteWorkshop for this PrefixRoute. This will not work for iBGP
        // with empty AS_PATHs.



        // NB:as long as we make sure the ingress_info describes the BGP
        // session (even when it comes in via BMP), we can use the remote_asn
        // from it.

        if let Some(ingress_info) = ingress_info {
            ingress_info.remote_asn == Some(peer_asn)
        } else {
            // possible workaround: check the right-most ASN in the AS_PATH
            // for now, log an error, because ideally we have the ingress_info
            // available anyway.
            error!("no ingress_info to use in match_peer_as");
            false
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use roto::types::{builtin::{
        PrefixRoute, NlriStatus, Provenance, RotondaId
    }, collections::BytesRecord, lazyrecord_types::BgpUpdateMessage};
    use routecore::bgp::{message::SessionConfig, types::AfiSafiType};

    use crate::bgp::encode::{mk_bgp_update, Announcements, Prefixes};

    use super::*;

    #[ignore = "FIXME this does not really test anything"]
    #[test]
    fn test_mk_result() {
        /*
        let announcements = Announcements::from_str(
            "e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32",
        )
        .unwrap();
        let bgp_update_bytes =
            mk_bgp_update(&Prefixes::default(), &announcements, &[]);

        let delta_id = (RotondaId(0), 0); // TODO
        let prefix =
            inetnum::addr::Prefix::from_str("192.168.0.1/32").unwrap();
        let roto_update_msg = BytesRecord::<BgpUpdateMessage>::new(
            bgp_update_bytes,
            SessionConfig::modern(),
        ).unwrap();
        let raw_route = PrefixRoute::new(
            // delta_id,
            prefix,
            roto_update_msg,
            AfiSafiType::Ipv4Unicast,
            None,
            NlriStatus::InConvergence,
            Provenance {
                timestamp: todo!(),
                connection_id: todo!(),
                peer_id: todo!(),
                peer_bgp_id: todo!(),
                peer_distuingisher: todo!(),
                peer_rib_type: todo!(),
            }
        );

        let details = Details::default();
        let route = raw_route; //PreHashedTypeValue::new(raw_route.into(), 1);
        let json_out =
            PrefixesApi::mk_result(
                &prefix,
                &route, //&Arc::new(route),
                ingress_id,
                RouteStatus::Active,
                &details,
                None, // ingressInfo
            );
        println!("{}", json_out);
        */
    }
}
