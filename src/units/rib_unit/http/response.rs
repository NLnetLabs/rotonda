use std::sync::Arc;

use hyper::{Body, Response};

use rotonda_store::prelude::Prefix;

use serde_json::{json, Value};

use crate::{
    common::json::EasilyExtendedJSONObject,
    units::rib_unit::rib::{PreHashedTypeValue, RibValue},
};

use super::{
    types::{Details, Filters, Includes, SortKey},
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
            .body(Body::from(serde_json::to_string_pretty(&response).unwrap()))
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

    #[cfg(not(test))]
    fn sort_results(_sort_cfg: &SortKey, _sortable_results: &mut [Value]) {}

    #[cfg(test)]
    fn sort_results(sort_cfg: &SortKey, sortable_results: &mut [Value]) {
        match sort_cfg {
            SortKey::None => {}
        };
    }

    fn mk_result(
        query_prefix: &Prefix,
        route: &Arc<PreHashedTypeValue>,
        details_cfg: &Details,
    ) -> Value {
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
            FilterKind::AsPath(filter_as_path) => Self::match_as_path(item, filter_as_path),

            FilterKind::Community(community) => Self::match_community(item, community),

            FilterKind::SourceAs(source_as) => Self::match_source_as(item, source_as),

            FilterKind::RoutingInformationBaseName(rib_name) => Self::match_rib_name(item, rib_name),
        };

        let mut discards = filter_cfg.discards().iter();
        let mut selects = filter_cfg.selects().iter();

        match filter_cfg.op() {
            FilterOp::Any => {
                (no_selects || selects.any(matches)) && (no_discards || !discards.any(matches))
            }
            FilterOp::All => {
                (no_selects || selects.all(matches)) && (no_discards || !discards.all(matches))
            }
        }
    }

    fn match_as_path(item: &Arc<PreHashedTypeValue>, filter_as_path: &[Asn]) -> bool {
        if let Some(advert) = &item.advert {
            advert.as_path == filter_as_path
        } else {
            false
        }
    }

    fn match_community(item: &Arc<PreHashedTypeValue>, community: &Community) -> bool {
        matches!(&item.advert, Some(advert) if advert.has_community(community))
    }

    fn match_source_as(item: &Arc<PreHashedTypeValue>, source_as: &Asn) -> bool {
        item.neighbor.0 == *source_as
    }

    fn match_rib_name(
        item: &Arc<PreHashedTypeValue>,
        rib_name: &RoutingInformationBase,
    ) -> bool {
        item.routing_information_base == *rib_name
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use roto::types::builtin::{RawRouteWithDeltas, RotondaId, RouteStatus, BgpUpdateMessage};
    use routecore::bgp::message::SessionConfig;

    use crate::bgp::{encode::{mk_bgp_update, Announcements, Prefixes}};

    use super::*;

    #[test]
    fn test_mk_result() {
        let announcements =
            Announcements::from_str("e [123,456,789] 10.0.0.1 BLACKHOLE,123:44 127.0.0.1/32")
                .unwrap();
        let bgp_update_bytes = mk_bgp_update(&Prefixes::default(), &announcements, &[]);

        let delta_id = (RotondaId(0), 0); // TODO
        let prefix = routecore::addr::Prefix::from_str("192.168.0.1/32").unwrap();
        let roto_update_msg = roto::types::builtin::UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        let bgp_update_msg = Arc::new(BgpUpdateMessage::new(delta_id, roto_update_msg));
        let raw_route = RawRouteWithDeltas::new_with_message_ref(
            delta_id,
            prefix.into(),
            &bgp_update_msg,
            RouteStatus::InConvergence,
        );

        let details = Details::default();
        let route = PreHashedTypeValue::new(raw_route.into(), 1);
        let json_out = PrefixesApi::mk_result(&prefix, &Arc::new(route), &details);
        println!("{}", json_out);
    }
}
