use std::sync::Arc;

use crate::representation;
use crate::representation::GenOutput;
use crate::representation::Json;
use log::warn;
use routecore::bgp::message_ng::common::AfiSafiType;
use routecore::bgp::message_ng::nlri::Nlri;
use routedb::index_set::routing_tables::Route;
use routedb::index_set::routing_tables::RoutingTable;
use routedb::route_db::RouteDb;
use routedb::TableId;
use serde::ser::SerializeSeq;
use serde::ser::SerializeStruct;
use serde::Serialize;

crate::genoutput_json!(RouteDbRoute<'_>);
crate::genoutput_json!(RouteDbRoutingTables);

//pub(crate) struct RouteDbIter<T>(pub T);

//#[derive(Serialize)]
//pub(crate) struct RouteDbRoutes<'a>(pub Vec<RouteDbRoute<'a>>);

struct RouteDbRoute<'a>(pub Route<'a>);

pub(super) struct RouteDbRouteIpv4Unicast<'a>(pub Route<'a>);
pub(super) struct RouteDbRouteIpv4UnicastAddPath<'a>(pub Route<'a>);

//struct RouteDbRouteIpv6Unicast<'a>(pub Route<'a>);
//struct RouteDbRouteIpv6UnicastAddpath<'a>(pub Route<'a>);

crate::genoutput_json!(RouteDbRouteIpv4Unicast<'_>);
crate::genoutput_json!(RouteDbRouteIpv4UnicastAddPath<'_>);
//crate::genoutput_json!(RouteDbRouteIpv6Unicast<'_>);
//crate::genoutput_json!(RouteDbRouteIpv6UnicastAddpath<'_>);

pub(crate) struct RouteDbRoutingTables {
    pub routedb: Arc<RouteDb>,
    pub ids: Vec<TableId>,
}

impl Serialize for RouteDbRoutingTables {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = s.serialize_seq(None)?;
        for rt_id in &self.ids {
            if let Some(tbl) = self.routedb.routing_tables().by_id(*rt_id) {
                let Ok(props) = tbl.props() else {
                    warn!("no properties for table");
                    continue;
                };
                match (props.afi_safi, props.add_path_cap) {
                    (AfiSafiType::IPV4UNICAST, false) => {
                        for r in tbl.iter() {
                            seq.serialize_element(&RouteDbRouteIpv4Unicast(
                                r,
                            ))?;
                        }
                    }
                    (AfiSafiType::IPV4UNICAST, true) => {
                        for r in tbl.iter() {
                            seq.serialize_element(
                                &RouteDbRouteIpv4UnicastAddPath(r),
                            )?;
                        }
                    }
                    (AfiSafiType::IPV6UNICAST, false) => {}
                    (AfiSafiType::IPV6UNICAST, true) => {}
                    (_, false) => {}
                    (_, true) => {}
                }
            }
        }
        seq.end()
    }
}

impl Serialize for RouteDbRoute<'_> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let nlri = self.0.nlri();

        let pa_header = routecore::bgp::message_ng::path_attributes::common::PreppedAttributesHeader::from(self.0.pa_hints()); // XXX why is this not [u8; 10]?
        let attrs = self.0.path_attrs();
        let prepped_attrs = routecore::bgp::message_ng::path_attributes::common::PreppedAttributes {
            header: &pa_header,
            path_attributes: routecore::bgp::message_ng::path_attributes::common::UncheckedPathAttributes::from_slice_unchecked(attrs),
        };
        let mut state = s.serialize_struct("_wrapper", 2)?;
        // TODO include ingress info
        state.serialize_field("nlri", &nlri)?;
        state.serialize_field("pathAttributes", &prepped_attrs)?;
        state.end()
    }
}

impl Serialize for RouteDbRouteIpv4Unicast<'_> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let nlri = self.0.nlri();

        let pa_header = routecore::bgp::message_ng::path_attributes::common::PreppedAttributesHeader::from(self.0.pa_hints()); // XXX why is this not [u8; 10]?
        let attrs = self.0.path_attrs();
        let prepped_attrs = routecore::bgp::message_ng::path_attributes::common::PreppedAttributes {
            header: &pa_header,
            path_attributes: routecore::bgp::message_ng::path_attributes::common::UncheckedPathAttributes::from_slice_unchecked(attrs),
        };
        let mut state = s.serialize_struct("_wrapper", 2)?;
        // TODO include ingress info
        state.serialize_field(
            "nlri",
            &routecore::bgp::message_ng::nlri::Ipv4UnicastNlri::try_from(nlri)
                .unwrap(),
        )?;
        state.serialize_field("pathAttributes", &prepped_attrs)?;
        state.end()
    }
}

impl Serialize for RouteDbRouteIpv4UnicastAddPath<'_> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let nlri = self.0.nlri();

        let pa_header = routecore::bgp::message_ng::path_attributes::common::PreppedAttributesHeader::from(self.0.pa_hints()); // XXX why is this not [u8; 10]?
        let attrs = self.0.path_attrs();
        let prepped_attrs = routecore::bgp::message_ng::path_attributes::common::PreppedAttributes {
            header: &pa_header,
            path_attributes: routecore::bgp::message_ng::path_attributes::common::UncheckedPathAttributes::from_slice_unchecked(attrs),
        };
        let mut state = s.serialize_struct("_wrapper", 2)?;

        // TODO include ingress info

        //state.serialize_field(
        //    "nlri",
        //    &routecore::bgp::message_ng::nlri::Ipv4UnicastNlriAddPath::try_from(nlri)
        //        .unwrap(),
        //)?;

        let nlri =
            routecore::bgp::message_ng::nlri::Ipv4UnicastNlriAddPath::try_from(
                nlri,
            ).unwrap();

        // eventually, nlri is not serialised here but one level 'above',
        // as the response is grouped per nlri
        state.serialize_field("nlri", &nlri.nlri())?;
        state.serialize_field("pathId", &nlri.path_id())?;
        state.serialize_field("pathAttributes", &prepped_attrs)?;
        state.end()
    }
}
