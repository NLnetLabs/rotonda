use std::sync::Arc;

use crate::representation::GenOutput;
use crate::representation::Json;
use routedb::index_set::routing_tables::Route;
use routedb::index_set::routing_tables::RoutingTable;
use routedb::route_db::RouteDb;
use routedb::TableId;
use serde::ser::SerializeSeq;
use serde::ser::SerializeStruct;
use serde::Serialize;

crate::genoutput_json!(RouteDbRoute<'_>);
crate::genoutput_json!(RouteDbRoutingTables);

pub(crate) struct RouteDbIter<T>(pub T);

#[derive(Serialize)]
pub(crate) struct RouteDbRoutes<'a>(pub Vec<RouteDbRoute<'a>>);
pub(crate) struct RouteDbRoute<'a>(pub Route<'a>);

pub(crate) struct RouteDbRoutingTables {
    pub routedb: Arc<RouteDb>,
    pub ids: Vec<TableId>,
}
pub(crate) struct RouteDbRoutingTable<'a>(pub RoutingTable<'a>);

impl<'a, T: 'a> Serialize for RouteDbIter<T>
where
    T: Iterator<Item = RouteDbRoute<'a>>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        todo!()
    }
}

impl Serialize for RouteDbRoutingTable<'_> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = s.serialize_seq(None)?;
        for r in self.0.iter() {
            seq.serialize_element(&RouteDbRoute(r))?;
        }
        seq.end()
    }
}

impl Serialize for RouteDbRoutingTables {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = s.serialize_seq(None)?;
        for rt_id in &self.ids {
            if let Some(tbl) = self.routedb.routing_tables().by_id(*rt_id) {
                for r in tbl.iter() {
                    seq.serialize_element(&RouteDbRoute(r))?;
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

impl axum::response::IntoResponse for RouteDbRoutes<'_> {
    fn into_response(self) -> axum::response::Response {
        vec![].into_response()
    }
}
