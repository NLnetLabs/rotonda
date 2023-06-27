use crate::{
    common::{
        frim::FrimMap,
        roto::{is_filtered_in_vm, ThreadLocalVM},
        status_reporter::{AnyStatusReporter, UnitStatusReporter},
    },
    comms::{
        AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus, Link, Terminated, TriggerData,
    },
    manager::{Component, WaitPoint},
    payload::{Payload, RouterId, Update},
    units::Unit,
};
use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;

use chrono::Utc;
use hash_hasher::{HashBuildHasher, HashedSet};
use log::{error, log_enabled, trace};
use non_empty_vec::NonEmpty;
use roto::{
    traits::RotoType,
    types::{builtin::BuiltinTypeValue, collections::ElementTypeValue, typevalue::TypeValue},
};
use rotonda_store::{
    custom_alloc::Upsert, epoch, prelude::multi::PrefixStoreError, QueryResult, RecordSet,
};

use routecore::addr::Prefix;
use serde::Deserialize;
use std::{
    cell::RefCell,
    collections::hash_map::DefaultHasher,
    fs::read_to_string,
    hash::{Hash, Hasher},
    ops::{ControlFlow, Deref},
    path::PathBuf,
    str::FromStr,
    string::ToString,
    sync::Arc,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{
    http::PrefixesApi,
    metrics::RibUnitMetrics,
    rib::{PhysicalRib, PreHashedTypeValue, RibValue},
    status_reporter::RibUnitStatusReporter,
};

use roto::types::typedef::TypeDef;
use std::time::Instant;

#[derive(Clone, Debug, Deserialize)]
pub struct MoreSpecifics {
    /// The shortest IPv4 prefix, N (from /N), for which more specific (i.e. longer prefix) matches can be queried.
    #[serde(default = "MoreSpecifics::default_shortest_prefix_ipv4")]
    pub shortest_prefix_ipv4: u8,

    /// The shortest IPv6 prefix, N (from /N), for which more specific (i.e. longer prefix) matches can be queried.
    #[serde(default = "MoreSpecifics::default_shortest_prefix_ipv6")]
    pub shortest_prefix_ipv6: u8,
}

impl MoreSpecifics {
    pub fn default_shortest_prefix_ipv4() -> u8 {
        // IPv4 space is densely populated, tree size quickly becomes very large at shorter prefix lengths so limit
        // searches to prefixes no shorter than /8, i.e. /7 is not permitted, but /32 is.
        8
    }

    pub fn default_shortest_prefix_ipv6() -> u8 {
        // IPv6 space is sparsely populated compared to IPv4 space so the tree is less dense at shorter prefixes
        // than the equivalent for IPv4 and so we can afford to be permit "deeper" searches for IPv6 than for IPv4.
        19
    }

    pub fn shortest_prefix_permitted(&self, prefix: &Prefix) -> u8 {
        if prefix.is_v4() {
            self.shortest_prefix_ipv4
        } else if prefix.is_v6() {
            self.shortest_prefix_ipv6
        } else {
            unreachable!()
        }
    }
}

impl Default for MoreSpecifics {
    fn default() -> Self {
        Self {
            shortest_prefix_ipv4: Self::default_shortest_prefix_ipv4(),
            shortest_prefix_ipv6: Self::default_shortest_prefix_ipv6(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct QueryLimits {
    pub more_specifics: MoreSpecifics,
}

#[derive(Copy, Clone, Debug, Default, Deserialize)]
pub enum RibType {
    /// A physical RIB has zero or one roto scripts and a prefix store. Queries to its HTTP API are answered using the
    /// local store.
    #[default]
    Physical,

    /// A virtual RIB has one roto script and no prefix store. Queries to its HTTP API are answered by sending a
    /// command to the nearest physical Rib to the West of the virtual RIB. A `Link` to the gate of that physical Rib
    /// is automatically injected as the virtuaL_upstream value in the RibUnit config below by the config loading
    /// process so that it can be used to send a GateCommand::Query message upstream to the physical Rib unit that owns
    /// the Gate that the Link refers to.
    ///
    /// The index (zero-based) indicates how far from the physical RIB this vRIB is.
    Virtual(u8),
}

impl PartialEq for RibType {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RibUnit {
    /// The set of units to receive updates from.
    pub sources: NonEmpty<DirectLink>,

    /// The relative path at which we should listen for HTTP query API requests
    #[serde(default = "RibUnit::default_http_api_path")]
    pub http_api_path: String,

    #[serde(default = "RibUnit::default_query_limits")]
    pub query_limits: QueryLimits,

    /// Path to roto script to use
    /// Note: Due to a special hack in `config.rs` the user can actually supply a collection of paths here. This unit
    /// will only ever see the first of them if so specified, the rest will be used to spawn additional RibUnits
    /// downstream from this one with `rib_type` set to `Virtual`.
    pub roto_path: Option<PathBuf>,

    /// What type of RIB is this?
    #[serde(default)]
    pub rib_type: RibType,

    /// Virtual RIB upstream physical RIB. Only used when rib_type is Virtual.
    #[serde(default)]
    pub vrib_upstream: Option<Link>,
}

impl RibUnit {
    pub async fn run(
        self,
        component: Component,
        gate: Gate,
        waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        RibUnitRunner::new(gate, component, self.roto_path, self.rib_type)
            .run(
                self.sources,
                self.http_api_path,
                self.query_limits,
                self.rib_type,
                self.vrib_upstream,
                waitpoint,
            )
            .await
    }

    fn default_http_api_path() -> String {
        "/prefixes/".to_string()
    }

    fn default_query_limits() -> QueryLimits {
        QueryLimits::default()
    }
}

pub struct RibUnitRunner {
    gate: Arc<Gate>,
    component: Component,
    rib: Arc<ArcSwapOption<PhysicalRib>>,
    status_reporter: Option<Arc<RibUnitStatusReporter>>,
    roto_source: Arc<ArcSwap<(std::time::Instant, String)>>,
    pending_vrib_query_results: Arc<PendingVirtualRibQueryResults>,
}

#[async_trait]
impl DirectUpdate for RibUnitRunner {
    async fn direct_update(&self, update: Update) {
        let gate = self.gate.clone();
        let status_reporter = self.status_reporter.clone().unwrap();
        let rib = self.rib.clone();
        let roto_source = self.roto_source.clone();
        let pending_vrib_query_results = self.pending_vrib_query_results.clone();
        Self::process_update(
            gate,
            status_reporter,
            update,
            rib,
            |pfx, meta, store| store.insert(pfx, meta),
            roto_source,
            pending_vrib_query_results,
        )
        .await;
    }
}

impl std::fmt::Debug for RibUnitRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RibUnitRunner").finish()
    }
}

impl AnyDirectUpdate for RibUnitRunner {}

pub type QueryId = Uuid;
pub type QueryOperationResult = Result<QueryResult<RibValue>, String>;
pub type QueryOperationResultSender = oneshot::Sender<QueryOperationResult>;
pub type PendingVirtualRibQueryResults = FrimMap<QueryId, Arc<QueryOperationResultSender>>;

impl RibUnitRunner {
    thread_local!(
        #[allow(clippy::type_complexity)]
        static VM: ThreadLocalVM = RefCell::new(None);
    );

    fn new(
        gate: Gate,
        component: Component,
        roto_path: Option<PathBuf>,
        rib_type: RibType,
    ) -> Self {
        let roto_source_code = roto_path
            .map(|v| read_to_string(v).unwrap())
            .unwrap_or_default();
        let roto_source = (Instant::now(), roto_source_code);
        let roto_source = Arc::new(ArcSwap::from_pointee(roto_source));

        let rib = match rib_type {
            RibType::Physical => {
                //
                // --- TODO: Create the Rib based on the Roto script Rib 'contains' type
                //
                // TODO: If the roto script changes we have to be able to detect if the record type changed, and if it
                // did, recreate the store, dropping the current data, right?
                //
                // TOOD: And that also these steps should be done at reconfiguration time as well, not just at
                // initialisation time (i.e. where we handle GateStatus::Reconfiguring below).
                //
                // --- End: Create the Rib based on the Roto script Rib 'contains' type
                //

                let physical_rib = PhysicalRib::new(
                    "my-rib", // TODO: Where should this name come from? Is it for lookup and access by other Roto
                );

                Arc::new(ArcSwapOption::from_pointee(physical_rib))
            }

            RibType::Virtual(_) => Default::default(),
        };

        Self {
            gate: Arc::new(gate),
            component,
            rib,
            status_reporter: None,
            roto_source,
            pending_vrib_query_results: Arc::new(FrimMap::default()),
        }
    }

    pub async fn run(
        mut self,
        mut sources: NonEmpty<DirectLink>,
        http_api_path: String,
        query_limits: QueryLimits,
        rib_type: RibType,
        prib_upstream: Option<Link>,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
        let component = &mut self.component;
        let unit_name = component.name().clone();

        // Setup metrics
        let metrics = Arc::new(RibUnitMetrics::new(&self.gate));
        component.register_metrics(metrics.clone());

        // Setup status reporting
        let status_reporter = Arc::new(RibUnitStatusReporter::new(&unit_name, metrics.clone()));
        self.status_reporter = Some(status_reporter.clone());

        // Setup REST API endpoint. vRIBs listen at the vRIB HTTP prefix + /n/ where n is the index assigned to the vRIB
        // during configuration post-processing.
        let http_api_path = http_api_path.trim_end_matches('/').to_string();
        let (http_api_path, is_sub_resource) = match rib_type {
            RibType::Physical => (Arc::new(format!("{http_api_path}/")), false),
            RibType::Virtual(index) => (Arc::new(format!("{http_api_path}/{index}/")), true),
        };
        let query_limits = Arc::new(ArcSwap::from_pointee(query_limits));
        let processor = PrefixesApi::new(
            self.rib.clone(),
            http_api_path,
            query_limits.clone(),
            rib_type,
            prib_upstream,
            self.pending_vrib_query_results.clone(),
        );
        let processor = Arc::new(processor);
        if is_sub_resource {
            component.register_sub_http_resource(processor.clone());
        } else {
            component.register_http_resource(processor.clone());
        }

        let arc_self = Arc::new(self);

        // Register as a direct update receiver with the linked gates.
        for link in sources.iter_mut() {
            link.connect(arc_self.clone(), false).await.unwrap();
        }

        // Wait for other components to be ready, and signal to other components that we are, ready to start. All units
        // and targets start together, otherwise data passed from one component to another may be lost if the receiving
        // component is not yet ready to accept it.
        arc_self.gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone waiting to send important gate status
        // updates won't send them while we are in process_until() which will just eat them without handling them.
        waitpoint.running().await;

        loop {
            match arc_self.gate.process().await {
                Ok(status) => {
                    status_reporter.gate_status_announced(&status);
                    match status {
                        GateStatus::Reconfiguring {
                            new_config:
                                Unit::RibUnit(RibUnit {
                                    sources: new_sources,
                                    query_limits: new_query_limits,
                                    ..
                                    // http_api_path
                                }),
                        } => {
                            // TODO: Handle changed RibUnit::vrib_upstream value.
                            status_reporter.reconfigured();

                            query_limits.store(Arc::new(new_query_limits));

                            // Register as a direct update receiver with the new
                            // set of linked gates.
                            status_reporter.upstream_sources_changed(sources.len(), new_sources.len());
                            sources = new_sources;
                            for link in sources.iter_mut() {
                                link.connect(arc_self.clone(), false).await.unwrap();
                            }
                        }

                        GateStatus::ReportLinks { report } => {
                            report.set_sources(&sources);
                            report.set_graph_status(arc_self.gate.metrics());
                        }

                        GateStatus::Triggered { data: TriggerData::MatchPrefix( uuid, prefix, match_options ) } => {
                            assert!(matches!(rib_type, RibType::Physical));
                            let res = {
                                if let Some(rib) = arc_self.rib.load().as_ref() {
                                    let guard = &epoch::pin();
                                    trace!("Performing triggered query {uuid}");
                                    Ok(rib.match_prefix(&prefix, &match_options, guard))
                                } else {
                                    Err("Cannot query non-existent RIB".to_string())
                                }
                            };
                            trace!("Sending query {uuid} results downstream");
                            arc_self.gate.update_data(Update::QueryResult(uuid, res)).await;
                        }

                        _ => { /* Nothing to do */ }
                    }
                }

                Err(Terminated) => {
                    status_reporter.terminated();
                    return Err(Terminated);
                }
            }
        }
    }

    async fn process_update<F>(
        gate: Arc<Gate>,
        status_reporter: Arc<RibUnitStatusReporter>,
        update: Update,
        rib: Arc<ArcSwapOption<PhysicalRib>>,
        insert: F,
        roto_source: Arc<ArcSwap<(Instant, String)>>,
        pending_vrib_query_results: Arc<PendingVirtualRibQueryResults>,
    ) where
        F: Fn(&Prefix, RibValue, &PhysicalRib) -> Result<(Upsert, u32), PrefixStoreError>
            + Send
            + Copy,
        F: 'static,
    {
        match update {
            Update::Bulk(updates) => {
                // let mut new_announcements = 0;
                // let mut modified_announcements = 0;
                // let mut new_withdrawals = 0;

                for payload in updates {
                    if let Some(update) = Self::process_update_single(
                        payload,
                        rib.clone(),
                        insert,
                        roto_source.clone(),
                        status_reporter.clone(),
                    )
                    .await
                    {
                        gate.update_data(update).await;
                    }
                }

                // status_reporter.update_processed(
                //     new_announcements,
                //     modified_announcements,
                //     new_withdrawals,
                // );

                if let Some(rib) = rib.load().as_ref() {
                    status_reporter.unique_prefix_count_updated(rib.prefixes_count());
                }
            }

            Update::Single(payload) => {
                // TODO: update status reporter/metrics as is done in the bulk case
                if let Some(update) = Self::process_update_single(
                    payload,
                    rib.clone(),
                    insert,
                    roto_source.clone(),
                    status_reporter.clone(),
                )
                .await
                {
                    gate.update_data(update).await;
                }
            }

            Update::QueryResult(uuid, upstream_query_result) => {
                trace!("Re-processing received query {uuid} result");
                let processed_res = match upstream_query_result {
                    Ok(res) => Ok(Self::reprocess_query_results(
                        res,
                        roto_source,
                        status_reporter.clone(),
                    )
                    .await),
                    Err(err) => Err(err),
                };

                // Were we waiting for this result?
                if let Some(tx) = pending_vrib_query_results.remove(&uuid) {
                    // Yes, send the result to the waiting HTTP request processing task
                    trace!("Notifying waiting HTTP request processor of query {uuid} results");
                    let tx = Arc::try_unwrap(tx).unwrap(); // TODO: handle this unwrap
                    tx.send(processed_res).unwrap(); // TODO: handle this unwrap
                } else {
                    // No, pass it on to the next virtual RIB
                    trace!("Sending re-processed triggered query {uuid} results downstream");
                    gate.update_data(Update::QueryResult(uuid, processed_res))
                        .await;
                }
            }
        }
    }

    pub async fn process_update_single<F>(
        payload: Payload,
        rib: Arc<ArcSwapOption<PhysicalRib>>,
        insert: F,
        roto_source: Arc<ArcSwap<(Instant, String)>>,
        status_reporter: Arc<RibUnitStatusReporter>,
    ) -> Option<Update>
    where
        F: Fn(&Prefix, RibValue, &PhysicalRib) -> Result<(Upsert, u32), PrefixStoreError> + Send,
        F: 'static,
    {
        match payload {
            Payload::TypeValue(input) => {
                match Self::is_filtered(input, Some(roto_source)) {
                    Ok(ControlFlow::Break(())) => {
                        // Nothing to do
                    }

                    Ok(ControlFlow::Continue(output)) => {
                        // TODO: Review the comments below. They may be outdated already.
                        //
                        // TODO: BgpUpdateMessage should have a "source" indication (the peer from which it came) so
                        // that we can work out which route in the RIB a withdraw applies to. When withdrawing a
                        // RawRouteWithDelta we should include this "source" in the hash key used to overwrite/remove
                        // the existing RawRouteWithDelta inside the RibValue at the prefix.
                        // TODO: For now could we abuse the RotondaId for this? Probably not, it's only a usize.
                        //
                        // What uniquely identifies a peer such that when that peering session goes down (either via
                        // our BGP-ingress losing its connection, or via our BMP-ingress receiving a Peer Down
                        // Notification message) we can withdraw the routes that were active (announced but not yet
                        // withdrawn) over that peering session (aka by that peer)?
                        //
                        // For BGP a peering session is a combination of a TCP session and a BGP OPEN message exchange.
                        // The TCP session defines the connecting peer by source IP address and source port number. The
                        // BGP OPEN message defines the connecting peer by ASN and 16-bit BGP identifier (historically
                        // the IPv4 address of the peer). One could also include the local IP address and port number
                        // tha the BGP session is connected to, in case we are listening on multiple, as these would be
                        // distinct connections from connections to other local IP address and port numbers.
                        //
                        // For BMP a peering session is identified by the contents of the Peer Up Notification message.
                        // This consists of a Local Address (IPv4 or IPv6 IP address), a Local Port, a Remote Port and
                        // the received BGP OPEN message. So the Local Address, Local Port and Remote Port are
                        // replacements for the information we don't have because we aren't ourselves actually
                        // connected via TCP to the peer. We also have the Per Peer Header for the Peer Up Notification
                        // message which contains the Peer Address which is the Remote Address we lack from the Peer Up
                        // Notification message.
                        //
                        // So, the TCP connection carrying a single BGP session is between two peers, and thus the peer
                        // that announces a route is uniquely identified by:
                        // - For BGP-in: the TCP connection remote address:port -> local address:port tuple.
                        // - For BMP-in: the Peer UP PPH address:Remote Port -> Peer Up Local Address:Local Port tuple.
                        // Possibly in combination with the BGP Identifier (in the BGP OPEN message and the BMP PPH):
                        //   "This 4-octet unsigned integer indicates the BGP Identifier of
                        //    the sender.  A given BGP speaker sets the value of its BGP
                        //    Identifier to an IP address that is assigned to that BGP
                        //    speaker.  The value of the BGP Identifier is determined upon
                        //    startup and is the same for every local interface and BGP peer."
                        //
                        // RFC 2686 relaxes the definition of BGP Identifier so we definitely cannot assume it is an
                        // IPv4 address as defined by BGP 4271:
                        //   "The BGP Identifier is a 4-octet, unsigned, non-zero integer that
                        //    should be unique within an AS.  The value of the BGP Identifier
                        //    for a BGP speaker is determined on startup and is the same for
                        //    every local interface and every BGP peer."
                        // But it still says "is the same for every local interface and every BGP peer".
                        //
                        // But the BMP RFC also says:
                        //   "When a route is
                        //    withdrawn by a peer, a corresponding withdraw is sent to the
                        //    monitoring station.  The withdraw MUST have its L flag set to
                        //    correspond to that of any previous announcement; if the route in
                        //    question was previously announced with L flag both clear and set, the
                        //    withdraw MUST similarly be sent twice, with L flag clear and set."
                        //
                        // The L flag:
                        //   "The L flag, if set to 1, indicates that the message reflects
                        //    the post-policy Adj-RIB-In (i.e., its path attributes reflect
                        //    the application of inbound policy).  It is set to 0 if the
                        //    message reflects the pre-policy Adj-RIB-In.  Locally sourced
                        //    routes also carry an L flag of 1.  See Section 5 for further
                        //    detail"
                        //
                        // And:
                        //   "A BMP speaker may send pre-policy routes, post-policy routes, or
                        //    both.  The selection may be due to implementation constraints (it is
                        //    possible that a BGP implementation may not store, for example, routes
                        //    that have been filtered out by policy).  Pre-policy routes MUST have
                        //    their L flag clear in the BMP header (see Section 4), post-policy
                        //    routes MUST have their L flag set.  When an implementation chooses to
                        //    send both pre- and post-policy routes, it is effectively multiplexing
                        //    two update streams onto the BMP session.  The streams are
                        //    distinguished by their L flags."
                        //
                        // And finally, RFC 2858 defines route withdrawals per AFI/SAFI pair:
                        //
                        //   "Network Layer Reachability Information:
                        //    (Ximon: MP_REACH_NLRI - announced routes)
                        //    A variable length field that lists NLRI for the feasible routes
                        //    that are being advertised in this attribute. When the
                        //    Subsequent Address Family Identifier field is set to one of the
                        //    values defined in this document, each NLRI is encoded as
                        //    specified in the "NLRI encoding" section of this document."
                        //
                        //   "Withdrawn Routes:
                        //    (Ximon: MP_UNREACH_NLRI - withdrawn routes)
                        //    A variable length field that lists NLRI for the routes that are
                        //    being withdrawn from service. When the Subsequent Address
                        //    Family Identifier field is set to one of the values defined in
                        //    this document, each NLRI is encoded as specified in the "NLRI
                        //    encoding" section of this document."
                        //
                        // So (S)AFI is also part of the route matching criteria.

                        // Only physical RIBs have a store to insert into.
                        if let Some(rib) = rib.load().as_ref() {
                            //
                            // Where do we get the prefix from?
                            // When we receive a route, use its prefix. This will always be the case at the start of
                            // the pipeline. *UPDATE*: DO THIS FOR NOW.
                            // When we receive a record, we can store it if it has a field named 'prefix' and of type
                            // Prefix. *UPDATE:* DO _NOT_ DO THIS FOR NOW. INSTEAD ONLY SUPPORT INSERTION OF ROUTES.
                            //
                            // And is the operation add or remove?
                            // Let's assume for now that "remove" only occurs due to some sort of expiration of entries
                            // and/or maximum number of entries management.
                            // The roto script can mark a route as withdrawn or for any other type it stores in the RIB
                            // can similarly overwrite a previous value by the same key and set a non-key field to some
                            // marker value indicating that this is a "removed" entry.
                            // *UPDATE:* DO _NOT_ DO THIS FOR NOW. INSTEAD ONLY SUPPORT SPECIAL HANDLING FOR ROUTES.
                            //
                            let prefix: Option<Prefix> = match &output {
                                TypeValue::Builtin(BuiltinTypeValue::Route(route)) => {
                                    Some(route.prefix.into())
                                }

                                TypeValue::Record(record) => {
                                    match record.get_value_for_field("prefix") {
                                        Some(ElementTypeValue::Primitive(TypeValue::Builtin(
                                            BuiltinTypeValue::Prefix(prefix),
                                        ))) => Some((*prefix).into()),
                                        _ => None,
                                    }
                                }

                                _ => None,
                            };

                            if let Some(prefix) = prefix {
                                let hash = rib.precompute_hash_code(&output);
                                let hashed_value = PreHashedTypeValue::new(output.clone(), hash);
                                let rib_value = RibValue::from(hashed_value);

                                let pre_insert = Utc::now();
                                match insert(&prefix, rib_value, rib) {
                                    Ok((upsert, num_retries)) => {
                                        let post_insert = Utc::now();
                                        let insert_delay = (post_insert - pre_insert)
                                            .num_microseconds()
                                            .unwrap_or(i64::MAX);

                                        // TODO: Use RawBgpMessage LogicalTime?
                                        // let propagation_delay = (post_insert - rib_el.received).num_milliseconds();
                                        let propagation_delay = 0;

                                        // TODO: Add a way to check RouteStatus on route or rib_value
                                        // let is_announcement = rib_el.advert.is_some();
                                        let is_announcement = true;

                                        let router_id =
                                            Arc::new(RouterId::from_str("dummy").unwrap());

                                        match upsert {
                                            Upsert::Insert => {
                                                status_reporter.insert_ok(
                                                    router_id,
                                                    insert_delay,
                                                    propagation_delay,
                                                    num_retries,
                                                    is_announcement,
                                                );

                                                // if is_announcement {
                                                //     new_announcements += 1;
                                                // }
                                            }
                                            Upsert::Update => {
                                                status_reporter.update_ok(
                                                    router_id,
                                                    insert_delay,
                                                    propagation_delay,
                                                    num_retries,
                                                );

                                                // if is_announcement {
                                                //     modified_announcements += 1;
                                                // }
                                            }
                                        }

                                        // if !is_announcement {
                                        //     new_withdrawals += 1;
                                        // }
                                    }

                                    Err(err) => status_reporter.insert_failed(prefix, err),
                                }
                            }
                        }

                        return Some(Update::Single(Payload::TypeValue(output)));
                    }

                    Err(err) => {
                        // TODO: Don't log here, instead increment counters?
                        error!("Failed to execute Roto VM: {err}");
                    }
                }
            }

            Payload::RawBmp { .. } => {
                status_reporter.input_mismatch("Payload::RawBmp(_)", "Payload::TypeValue(_)");
            }
        }

        None
    }

    async fn reprocess_query_results(
        res: QueryResult<RibValue>,
        roto_source: Arc<arc_swap::ArcSwapAny<Arc<(Instant, String)>>>,
        status_reporter: Arc<RibUnitStatusReporter>,
    ) -> QueryResult<RibValue> {
        let mut processed_res = QueryResult::<RibValue> {
            match_type: res.match_type,
            prefix: res.prefix,
            prefix_meta: None,
            less_specifics: None,
            more_specifics: None,
        };

        if let Some(v) = &res.prefix_meta {
            processed_res.prefix_meta =
                Self::reprocess_rib_value(v, roto_source.clone(), status_reporter.clone()).await;
        }

        if let Some(record_set) = &res.less_specifics {
            processed_res.less_specifics =
                Self::reprocess_record_set(record_set, &roto_source, status_reporter.clone()).await;
        }

        if let Some(record_set) = &res.more_specifics {
            processed_res.more_specifics =
                Self::reprocess_record_set(record_set, &roto_source, status_reporter.clone()).await;
        }

        if log_enabled!(log::Level::Trace) {
            let exact_match_diff =
                (res.prefix_meta.is_some() as u8) - (processed_res.prefix_meta.is_some() as u8);
            let less_specifics_diff = res.less_specifics.map_or(0, |v| v.len())
                - processed_res.less_specifics.as_ref().map_or(0, |v| v.len());
            let more_specifics_diff = res.more_specifics.map_or(0, |v| v.len())
                - processed_res.more_specifics.as_ref().map_or(0, |v| v.len());
            if exact_match_diff != 0 || less_specifics_diff != 0 || more_specifics_diff != 0 {
                trace!("Virtual RIB reprocessing of QueryResult discarded some results: exact: {}, less_specific: {}, more_specific: {}",
                    exact_match_diff, less_specifics_diff, more_specifics_diff);
            }
        }

        processed_res
    }

    /// Re-process a value from our Rib through our roto script.
    ///
    /// Used by virtual RIBs when a query result flows through them from West to East as a result of a query from a virtual
    /// RIB to the East made against a physical RIB to the West.
    async fn reprocess_rib_value(
        rib_value: &RibValue,
        roto_source: Arc<ArcSwap<(Instant, String)>>,
        status_reporter: Arc<RibUnitStatusReporter>,
    ) -> Option<RibValue> {
        let mut new_values = HashedSet::with_capacity_and_hasher(1, HashBuildHasher::default());

        for route in rib_value.iter() {
            let route_with_user_defined_hash = route.deref();

            // TODO: Once RibValue stores TypeValue instead of RawRouteWithDelta we can
            // get rid of this conversion to TypeValue.
            // TODO: Errm, we're now storing TypeValue, so what do we need to change here?
            let raw_route_with_deltas = route_with_user_defined_hash.deref().clone();
            let payload = Payload::TypeValue(raw_route_with_deltas);

            trace!("Reprocessing route");

            let processed_update = Self::process_update_single(
                payload,
                Arc::default(),
                |_, _, _| unreachable!(),
                roto_source.clone(),
                status_reporter.clone(),
            )
            .await;

            #[allow(clippy::collapsible_match)]
            if let Some(Update::Single(Payload::TypeValue(mut type_value))) = processed_update {
                // Add this processed query result route into the new query result
                // TODO: Once RibValue stores TypeValue instead of RawRouteWithDelta we
                // can get rid of this conversion from TypeValue.
                // TODO: Don't hard code the hash keys here
                // TODO: Shouldn't this also use the new Rib::precompute_hash_code() here?
                let hash =
                    mk_user_defined_hash(&mut type_value, &[TypeDef::IpAddress, TypeDef::AsPath]);
                let hashed_value = PreHashedTypeValue::new(type_value, hash);
                new_values.insert(hashed_value.into());
            }
        }

        if new_values.is_empty() {
            None
        } else {
            Some(RibValue::new(new_values))
        }
    }

    async fn reprocess_record_set(
        record_set: &RecordSet<RibValue>,
        roto_source: &Arc<arc_swap::ArcSwapAny<Arc<(Instant, String)>>>,
        status_reporter: Arc<RibUnitStatusReporter>,
    ) -> Option<RecordSet<RibValue>> {
        let mut new_record_set = RecordSet::<RibValue>::new();

        for record in record_set.iter() {
            let rib_value = record.meta;
            if let Some(rib_value) =
                Self::reprocess_rib_value(&rib_value, roto_source.clone(), status_reporter.clone())
                    .await
            {
                new_record_set.push(record.prefix, rib_value);
            }
        }

        if !new_record_set.is_empty() {
            Some(new_record_set)
        } else {
            None
        }
    }

    fn is_filtered<R: RotoType>(
        rx_tx: R,
        roto_source: Option<Arc<ArcSwap<(Instant, String)>>>,
    ) -> Result<ControlFlow<(), TypeValue>, String> {
        match roto_source {
            None => {
                // No Roto filter defined, accept the BGP UPDATE messsage
                Ok(ControlFlow::Continue(rx_tx.into()))
            }
            Some(roto_source) => {
                // TODO: Run the Roto VM on a dedicated thread pool, to prevent blocking the Tokio async runtime, as we
                // don't know how long we will have to wait for the VM execution to complete (as it depends on the
                // behaviour of the user provided script). Timeouts might be a good idea!
                Self::VM.with(move |vm| -> Result<ControlFlow<(), TypeValue>, String> {
                    is_filtered_in_vm(vm, roto_source, rx_tx)
                })
            }
        }
    }
}

fn mk_user_defined_hash(value: &mut TypeValue, hash_keys: &[TypeDef]) -> u64 {
    let mut hasher = DefaultHasher::new();

    // Temporary behaviour until https://github.com/NLnetLabs/roto/pull/5 is ready
    // Errm, that PR is merged, what do we need to change here?
    if let TypeValue::Builtin(BuiltinTypeValue::Route(route)) = value {
        let attrs = route.get_latest_attrs();

        for key in hash_keys {
            match key {
                TypeDef::AsPath => {
                    let hops = attrs.as_path.as_routecore_hops_vec();
                    for hop in hops {
                        match hop {
                            routecore::bgp::aspath::Hop::Asn(asn) => asn.hash(&mut hasher),
                            routecore::bgp::aspath::Hop::Segment(segment) => {
                                segment.hash(&mut hasher)
                            }
                        }
                    }
                }

                TypeDef::IpAddress => {
                    let prefix = Prefix::try_from(attrs.prefix.as_ref().unwrap()).unwrap();
                    prefix.addr().hash(&mut hasher);
                }

                _ => todo!(),
            }
        }
    }

    hasher.finish()
}

// fn mk_rib_record_typedef() -> Result<TypeDef, CompileError> {
//     // TODO: Generate this based on the roto script.
//     //
//     // E.g. this:
//     //
//     //     rib my-rib contains StreamRoute {
//     //         prefix: Prefix,
//     //         as-path: AsPath,
//     //         origin: Origin,
//     //         next-hop: IpAddress,
//     //         med: U32,
//     //         local-pref: U32,
//     //         community: [Community]
//     //     }
//     //
//     // Becomes Rust code to create a record type:
//     //
//     //   <rant>
//     //     for some reason absolute type definitions don't work properly so we can't do this:
//     //         let my_comms_type = TypeDef::List(Box::new(TypeDef::Community));
//     //   </rant>
//     //
//     let comms =
//         TypeValue::List(List::new(vec![ElementTypeValue::Primitive(
//             Community::new(routecore::bgp::communities::Community::from([
//                 127, 12, 13, 12,
//             ]))
//             .into(),
//         )]));

//     let my_comms_type: TypeDef = (&comms).into();

//     TypeDef::new_record_type(vec![
//         ("prefix", Box::new(TypeDef::Prefix)),
//         ("as-path", Box::new(TypeDef::AsPath)),
//         ("origin", Box::new(TypeDef::OriginType)),
//         ("next-hop", Box::new(TypeDef::IpAddress)),
//         ("med", Box::new(TypeDef::U32)),
//         ("local-pref", Box::new(TypeDef::U32)),
//         ("community", Box::new(my_comms_type)),
//     ])
// }

// --- Tests ----------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, str::FromStr};

    use roto::types::builtin::{RawRouteWithDeltas, RotondaId, RouteStatus, UpdateMessage};
    use routecore::{addr::Prefix, bgp::message::SessionConfig};

    use crate::bgp::encode::{mk_bgp_update, Announcements, Prefixes};

    use super::PhysicalRib;

    #[test]
    fn test_route_comparison_using_default_hash_key_values() {
        let rib = PhysicalRib::new("test-rib");
        let prefix = Prefix::new("127.0.0.1".parse().unwrap(), 32).unwrap();
        let peer_one = IpAddr::from_str("192.168.0.1").unwrap();
        let peer_two = IpAddr::from_str("192.168.0.2").unwrap();
        let announcement_one_from_peer_one =
            mk_route_announcement(prefix, "123,456").with_peer_ip(peer_one);
        let announcement_two_from_peer_one =
            mk_route_announcement(prefix, "789,456").with_peer_ip(peer_one);
        let announcement_one_from_peer_two =
            mk_route_announcement(prefix, "123,456").with_peer_ip(peer_two);
        let announcement_two_from_peer_two =
            mk_route_announcement(prefix, "789,456").with_peer_ip(peer_two);

        let hash_code_route_one_peer_one =
            rib.precompute_hash_code(&announcement_one_from_peer_one.into());
        let hash_code_route_one_peer_two =
            rib.precompute_hash_code(&announcement_one_from_peer_two.into());
        let hash_code_route_two_peer_one =
            rib.precompute_hash_code(&announcement_two_from_peer_one.into());
        let hash_code_route_two_peer_two =
            rib.precompute_hash_code(&announcement_two_from_peer_two.into());

        assert_ne!(
            hash_code_route_one_peer_one, hash_code_route_one_peer_two,
            "Routes that differ only by peer IP should be considered different"
        );
        assert_ne!(
            hash_code_route_two_peer_one, hash_code_route_two_peer_two,
            "Routes that differ only by peer IP should be considered different"
        );
        assert_ne!(
            hash_code_route_one_peer_one, hash_code_route_two_peer_one,
            "Routes that differ only by AS path should be considered different"
        );
        assert_ne!(
            hash_code_route_one_peer_two, hash_code_route_two_peer_two,
            "Routes that differ only by AS path should be considered different"
        );

        // Sanity checks
        assert_eq!(hash_code_route_one_peer_one, hash_code_route_one_peer_one);
        assert_eq!(hash_code_route_one_peer_two, hash_code_route_one_peer_two);
        assert_eq!(hash_code_route_two_peer_one, hash_code_route_two_peer_one);
        assert_eq!(hash_code_route_two_peer_two, hash_code_route_two_peer_two);
    }

    fn mk_route_announcement(prefix: Prefix, as_path: &str) -> RawRouteWithDeltas {
        let delta_id = (RotondaId(0), 0);
        let announcements = Announcements::from_str(&format!(
            "e [{as_path}] 10.0.0.1 BLACKHOLE,123:44 {}",
            prefix
        ))
        .unwrap();
        let bgp_update_bytes = mk_bgp_update(&Prefixes::default(), &announcements, &[]);

        // When it is processed by this unit
        let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        RawRouteWithDeltas::new_with_message(
            delta_id,
            prefix.into(),
            bgp_update_msg,
            RouteStatus::InConvergence,
        )
    }

    fn mk_route_withdrawal(prefix: Prefix) -> RawRouteWithDeltas {
        let delta_id = (RotondaId(0), 0);
        let bgp_update_bytes =
            mk_bgp_update(&Prefixes::new(vec![prefix]), &Announcements::None, &[]);

        // When it is processed by this unit
        let bgp_update_msg = UpdateMessage::new(bgp_update_bytes, SessionConfig::modern());
        RawRouteWithDeltas::new_with_message(
            delta_id,
            prefix.into(),
            bgp_update_msg,
            RouteStatus::Withdrawn,
        )
    }
}
