use crate::{
    common::{
        file_io::{FileIo, TheFileIo},
        frim::FrimMap,
        roto::ThreadLocalVM,
        status_reporter::{AnyStatusReporter, UnitStatusReporter},
    },
    comms::{
        AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus, Link, Terminated, TriggerData,
    },
    manager::{Component, WaitPoint},
    payload::{Filterable, Payload, RouterId, Update, UpstreamStatus},
    tokio::TokioTaskMetrics,
    units::Unit,
};
use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;

use chrono::Utc;
use hash_hasher::{HashBuildHasher, HashedSet};
use log::{error, log_enabled, trace};
use non_empty_vec::NonEmpty;
use roto::types::{
    builtin::{BuiltinTypeValue, RouteToken},
    collections::ElementTypeValue,
    typevalue::TypeValue,
};
use rotonda_store::{
    custom_alloc::Upsert,
    epoch,
    prelude::{multi::PrefixStoreError, MergeUpdate},
    QueryResult, RecordSet,
};

use routecore::addr::Prefix;
use serde::Deserialize;
use smallvec::SmallVec;
use std::{cell::RefCell, ops::Deref, path::PathBuf, str::FromStr, string::ToString, sync::Arc};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{
    http::PrefixesApi,
    metrics::RibUnitMetrics,
    rib::{PhysicalRib, PreHashedTypeValue, RibValue, RouteExtra},
    status_reporter::RibUnitStatusReporter,
};
use super::{rib::StoreInsertionReport, statistics::RibMergeUpdateStatistics};

use std::time::Instant;

const RECORD_MERGE_UPDATE_SPEED_EVERY_N_CALLS: u64 = 1000;

thread_local!(
    static STATS_COUNTER: RefCell<u64> = RefCell::new(0);
);

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

    /// Which fields are the key for RIB metadata items?
    #[serde(default = "RibUnit::default_rib_keys")]
    pub rib_keys: NonEmpty<RouteToken>,

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
        RibUnitRunner::new(
            gate,
            component,
            self.http_api_path,
            self.query_limits,
            self.roto_path,
            self.rib_type,
            &self.rib_keys,
            self.vrib_upstream,
            TheFileIo::default(),
        )
        .run(self.sources, waitpoint)
        .await
    }

    fn default_http_api_path() -> String {
        "/prefixes/".to_string()
    }

    fn default_query_limits() -> QueryLimits {
        QueryLimits::default()
    }

    fn default_rib_keys() -> NonEmpty<RouteToken> {
        NonEmpty::try_from(vec![
            RouteToken::PeerIp,
            RouteToken::PeerAsn,
            RouteToken::AsPath,
        ])
        .unwrap()
    }
}

pub struct RibUnitRunner {
    gate: Arc<Gate>,
    #[allow(dead_code)] // A strong ref needs to be held to http_processor but not used otherwise the HTTP resource manager will discard its registration
    http_processor: Arc<PrefixesApi>,
    query_limits: Arc<ArcSwap<QueryLimits>>,
    rib: Arc<ArcSwapOption<PhysicalRib>>,
    rib_type: RibType,
    roto_source: Arc<ArcSwap<(std::time::Instant, String)>>,
    pending_vrib_query_results: Arc<PendingVirtualRibQueryResults>,
    rib_merge_update_stats: Arc<RibMergeUpdateStatistics>,
    status_reporter: Arc<RibUnitStatusReporter>,
    process_metrics: Arc<TokioTaskMetrics>,
}

#[async_trait]
impl DirectUpdate for RibUnitRunner {
    async fn direct_update(&self, update: Update) {
        if let Err(err) = self
            .process_update(update, |pfx, meta, store| store.insert(pfx, meta))
            .await
        {
            error!("Error handling update: {err}");
        }
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
        mut component: Component,
        http_api_path: String,
        query_limits: QueryLimits,
        roto_path: Option<PathBuf>,
        rib_type: RibType,
        rib_keys: &[RouteToken],
        vrib_upstream: Option<Link>,
        file_io: TheFileIo,
    ) -> Self {
        let unit_name = component.name().clone();
        let gate = Arc::new(gate);
        let rib = Self::mk_rib(rib_type, rib_keys);
        let rib_merge_update_stats: Arc<RibMergeUpdateStatistics> = Default::default();
        let pending_vrib_query_results = Arc::new(FrimMap::default());

        let roto_source_code = roto_path
            .map(|v| file_io.read_to_string(v).unwrap())
            .unwrap_or_default();
        let roto_source = (Instant::now(), roto_source_code);
        let roto_source = Arc::new(ArcSwap::from_pointee(roto_source));

        let process_metrics = Arc::new(TokioTaskMetrics::new());
        component.register_metrics(process_metrics.clone());

        // Setup metrics
        let metrics = Arc::new(RibUnitMetrics::new(&gate, rib_merge_update_stats.clone()));
        component.register_metrics(metrics.clone());

        // Setup REST API endpoint. vRIBs listen at the vRIB HTTP prefix + /n/ where n is the index assigned to the vRIB
        // during configuration post-processing.
        let http_api_path = http_api_path.trim_end_matches('/').to_string();
        let (http_api_path, is_sub_resource) = match rib_type {
            RibType::Physical => (Arc::new(format!("{http_api_path}/")), false),
            RibType::Virtual(index) => (Arc::new(format!("{http_api_path}/{index}/")), true),
        };
        let query_limits = Arc::new(ArcSwap::from_pointee(query_limits));
        let http_processor = PrefixesApi::new(
            rib.clone(),
            http_api_path,
            query_limits.clone(),
            rib_type,
            vrib_upstream,
            pending_vrib_query_results.clone(),
        );
        let http_processor = Arc::new(http_processor);
        if is_sub_resource {
            component.register_sub_http_resource(http_processor.clone());
        } else {
            component.register_http_resource(http_processor.clone());
        }

        // Setup status reporting
        let status_reporter = Arc::new(RibUnitStatusReporter::new(&unit_name, metrics.clone()));

        Self {
            gate,
            http_processor,
            query_limits,
            rib,
            rib_type,
            status_reporter,
            roto_source,
            pending_vrib_query_results,
            process_metrics,
            rib_merge_update_stats,
        }
    }

    #[cfg(test)]
    pub(crate) fn mock(roto_source_code: &str, rib_type: RibType) -> Self {
        let (gate, _) = Gate::new(0);
        let gate = gate.into();
        let query_limits = Arc::new(ArcSwap::from_pointee(QueryLimits::default()));
        let rib_keys = RibUnit::default_rib_keys();
        let rib = Self::mk_rib(rib_type, &rib_keys);
        let status_reporter = RibUnitStatusReporter::default().into();
        let roto_source = (Instant::now(), roto_source_code.into());
        let roto_source = Arc::new(ArcSwap::from_pointee(roto_source));
        let pending_vrib_query_results = Arc::new(FrimMap::default());
        let process_metrics = Arc::new(TokioTaskMetrics::new());
        let rib_merge_update_stats: Arc<RibMergeUpdateStatistics> = Default::default();
        let http_processor = Arc::new(PrefixesApi::new(
            rib.clone(),
            Arc::new("dummy".to_string()),
            query_limits.clone(),
            rib_type,
            None,
            pending_vrib_query_results.clone(),
        ));

        Self {
            gate,
            http_processor,
            query_limits,
            rib,
            rib_type,
            status_reporter,
            roto_source,
            pending_vrib_query_results,
            process_metrics,
            rib_merge_update_stats,
        }
    }

    fn mk_rib(rib_type: RibType, rib_keys: &[RouteToken]) -> Arc<ArcSwapOption<PhysicalRib>> {
        match rib_type {
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
                Arc::new(ArcSwapOption::from_pointee(PhysicalRib::new(rib_keys)))
            }

            RibType::Virtual(_) => Arc::new(ArcSwapOption::empty()),
        }
    }

    #[cfg(test)]
    pub(super) fn gate(&self) -> Arc<Gate> {
        self.gate.clone()
    }

    #[cfg(test)]
    pub(super) fn rib(&self) -> Option<Arc<PhysicalRib>> {
        Option::clone(&self.rib.load())
    }

    pub async fn run(
        self,
        mut sources: NonEmpty<DirectLink>,
        mut waitpoint: WaitPoint,
    ) -> Result<(), Terminated> {
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
                    arc_self.status_reporter.gate_status_announced(&status);
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
                            arc_self.status_reporter.reconfigured();

                            arc_self.query_limits.store(Arc::new(new_query_limits));

                            // Register as a direct update receiver with the new
                            // set of linked gates.
                            arc_self.status_reporter.upstream_sources_changed(sources.len(), new_sources.len());
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
                            assert!(matches!(arc_self.rib_type, RibType::Physical));
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
                    arc_self.status_reporter.terminated();
                    return Err(Terminated);
                }
            }
        }
    }

    pub(super) async fn process_update<F>(&self, update: Update, insert_fn: F) -> Result<(), String>
    where
        F: Fn(
                &Prefix,
                TypeValue,
                &PhysicalRib,
            )
                -> Result<(Upsert<<RibValue as MergeUpdate>::UserDataOut>, u32), PrefixStoreError>
            + Copy
            + Clone
            + Send
            + 'static,
    {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream { .. }) => {
                // Nothing to do, pass it on
                self.gate.update_data(update).await;
            }

            Update::Bulk(payloads) => {
                if let Some(filtered_update) = Self::VM.with(|vm| {
                    let filter_fn =
                        |value| crate::common::roto::filter(vm, self.roto_source.clone(), value);
                    payloads
                        .filter(filter_fn.clone())
                        .and_then(|filtered_payloads| {
                            self.insert_payloads(filtered_payloads, insert_fn)
                        })
                })? {
                    self.gate.update_data(filtered_update).await;
                }

                if let Some(rib) = self.rib.load().as_ref() {
                    self.status_reporter
                        .unique_prefix_count_updated(rib.prefixes_count());
                }
            }

            Update::Single(payload) => {
                // TODO: update status reporter/metrics as is done in the bulk case
                if let Some(filtered_update) = Self::VM.with(|vm| {
                    let filter_fn =
                        |value| crate::common::roto::filter(vm, self.roto_source.clone(), value);
                    payload.filter(filter_fn).and_then(|filtered_payloads| {
                        self.insert_payloads(filtered_payloads, insert_fn)
                    })
                })? {
                    self.gate.update_data(filtered_update).await;
                }
            }

            Update::QueryResult(uuid, upstream_query_result) => {
                trace!("Re-processing received query {uuid} result");
                let processed_res = match upstream_query_result {
                    Ok(res) => Ok(self.reprocess_query_results(res).await),
                    Err(err) => Err(err),
                };

                // Were we waiting for this result?
                if let Some(tx) = self.pending_vrib_query_results.remove(&uuid) {
                    // Yes, send the result to the waiting HTTP request processing task
                    trace!("Notifying waiting HTTP request processor of query {uuid} results");
                    let tx = Arc::try_unwrap(tx).unwrap(); // TODO: handle this unwrap
                    tx.send(processed_res).unwrap(); // TODO: handle this unwrap
                } else {
                    // No, pass it on to the next virtual RIB
                    trace!("Sending re-processed triggered query {uuid} results downstream");
                    self.gate
                        .update_data(Update::QueryResult(uuid, processed_res))
                        .await;
                }
            }
        }

        Ok(())
    }

    pub fn insert_payloads<F>(
        &self,
        mut payloads: SmallVec<[Payload; 8]>,
        insert_fn: F,
    ) -> Result<Option<Update>, String>
    where
        F: Fn(
                &Prefix,
                TypeValue,
                &PhysicalRib,
            )
                -> Result<(Upsert<<RibValue as MergeUpdate>::UserDataOut>, u32), PrefixStoreError>
            + Copy
            + Send
            + 'static,
    {
        for payload in &payloads {
            self.process_metrics
                .instrument(self.insert_payload(&payload, insert_fn));
        }

        let update = match payloads.len() {
            0 => None,
            1 => Some(Update::Single(payloads.pop().unwrap())),
            _ => Some(Update::Bulk(payloads)),
        };

        Ok(update)
    }

    pub fn insert_payload<F>(&self, payload: &Payload, insert_fn: F)
    where
        F: Fn(
                &Prefix,
                TypeValue,
                &PhysicalRib,
            )
                -> Result<(Upsert<<RibValue as MergeUpdate>::UserDataOut>, u32), PrefixStoreError>
            + Send
            + 'static,
    {
        // Only physical RIBs have a store to insert into.
        if let Some(rib) = self.rib.load().as_ref() {
            let prefix: Option<Prefix> = match &payload.value {
                TypeValue::Builtin(BuiltinTypeValue::Route(route)) => Some(route.prefix.into()),

                TypeValue::Record(record) => match record.get_value_for_field("prefix") {
                    Some(ElementTypeValue::Primitive(TypeValue::Builtin(
                        BuiltinTypeValue::Prefix(prefix),
                    ))) => Some((*prefix).into()),
                    _ => None,
                },

                _ => None,
            };

            if let Some(prefix) = prefix {
                let is_withdraw = payload.value.is_withdrawn();

                let pre_insert = Utc::now();
                match insert_fn(&prefix, payload.value.clone(), rib) {
                    Ok((upsert, num_retries)) => {
                        let post_insert = Utc::now();
                        let insert_delay = (post_insert - pre_insert)
                            .num_microseconds()
                            .unwrap_or(i64::MAX);

                        // TODO: Use RawBgpMessage LogicalTime?
                        // let propagation_delay = (post_insert - rib_el.received).num_milliseconds();
                        let propagation_delay = 0;

                        let router_id =
                            Arc::new(RouterId::from_str("not implemented yet").unwrap());

                        match upsert {
                            Upsert::Insert => {
                                self.status_reporter.insert_ok(
                                    router_id,
                                    insert_delay,
                                    propagation_delay,
                                    num_retries,
                                    is_withdraw,
                                    1,
                                );
                            }
                            Upsert::Update(StoreInsertionReport {
                                item_count_delta,
                                item_count_total,
                                op_duration,
                            }) => {
                                STATS_COUNTER.with(|counter| {
                                    *counter.borrow_mut() += 1;
                                    let res = *counter.borrow();
                                    if res % RECORD_MERGE_UPDATE_SPEED_EVERY_N_CALLS == 0 {
                                        self.rib_merge_update_stats.add(
                                            op_duration.num_microseconds().unwrap_or(i64::MAX)
                                                as u64,
                                            item_count_total,
                                            is_withdraw,
                                        );
                                    }
                                });

                                self.status_reporter.update_ok(
                                    router_id,
                                    insert_delay,
                                    propagation_delay,
                                    num_retries,
                                    item_count_delta,
                                );

                                // status_reporter.update_processed(
                                //     new_announcements,
                                //     modified_announcements,
                                //     new_withdrawals,
                                // );
                            }
                        }
                    }

                    Err(err) => {
                        self.status_reporter.insert_failed(prefix, err);
                    }
                }
            }
        }
    }

    async fn reprocess_query_results(&self, res: QueryResult<RibValue>) -> QueryResult<RibValue> {
        let mut processed_res = QueryResult::<RibValue> {
            match_type: res.match_type,
            prefix: res.prefix,
            prefix_meta: None,
            less_specifics: None,
            more_specifics: None,
        };

        let is_in_prefix_meta_set = res.prefix_meta.is_some();

        if let Some(rib_value) = res.prefix_meta {
            processed_res.prefix_meta = self.reprocess_rib_value(rib_value).await;
        }

        if let Some(record_set) = &res.less_specifics {
            processed_res.less_specifics = self.reprocess_record_set(record_set).await;
        }

        if let Some(record_set) = &res.more_specifics {
            processed_res.more_specifics = self.reprocess_record_set(record_set).await;
        }

        if log_enabled!(log::Level::Trace) {
            let is_out_prefix_meta_set = processed_res.prefix_meta.is_some();
            let exact_match_diff = (is_in_prefix_meta_set as u8) - (is_out_prefix_meta_set as u8);
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
    async fn reprocess_rib_value(&self, rib_value: RibValue) -> Option<RibValue> {
        let mut new_values = HashedSet::with_capacity_and_hasher(1, HashBuildHasher::default());

        for route in rib_value.iter() {
            let in_type_value: &TypeValue = route.deref();
            let payload = Payload::from(in_type_value.clone()); // TODO: Do we really want to clone here? Or pass the Arc on?

            trace!("Reprocessing route");

            if let Ok(filtered_payloads) = Self::VM.with(|vm| {
                let filter_fn =
                    |value| crate::common::roto::filter(vm, self.roto_source.clone(), value);
                payload.filter(filter_fn)
            }) {
                for filtered_payload in filtered_payloads {
                    // Add this processed query result route into the new query result
                    let hash = self
                        .rib
                        .load()
                        .as_ref()
                        .unwrap()
                        .precompute_hash_code(&filtered_payload.value);
                    let hashed_value = PreHashedTypeValue::new(filtered_payload.value, hash);
                    new_values.insert(hashed_value.into());
                }
            }
        }

        if new_values.is_empty() {
            None
        } else {
            Some(new_values.into())
        }
    }

    async fn reprocess_record_set(
        &self,
        record_set: &RecordSet<RibValue>,
    ) -> Option<RecordSet<RibValue>> {
        let mut new_record_set = RecordSet::<RibValue>::new();

        for record in record_set.iter() {
            let rib_value = record.meta;
            if let Some(rib_value) = self.reprocess_rib_value(rib_value).await {
                new_record_set.push(record.prefix, rib_value);
            }
        }

        if !new_record_set.is_empty() {
            Some(new_record_set)
        } else {
            None
        }
    }
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
    use super::*;

    #[test]
    fn default_rib_keys_are_as_expected() {
        let toml = r#"
        sources = ["some source"]
        "#;

        let config = mk_config_from_toml(toml).unwrap();

        assert_eq!(
            config.rib_keys.as_slice(),
            &[RouteToken::PeerIp, RouteToken::PeerAsn, RouteToken::AsPath]
        );
    }

    #[test]
    fn specified_rib_keys_are_received() {
        let toml = r#"
        sources = ["some source"]
        rib_keys = ["PeerIp", "NextHop"]
        "#;

        let config = mk_config_from_toml(toml).unwrap();

        assert_eq!(
            config.rib_keys.as_slice(),
            &[RouteToken::PeerIp, RouteToken::NextHop]
        );
    }

    #[test]
    #[should_panic(expected = "empty vector")]
    fn if_specified_rib_keys_must_be_non_empty() {
        let toml = r#"
        sources = ["some source"]
        rib_keys = []
        "#;

        mk_config_from_toml(toml).unwrap();
    }

    #[test]
    #[should_panic(expected = "unknown variant")]
    fn only_known_route_tokens_may_be_used_as_rib_keys() {
        let toml = r#"
        sources = ["some source"]
        rib_keys = ["blah"]
        "#;

        mk_config_from_toml(toml).unwrap();
    }

    fn mk_config_from_toml(toml: &str) -> Result<RibUnit, toml::de::Error> {
        toml::de::from_slice::<RibUnit>(toml.as_bytes())
    }
}
