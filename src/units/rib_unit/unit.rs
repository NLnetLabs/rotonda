use crate::{
    common::{
        frim::FrimMap,
        roto::{FilterName, RotoScripts, ThreadLocalVM},
        status_reporter::{AnyStatusReporter, UnitStatusReporter},
    },
    comms::{
        AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus, Link,
        Terminated, TriggerData,
    },
    manager::{Component, WaitPoint},
    payload::{
        FilterError, Filterable, Payload, RouterId, Update, UpstreamStatus,
    },
    tokio::TokioTaskMetrics,
    tracing::{BoundTracer, Tracer},
    units::Unit,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;

use chrono::Utc;
use hash_hasher::{HashBuildHasher, HashedSet};
use log::{error, log_enabled, trace, warn};
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
use std::{
    cell::RefCell, ops::Deref, str::FromStr, string::ToString, sync::Arc,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{
    http::PrefixesApi,
    metrics::RibUnitMetrics,
    rib::{
        HashedRib, PreHashedTypeValue, RibValue, RouteExtra,
        StoreEvictionPolicy, StoreInsertionEffect, StoreMergeUpdateSettings,
    },
    status_reporter::RibUnitStatusReporter,
};
use super::{
    rib::StoreInsertionReport, statistics::RibMergeUpdateStatistics,
};

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
    /// is automatically injected as the vrib_upstream value in the RibUnit config below by the config loading
    /// process so that it can be used to send a GateCommand::Query message upstream to the physical Rib unit that owns
    /// the Gate that the Link refers to.
    Virtual,

    /// The index (zero-based) indicates how far from the physical RIB this vRIB is. This is used to suffix the HTTP API
    /// path differently for each vRIB compared to each other and the pRIB.
    GeneratedVirtual(u8),
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

    /// The name of the Roto filter to execute.
    /// Note: Due to a special hack in `config.rs` the user can actually supply a collection of fileter names here.
    /// Additional RibUnit instances will be spawned for the additional filter names with each additional unit
    /// wired up downstream from this one with its `rib_type` set to `Virtual`.
    #[serde(default)]
    pub filter_name: Option<FilterName>,

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
            self.filter_name.unwrap_or_default(),
            self.rib_type,
            &self.rib_keys,
            self.vrib_upstream,
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
    roto_scripts: RotoScripts,
    gate: Arc<Gate>,
    #[allow(dead_code)]
    // A strong ref needs to be held to http_processor but not used otherwise the HTTP resource manager will discard its registration
    http_processor: Arc<PrefixesApi>,
    query_limits: Arc<ArcSwap<QueryLimits>>,
    rib: Arc<ArcSwap<HashedRib>>,
    rib_type: RibType,
    filter_name: Arc<ArcSwap<FilterName>>,
    pending_vrib_query_results: Arc<PendingVirtualRibQueryResults>,
    rib_merge_update_stats: Arc<RibMergeUpdateStatistics>,
    status_reporter: Arc<RibUnitStatusReporter>,
    _process_metrics: Arc<TokioTaskMetrics>,
    tracer: Arc<Tracer>,
}

#[async_trait]
impl DirectUpdate for RibUnitRunner {
    async fn direct_update(&self, update: Update) {
        if let Err(err) = self
            .process_update(update, |pfx, meta, store| {
                store.insert(pfx, meta)
            })
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
pub type PendingVirtualRibQueryResults =
    FrimMap<QueryId, Arc<QueryOperationResultSender>>;

impl RibUnitRunner {
    thread_local!(
        static VM: ThreadLocalVM = RefCell::new(None);
    );

    #[allow(clippy::too_many_arguments)]
    fn new(
        gate: Gate,
        mut component: Component,
        http_api_path: String,
        query_limits: QueryLimits,
        filter_name: FilterName,
        rib_type: RibType,
        rib_keys: &[RouteToken],
        vrib_upstream: Option<Link>,
    ) -> Self {
        let unit_name = component.name().clone();
        let gate = Arc::new(gate);
        let rib = Self::mk_rib(
            rib_type,
            rib_keys,
            StoreEvictionPolicy::UpdateStatusOnWithdraw.into(),
        );
        let rib_merge_update_stats: Arc<RibMergeUpdateStatistics> =
            Default::default();
        let pending_vrib_query_results = Arc::new(FrimMap::default());

        // Setup metrics
        let _process_metrics = Arc::new(TokioTaskMetrics::new());
        component.register_metrics(_process_metrics.clone());

        let metrics = Arc::new(RibUnitMetrics::new(
            &gate,
            rib_merge_update_stats.clone(),
        ));
        component.register_metrics(metrics.clone());

        // Setup status reporting
        let status_reporter =
            Arc::new(RibUnitStatusReporter::new(&unit_name, metrics.clone()));

        // Setup the Roto filter source
        let filter_name = Arc::new(ArcSwap::from_pointee(filter_name));

        // Setup REST API endpoint. vRIBs listen at the vRIB HTTP prefix + /n/ where n is the index assigned to the vRIB
        // during configuration post-processing.
        let (http_api_path, is_sub_resource) =
            Self::http_api_path_for_rib_type(&http_api_path, rib_type);
        let query_limits = Arc::new(ArcSwap::from_pointee(query_limits));
        let http_processor = PrefixesApi::new(
            rib.clone(),
            http_api_path.clone(),
            query_limits.clone(),
            rib_type,
            vrib_upstream,
            pending_vrib_query_results.clone(),
        );
        let http_processor = Arc::new(http_processor);
        if is_sub_resource {
            component.register_sub_http_resource(
                http_processor.clone(),
                &http_api_path,
            );
        } else {
            component.register_http_resource(
                http_processor.clone(),
                &http_api_path,
            );
        }

        let roto_scripts = component.roto_scripts().clone();
        let tracer = component.tracer().clone();

        Self {
            roto_scripts,
            gate,
            http_processor,
            query_limits,
            rib,
            rib_type,
            status_reporter,
            filter_name,
            pending_vrib_query_results,
            _process_metrics,
            rib_merge_update_stats,
            tracer,
        }
    }

    #[cfg(test)]
    pub(crate) fn mock(
        roto_script: &str,
        rib_type: RibType,
        settings: StoreMergeUpdateSettings,
    ) -> (Self, crate::comms::GateAgent) {
        use crate::common::roto::RotoScriptOrigin;

        let roto_scripts = RotoScripts::default();
        if !roto_script.is_empty() {
            roto_scripts
                .add_or_update_script(
                    RotoScriptOrigin::Named("mock".to_string()),
                    roto_script,
                )
                .unwrap();
        }
        let (gate, gate_agent) = Gate::new(0);
        let gate = gate.into();
        let query_limits =
            Arc::new(ArcSwap::from_pointee(QueryLimits::default()));
        let rib_keys = RibUnit::default_rib_keys();
        let rib = Self::mk_rib(rib_type, &rib_keys, settings);
        let status_reporter = RibUnitStatusReporter::default().into();
        let pending_vrib_query_results = Arc::new(FrimMap::default());
        let filter_name =
            Arc::new(ArcSwap::from_pointee(FilterName::default()));
        let _process_metrics = Arc::new(TokioTaskMetrics::new());
        let rib_merge_update_stats: Arc<RibMergeUpdateStatistics> =
            Default::default();
        let http_processor = Arc::new(PrefixesApi::new(
            rib.clone(),
            Arc::new("dummy".to_string()),
            query_limits.clone(),
            rib_type,
            None,
            pending_vrib_query_results.clone(),
        ));
        let tracer = Arc::new(Tracer::new());

        let runner = Self {
            roto_scripts,
            gate,
            http_processor,
            query_limits,
            rib,
            rib_type,
            status_reporter,
            filter_name,
            pending_vrib_query_results,
            _process_metrics,
            rib_merge_update_stats,
            tracer,
        };

        (runner, gate_agent)
    }

    fn http_api_path_for_rib_type(
        http_api_path: &str,
        rib_type: RibType,
    ) -> (Arc<String>, bool) {
        let http_api_path = http_api_path.trim_end_matches('/').to_string();
        let (http_api_path, is_sub_resource) = match rib_type {
            RibType::Physical | RibType::Virtual => {
                (Arc::new(format!("{http_api_path}/")), false)
            }
            RibType::GeneratedVirtual(index) => {
                (Arc::new(format!("{http_api_path}/{index}/")), true)
            }
        };
        (http_api_path, is_sub_resource)
    }

    fn mk_rib(
        rib_type: RibType,
        rib_keys: &[RouteToken],
        settings: StoreMergeUpdateSettings,
    ) -> Arc<ArcSwap<HashedRib>> {
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
        let physical = matches!(rib_type, RibType::Physical);
        let rib = HashedRib::new(rib_keys, physical, settings);
        Arc::new(ArcSwap::from_pointee(rib))
    }

    #[cfg(test)]
    pub(super) fn status_reporter(&self) -> Arc<RibUnitStatusReporter> {
        self.status_reporter.clone()
    }

    #[cfg(test)]
    pub(super) fn gate(&self) -> Arc<Gate> {
        self.gate.clone()
    }

    #[cfg(test)]
    pub(super) fn rib(&self) -> Arc<HashedRib> {
        self.rib.load().clone()
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
                                    filter_name: new_filter_name,
                                    http_api_path: new_http_api_path,
                                    rib_keys: new_rib_keys,
                                    rib_type: new_rib_type,
                                    vrib_upstream: new_vrib_upstream,
                                }),
                        } => {
                            arc_self.status_reporter.reconfigured();

                            let old_http_api_path =
                                arc_self.http_processor.http_api_path();
                            let (new_http_api_path, _is_sub_resource) =
                                Self::http_api_path_for_rib_type(
                                    &new_http_api_path,
                                    new_rib_type,
                                );
                            if new_http_api_path.as_str() != old_http_api_path
                            {
                                warn!(
                                    "Ignoring changed http_api_path: {} -> {}",
                                    old_http_api_path, new_http_api_path
                                );
                            }

                            let old_rib_keys =
                                arc_self.rib.load().key_fields();
                            if new_rib_keys.as_slice() != old_rib_keys {
                                warn!(
                                    "Ignoring changed rib_keys: {:?} -> {:?}",
                                    old_rib_keys, new_rib_keys
                                );
                            }

                            if new_rib_type != arc_self.rib_type {
                                warn!(
                                    "Ignoring changed rib_type: {:?} -> {:?}",
                                    arc_self.rib_type, new_rib_type
                                );
                            }

                            // Replace the vRIB upstream link with the new one
                            arc_self
                                .http_processor
                                .set_vrib_upstream(new_vrib_upstream);

                            // Replace the roto script with the new one
                            let old_filter_name =
                                &*arc_self.filter_name.load();
                            match new_filter_name {
                                Some(new_filter_name) => {
                                    if old_filter_name.as_ref()
                                        != &new_filter_name
                                    {
                                        arc_self
                                            .status_reporter
                                            .filter_name_changed(
                                                old_filter_name,
                                                Some(&new_filter_name),
                                            );
                                        arc_self
                                            .filter_name
                                            .store(new_filter_name.into());
                                    }
                                }
                                None => {
                                    if old_filter_name.as_ref()
                                        != &FilterName::default()
                                    {
                                        arc_self
                                            .status_reporter
                                            .filter_name_changed(
                                                old_filter_name,
                                                None,
                                            );
                                        arc_self.filter_name.store(
                                            FilterName::default().into(),
                                        );
                                    }
                                }
                            }

                            arc_self
                                .query_limits
                                .store(Arc::new(new_query_limits));

                            // Register as a direct update receiver with the new
                            // set of linked gates.
                            arc_self
                                .status_reporter
                                .upstream_sources_changed(
                                    sources.len(),
                                    new_sources.len(),
                                );
                            sources = new_sources;
                            for link in sources.iter_mut() {
                                link.connect(arc_self.clone(), false)
                                    .await
                                    .unwrap();
                            }
                        }

                        GateStatus::ReportLinks { report } => {
                            report.set_sources(&sources);
                            report.set_graph_status(arc_self.gate.metrics());
                        }

                        GateStatus::Triggered {
                            data:
                                TriggerData::MatchPrefix(
                                    uuid,
                                    prefix,
                                    match_options,
                                ),
                        } => {
                            assert!(matches!(
                                arc_self.rib_type,
                                RibType::Physical
                            ));
                            let res = {
                                if let Ok(store) = arc_self.rib.load().store()
                                {
                                    let guard = &epoch::pin();
                                    trace!(
                                        "Performing triggered query {uuid}"
                                    );
                                    Ok(store.match_prefix(
                                        &prefix,
                                        &match_options,
                                        guard,
                                    ))
                                } else {
                                    Err("Cannot query non-existent RIB"
                                        .to_string())
                                }
                            };
                            trace!("Sending query {uuid} results downstream");
                            arc_self
                                .gate
                                .update_data(Update::QueryResult(uuid, res))
                                .await;
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

    pub(super) async fn process_update<F>(
        &self,
        update: Update,
        insert_fn: F,
    ) -> Result<(), FilterError>
    where
        F: Fn(
                &Prefix,
                TypeValue,
                &HashedRib,
            ) -> Result<
                (Upsert<<RibValue as MergeUpdate>::UserDataOut>, u32),
                PrefixStoreError,
            > + Copy
            + Clone
            + Send
            + 'static,
    {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream {
                ..
            }) => {
                // Nothing to do, pass it on
                self.gate.update_data(update).await;
            }

            Update::Bulk(payloads) => {
                self.filter_payload(payloads, insert_fn).await?
            }

            Update::Single(payload) => {
                self.filter_payload(payload, insert_fn).await?
            }

            Update::QueryResult(uuid, upstream_query_result) => {
                trace!("Re-processing received query {uuid} result");
                let processed_res = match upstream_query_result {
                    Ok(res) => Ok(self.reprocess_query_results(res).await),
                    Err(err) => Err(err),
                };

                // Were we waiting for this result?
                if let Some(tx) =
                    self.pending_vrib_query_results.remove(&uuid)
                {
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

    async fn filter_payload<T, F>(
        &self,
        payload: T,
        insert_fn: F,
    ) -> Result<(), FilterError>
    where
        T: Filterable,
        F: Fn(
                &Prefix,
                TypeValue,
                &HashedRib,
            ) -> Result<
                (Upsert<<RibValue as MergeUpdate>::UserDataOut>, u32),
                PrefixStoreError,
            > + Copy
            + Clone
            + Send
            + 'static,
    {
        let bound_tracer = self.tracer.bind(self.gate.id());
        if let Some(filtered_update) = Self::VM
            .with(|vm| {
                payload
                    .filter(|value, received, trace_id| {
                        self.roto_scripts.exec_with_tracer(
                            vm,
                            &self.filter_name.load(),
                            value,
                            received,
                            bound_tracer.clone(),
                            trace_id,
                        )
                    },
                    |_source_id| { /* TODO: self.status_reporter.message_filtered(source_id) */ })
                    .map(|filtered_payloads| {
                        self.insert_payloads(filtered_payloads, insert_fn)
                    })
            })
            .map_err(|err| {
                self.status_reporter.message_filtering_failure(&err);
                err
            })?
        {
            self.gate.update_data(filtered_update).await;
        }

        if let Ok(rib) = self.rib.load().store() {
            self.status_reporter
                .unique_prefix_count_updated(rib.prefixes_count());
        }

        Ok(())
    }

    pub fn insert_payloads<F>(
        &self,
        mut payloads: SmallVec<[Payload; 8]>,
        insert_fn: F,
    ) -> Option<Update>
    where
        F: Fn(
                &Prefix,
                TypeValue,
                &HashedRib,
            ) -> Result<
                (Upsert<<RibValue as MergeUpdate>::UserDataOut>, u32),
                PrefixStoreError,
            > + Copy
            + Send
            + 'static,
    {
        for payload in &payloads {
            self.insert_payload(payload, insert_fn);
        }

        match payloads.len() {
            0 => None,
            1 => Some(Update::Single(payloads.pop().unwrap())),
            _ => Some(Update::Bulk(payloads)),
        }
    }

    pub fn insert_payload<F>(&self, payload: &Payload, insert_fn: F)
    where
        F: Fn(
                &Prefix,
                TypeValue,
                &HashedRib,
            ) -> Result<
                (Upsert<<RibValue as MergeUpdate>::UserDataOut>, u32),
                PrefixStoreError,
            > + Send
            + 'static,
    {
        let rib = self.rib.load();
        if rib.is_physical() {
            let prefix: Option<Prefix> = match &payload.value {
                TypeValue::Builtin(BuiltinTypeValue::Route(route)) => {
                    Some(route.prefix)
                }

                TypeValue::Record(record) => match record
                    .get_value_for_field("prefix")
                {
                    Some(ElementTypeValue::Primitive(
                        TypeValue::Builtin(BuiltinTypeValue::Prefix(prefix)),
                    )) => Some(*prefix),
                    _ => None,
                },

                _ => None,
            };

            if let Some(prefix) = prefix {
                let is_announcement = !payload.value.is_withdrawn();
                let router_id =
                    payload.value.router_id().unwrap_or_else(|| {
                        Arc::new(RouterId::from_str("unknown").unwrap())
                    });

                let pre_insert = Utc::now();
                match insert_fn(&prefix, payload.value.clone(), &rib) {
                    Ok((upsert, num_retries)) => {
                        let post_insert = Utc::now();
                        let store_op_delay =
                            (post_insert - pre_insert).to_std().unwrap();
                        let propagation_delay = (post_insert
                            - payload.received)
                            .to_std()
                            .unwrap();

                        match upsert {
                            Upsert::Insert => {
                                let change = if is_announcement {
                                    StoreInsertionEffect::RouteAdded
                                } else {
                                    // WTF - a withdrawal should NOT result in 'Insert' as that means
                                    // that the prefix didn't yet have any routes associated with it
                                    // so what was there to withdraw?
                                    StoreInsertionEffect::RoutesWithdrawn(0)
                                };
                                self.status_reporter.insert_ok(
                                    router_id,
                                    store_op_delay,
                                    propagation_delay,
                                    num_retries,
                                    change,
                                );
                            }
                            Upsert::Update(StoreInsertionReport {
                                change,
                                item_count,
                                op_duration,
                            }) => {
                                STATS_COUNTER.with(|counter| {
                                    *counter.borrow_mut() += 1;
                                    let res = *counter.borrow();
                                    if res % RECORD_MERGE_UPDATE_SPEED_EVERY_N_CALLS == 0 {
                                        self.rib_merge_update_stats.add(
                                            op_duration.num_microseconds().unwrap_or(i64::MAX)
                                                as u64,
                                            item_count,
                                            is_announcement,
                                        );
                                    }
                                });

                                self.status_reporter.update_ok(
                                    router_id,
                                    store_op_delay,
                                    propagation_delay,
                                    num_retries,
                                    change,
                                );
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

    async fn reprocess_query_results(
        &self,
        res: QueryResult<RibValue>,
    ) -> QueryResult<RibValue> {
        let mut processed_res = QueryResult::<RibValue> {
            match_type: res.match_type,
            prefix: res.prefix,
            prefix_meta: None,
            less_specifics: None,
            more_specifics: None,
        };

        let is_in_prefix_meta_set = res.prefix_meta.is_some();

        if let Some(rib_value) = res.prefix_meta {
            processed_res.prefix_meta =
                self.reprocess_rib_value(rib_value).await;
        }

        if let Some(record_set) = &res.less_specifics {
            processed_res.less_specifics =
                self.reprocess_record_set(record_set).await;
        }

        if let Some(record_set) = &res.more_specifics {
            processed_res.more_specifics =
                self.reprocess_record_set(record_set).await;
        }

        if log_enabled!(log::Level::Trace) {
            let is_out_prefix_meta_set = processed_res.prefix_meta.is_some();
            let exact_match_diff = (is_in_prefix_meta_set as u8)
                - (is_out_prefix_meta_set as u8);
            let less_specifics_diff =
                res.less_specifics.map_or(0, |v| v.len())
                    - processed_res
                        .less_specifics
                        .as_ref()
                        .map_or(0, |v| v.len());
            let more_specifics_diff =
                res.more_specifics.map_or(0, |v| v.len())
                    - processed_res
                        .more_specifics
                        .as_ref()
                        .map_or(0, |v| v.len());
            if exact_match_diff != 0
                || less_specifics_diff != 0
                || more_specifics_diff != 0
            {
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
        &self,
        rib_value: RibValue,
    ) -> Option<RibValue> {
        let mut new_values = HashedSet::with_capacity_and_hasher(
            1,
            HashBuildHasher::default(),
        );

        let tracer = BoundTracer::new(self.tracer.clone(), self.gate.id());

        for route in rib_value.iter() {
            let in_type_value: &TypeValue = route.deref();
            let payload = Payload::from(in_type_value.clone()); // TODO: Do we really want to clone here? Or pass the Arc on?

            trace!("Re-processing route");

            if let Ok(filtered_payloads) = Self::VM.with(|vm| {
                payload.filter(|value, received, trace_id| {
                    self.roto_scripts.exec_with_tracer(
                        vm,
                        &self.filter_name.load(),
                        value,
                        received,
                        tracer.clone(),
                        trace_id,
                    )
                }, |_source_id| { /* TODO: self.status_reporter.message_filtered(source_id) */ })
            }) {
                new_values.extend(
                    filtered_payloads
                        .into_iter()
                        .filter(|payload| {
                            !matches!(
                                payload.value,
                                TypeValue::OutputStreamMessage(_)
                            )
                        })
                        .map(|payload| {
                            // Add this processed query result route into the new query result
                            let hash = self
                                .rib
                                .load()
                                .precompute_hash_code(&payload.value);
                            PreHashedTypeValue::new(payload.value, hash)
                                .into()
                        }),
                );
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
            if let Some(rib_value) = self.reprocess_rib_value(rib_value).await
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
        toml::from_str::<RibUnit>(toml)
    }
}
