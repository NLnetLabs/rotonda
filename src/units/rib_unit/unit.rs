use crate::{
    common::{
        frim::FrimMap,
        status_reporter::{AnyStatusReporter, UnitStatusReporter},
    }, comms::{
        AnyDirectUpdate, DirectLink, DirectUpdate, Gate, GateStatus, Link,
        Terminated, TriggerData,
    }, ingress, manager::{Component, WaitPoint}, payload::{
        Payload, RotondaPaMap, RotondaRoute, RouterId, Update, UpstreamStatus,
    }, roto_runtime::{self, types::{FilterName, InsertionInfo, Output, OutputStreamMessage, RotoOutputStream, RouteContext}, Ctx}, tokio::TokioTaskMetrics, tracing::{BoundTracer, Tracer}, units::Unit
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::{collections::HashSet, io::prelude::*};

use chrono::Utc;
use hash_hasher::{HashBuildHasher, HashedSet};
use log::{debug, error, info, log_enabled, trace, warn};
use non_empty_vec::NonEmpty;
use rotonda_store::{
    custom_alloc::UpsertReport, prelude::multi::{PrefixStoreError, RouteStatus},
    QueryResult, RecordSet,
};

use inetnum::addr::Prefix;
use routecore::bgp::types::AfiSafiType;
use serde::Deserialize;
use smallvec::{smallvec, SmallVec};
use std::{
    cell::RefCell, ops::Deref, str::FromStr, string::ToString, sync::Arc,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{
    http::PrefixesApi,
    metrics::RibUnitMetrics,
    rib::{Rib, RouteExtra, StoreInsertionEffect},
    status_reporter::RibUnitStatusReporter,
};
use super::{
    rib::StoreInsertionReport, statistics::RibMergeUpdateStatistics,
};


pub(crate) type RotoFuncPre = roto::TypedFunc<
    Ctx,
    (
        roto::Val<RotondaRoute>,
        //roto::Val<RouteContext>,
    ),
    roto::Verdict<(), ()>,
>;

type RotoFuncPost = roto::TypedFunc<
    Ctx,
    (
        roto::Val<RotondaRoute>,
        roto::Val<InsertionInfo>,
    ),
    roto::Verdict<(), ()>,
>;

impl From<UpsertReport> for InsertionInfo {
    fn from(value: UpsertReport) -> Self {
        Self {
            prefix_new: value.prefix_new,
            new_peer: value.mui_new,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct MoreSpecifics {
    /// The shortest IPv4 prefix, N (from /N), for which more specific (i.e.
    /// longer prefix) matches can be queried.
    #[serde(default = "MoreSpecifics::default_shortest_prefix_ipv4")]
    pub shortest_prefix_ipv4: u8,

    /// The shortest IPv6 prefix, N (from /N), for which more specific (i.e.
    /// longer prefix) matches can be queried.
    #[serde(default = "MoreSpecifics::default_shortest_prefix_ipv6")]
    pub shortest_prefix_ipv6: u8,
}

impl MoreSpecifics {
    pub fn default_shortest_prefix_ipv4() -> u8 {
        // IPv4 space is densely populated, tree size quickly becomes very
        // large at shorter prefix lengths so limit searches to prefixes no
        // shorter than /8, i.e. /7 is not permitted, but /32 is.
        8
    }

    pub fn default_shortest_prefix_ipv6() -> u8 {
        // IPv6 space is sparsely populated compared to IPv4 space so the tree
        // is less dense at shorter prefixes than the equivalent for IPv4 and
        // so we can afford to be permit "deeper" searches for IPv6 than for
        // IPv4.
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
    /// A physical RIB has zero or one roto scripts and a prefix store.
    /// Queries to its HTTP API are answered using the local store.
    #[default]
    Physical,

    /// A virtual RIB has one roto script and no prefix store. Queries to its
    /// HTTP API are answered by sending a command to the nearest physical Rib
    /// to the West of the virtual RIB. A `Link` to the gate of that physical
    /// Rib is automatically injected as the vrib_upstream value in the
    /// RibUnit config below by the config loading process so that it can be
    /// used to send a GateCommand::Query message upstream to the physical Rib
    /// unit that owns the Gate that the Link refers to.
    Virtual,

    /// The index (zero-based) indicates how far from the physical RIB this
    /// vRIB is. This is used to suffix the HTTP API path differently for each
    /// vRIB compared to each other and the pRIB.
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

    /// The relative path at which we should listen for HTTP query API
    /// requests
    #[serde(default = "RibUnit::default_http_api_path")]
    pub http_api_path: String,

    #[serde(default = "RibUnit::default_query_limits")]
    pub query_limits: QueryLimits,

    /// The name of the Roto filter to execute. Note: Due to a special hack in
    /// `config.rs` the user can actually supply a collection of filter names
    /// here. Additional RibUnit instances will be spawned for the additional
    /// filter names with each additional unit wired up downstream from this
    /// one with its `rib_type` set to `Virtual`.
    #[serde(default)]
    pub filter_name: Option<FilterName>,

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
        RibUnitRunner::new(
            gate,
            component,
            self.http_api_path,
            self.query_limits,
            self.filter_name.unwrap_or_default(),
            self.rib_type,
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
}

#[derive(Debug, Default)]
pub struct RtrCache {
    route_origins: HashSet<rpki::rtr::payload::RouteOrigin>,
    router_keys: HashSet<rpki::rtr::payload::RouterKey>,
    aspas: HashSet<rpki::rtr::payload::Aspa>,
}

pub struct RibUnitRunner {
    roto_function_pre: Option<RotoFuncPre>,
    roto_function_post: Option<RotoFuncPost>,
    gate: Arc<Gate>,
    #[allow(dead_code)]
    // A strong ref needs to be held to http_processor but not used otherwise
    // the HTTP resource manager will discard its registration
    http_processor: Arc<PrefixesApi>,
    query_limits: Arc<ArcSwap<QueryLimits>>,
    rib: Arc<ArcSwap<Rib>>, // XXX LH: why the ArcSwap here?
    rib_type: RibType,
    rtr_cache: std::sync::RwLock<RtrCache>,
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
        if let Err(err) = self.process_update(update).await {
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
pub type QueryOperationResult = Result<QueryResult<RotondaPaMap>, String>;
pub type QueryOperationResultSender = oneshot::Sender<QueryOperationResult>;
pub type PendingVirtualRibQueryResults =
    FrimMap<QueryId, Arc<QueryOperationResultSender>>;

impl RibUnitRunner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        gate: Gate,
        mut component: Component,
        http_api_path: String,
        query_limits: QueryLimits,
        filter_name: FilterName,
        rib_type: RibType,
        vrib_upstream: Option<Link>,
    ) -> Self {
        let unit_name = component.name().clone();
        let gate = Arc::new(gate);
        let rib = Arc::new(ArcSwap::from_pointee(Rib::new_physical()));
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

        // Setup REST API endpoint. vRIBs listen at the vRIB HTTP prefix + /
        // n/ where n is the index assigned to the vRIB during configuration
        // post-processing.
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
            component.ingresses(),
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

        let roto_compiled = component.roto_compiled().clone();
        let roto_function_pre: Option<RotoFuncPre> =
            roto_compiled.clone().and_then(|c| {
                let mut c = c.lock().unwrap();
                c.get_function("rib-in-pre")
                .inspect_err(|_|
                    warn!("Loaded Roto script has no filter for rib-in-pre")
                )
                .ok()
            });

        // The rib-in-post filter is not used yet.
        let roto_function_post: Option<RotoFuncPost> = None;
        //let roto_function_post: Option<RotoFuncPost> = roto_compiled
        //    .and_then(|c| {
        //        let mut c = c.lock().unwrap();
        //        c.get_function("rib-in-post")
        //        .inspect_err(|_|
        //            warn!("Loaded Roto script has no filter for rib-in-post")
        //        )
        //        .ok()
        //    });

        let tracer = component.tracer().clone();

        Self {
            roto_function_pre,
            roto_function_post,
            gate,
            http_processor,
            query_limits,
            rib,
            rib_type,
            rtr_cache: std::sync::RwLock::new(RtrCache::default()),
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
    ) -> (Self, crate::comms::GateAgent) {
        //use crate::common::roto::RotoScriptOrigin;

        use crate::roto_runtime::types::RotoScripts;

        let roto_scripts = RotoScripts::default();
        let (gate, gate_agent) = Gate::new(0);
        let gate = gate.into();
        let query_limits =
            Arc::new(ArcSwap::from_pointee(QueryLimits::default()));
        let rib = Rib::new_physical();
        let status_reporter = RibUnitStatusReporter::default().into();
        let pending_vrib_query_results = Arc::new(FrimMap::default());
        let filter_name =
            Arc::new(ArcSwap::from_pointee(FilterName::default()));
        let _process_metrics = Arc::new(TokioTaskMetrics::new());
        let rib_merge_update_stats: Arc<RibMergeUpdateStatistics> =
            Default::default();

        let shared_rib = Arc::new(ArcSwap::new(Arc::new(rib)));
        let http_processor = Arc::new(PrefixesApi::new(
            shared_rib.clone(),
            Arc::new("dummy".to_string()),
            query_limits.clone(),
            rib_type,
            None,
            pending_vrib_query_results.clone(),
            Arc::default(), // ingress::Register
        ));
        let tracer = Arc::new(Tracer::new());

        let runner = Self {
            gate,
            http_processor,
            query_limits,
            rib: shared_rib,
            rib_type,
            status_reporter,
            filter_name,
            pending_vrib_query_results,
            _process_metrics,
            rib_merge_update_stats,
            tracer,
            roto_function_pre: None,
            roto_function_post: None,
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

    #[cfg(test)]
    pub(super) fn status_reporter(&self) -> Arc<RibUnitStatusReporter> {
        self.status_reporter.clone()
    }

    #[cfg(test)]
    pub(super) fn gate(&self) -> Arc<Gate> {
        self.gate.clone()
    }

    #[cfg(test)]
    pub(super) fn rib(&self) -> Arc<Rib> {
        self.rib.load().clone()
    }

    fn signal_withdraw(
        &self,
        ingress_id: ingress::IngressId,
        specific_afisafi: Option<AfiSafiType>,
    ) {
        self.rib
            .load()
            .withdraw_for_ingress(ingress_id, specific_afisafi);
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

        // Wait for other components to be ready, and signal to other
        // components that we are, ready to start. All units and targets start
        // together, otherwise data passed from one component to another may
        // be lost if the receiving component is not yet ready to accept it.
        arc_self.gate.process_until(waitpoint.ready()).await?;

        // Signal again once we are out of the process_until() so that anyone
        // waiting to send important gate status updates won't send them while
        // we are in process_until() which will just eat them without handling
        // them.
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
                                    //rib_keys: new_rib_keys,
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
                                // XXX LH as long as the HTTP API (and
                                // TriggerData) is limited to 'simple
                                // prefixes', we default to unicast for now.
                                // Eventually, this should facilitate all
                                // afisafis.
                                arc_self
                                    .rib
                                    .load()
                                    .match_prefix(&prefix, &match_options)
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

    pub(super) async fn process_update(
        &self,
        update: Update,
    ) -> Result<(), String> {
        match update {
            Update::UpstreamStatusChange(UpstreamStatus::EndOfStream {
                ..
            }) => {
                // We expect withdrawals to come in via Update::Withdraw
                // messages. Nothing else to do, pass it on.
                self.gate.update_data(update).await;
            }

            Update::Bulk(payloads) => {
                self.filter_payload(payloads /* insert_fn*/).await?
            }

            Update::Single(payload) => {
                self.filter_payload([payload] /* insert_fn*/).await?
            }

            Update::WithdrawBulk(ingress_ids) => {
                ingress_ids
                    .iter()
                    .for_each(|&id| self.signal_withdraw(id, None));
            }

            Update::Withdraw(ingress_id, maybe_afisafi) => {
                self.signal_withdraw(ingress_id, maybe_afisafi)
            }

            Update::OutputStream(..) => {
                // Nothing to do, pass it on
                self.gate.update_data(update).await;
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
            Update::Rtr(rtr_update) => {
                use crate::units::RtrUpdate;
                use rpki::rtr::Payload as RtrPayload;
                match rtr_update {
                    RtrUpdate::Full(rtr_verbs) => {
                        let mut new_cache = RtrCache::default();
                        for (action, payload) in rtr_verbs {
                            if action == rpki::rtr::Action::Withdraw {
                                warn!("Unexpected RTR Withdraw in Cache Reset");
                                continue;
                            }
                            match payload {
                                RtrPayload::Origin(route_origin) => new_cache.route_origins.insert(route_origin),
                                RtrPayload::RouterKey(router_key) => new_cache.router_keys.insert(router_key),
                                RtrPayload::Aspa(aspa) => new_cache.aspas.insert(aspa),
                            };
                        }
                        debug!(
                            "new RTR cache, vrp/routerkey/aspa {}/{}/{}",
                            new_cache.route_origins.len(),
                            new_cache.router_keys.len(),
                            new_cache.aspas.len(),
                        );
                        match self.rtr_cache.try_write() {
                            Ok(mut lock) => {
                                *lock = new_cache;
                            }
                            Err(_) => warn!("failed to update RTR-cache in RIB unit (Reset)"),
                        }
                    }
                    RtrUpdate::Delta(rtr_verbs) => {
                        let mut lock = match self.rtr_cache.try_write() {
                            Ok(lock) => lock,
                            Err(_) => return Err("failed to update RTR-cache in RIB unit (Serial update)".into()),
                        };

                        for (action, payload) in rtr_verbs.into_iter() {
                            match action {
                                rpki::rtr::Action::Announce => {
                                    match payload {
                                        RtrPayload::Origin(route_origin) => { lock.route_origins.replace(route_origin); }
                                        RtrPayload::RouterKey(router_key) => { lock.router_keys.replace(router_key); }
                                        RtrPayload::Aspa(aspa) => { lock.aspas.replace(aspa); }
                                    }
                                }
                                rpki::rtr::Action::Withdraw => {
                                    match payload {
                                        RtrPayload::Origin(route_origin) => { lock.route_origins.remove(&route_origin); }
                                        RtrPayload::RouterKey(router_key) => { lock.router_keys.remove(&router_key); }
                                        RtrPayload::Aspa(aspa) => { lock.aspas.remove(&aspa); }
                                    }
                                }
                            }
                        }
                        debug!(
                            "RTR cache after Serial update, vrp/routerkey/aspa {}/{}/{}",
                            lock.route_origins.len(),
                            lock.router_keys.len(),
                            lock.aspas.len(),
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn filter_payload(
        &self,
        payload: impl IntoIterator<Item = Payload>,
    ) -> Result<(), String> {
        let mut res = SmallVec::<[Payload; 8]>::new();

        let mut output_stream = RotoOutputStream::new();
        let mut ctx = Ctx::new(&mut output_stream);

        for p in payload {
            let ingress_id = match &p.context {
                RouteContext::Fresh(f) => Some(f.provenance().ingress_id),
                RouteContext::Mrt(m) => Some(m.provenance().ingress_id),
                _ => None,
            };
            if let Some(ref roto_function) = self.roto_function_pre {
                match roto_function.call(
                    &mut ctx,
                    roto::Val(p.rx_value.clone()),
                    //roto::Val(p.context.clone()),
                ) {
                    roto::Verdict::Accept(_) => {
                        self.insert_payload(&p);
                        res.push(p.clone());
                    }
                    roto::Verdict::Reject(_) => {
                        debug!("roto::Verdict Reject, dropping {p:#?}");
                    }
                }
            } else {
                // default action accept
                self.insert_payload(&p);
                res.push(p.clone());
            }
            if !output_stream.is_empty() {
                let mut osms = smallvec![];

                for entry in output_stream.drain() {
                    debug!("output stream entry {entry:?}");
                    let osm = match entry {
                        Output::Prefix(_prefix) => {
                            OutputStreamMessage::prefix(
                                Some(p.rx_value.clone()),
                                ingress_id,
                            )
                        }
                        Output::Community(_u32) => {
                            OutputStreamMessage::community(
                                Some(p.rx_value.clone()),
                                ingress_id,
                            )
                        }
                        Output::Asn(_u32) => OutputStreamMessage::asn(
                            Some(p.rx_value.clone()),
                            ingress_id,
                        ),
                        Output::Origin(_u32) => OutputStreamMessage::origin(
                            Some(p.rx_value.clone()),
                            ingress_id,
                        ),
                        Output::PeerDown => {
                            debug!("Logged PeerDown from Rib unit, ignoring");
                            continue;
                        }
                        Output::Custom((id, local)) => {
                            OutputStreamMessage::custom(id, local, ingress_id)
                        }
                        Output::Entry(entry) => {
                            OutputStreamMessage::entry(
                                entry,
                                ingress_id,
                            )
                        }

                    };
                    osms.push(osm);
                }
                self.gate.update_data(Update::OutputStream(osms)).await;
            }
        }

        match res.len() {
            0 => {}
            1 => {
                self.gate
                    .update_data(Update::Single(
                        res.into_iter().next().unwrap(),
                    ))
                    .await;
            }
            _ => {
                self.gate.update_data(Update::Bulk(res)).await;
            }
        }

        Ok(())
    }

    pub fn insert_payload(&self, payload: &Payload) {
        let rib = self.rib.load();
        if !rib.is_physical() {
            return;
        }

        let pre_insert = std::time::Instant::now();

        let (route_status, provenance) = match &payload.context {
            RouteContext::Fresh(ctx) => (ctx.status, ctx.provenance),
            RouteContext::Mrt(ctx) => (ctx.status, ctx.provenance),
            RouteContext::Reprocess => {
                error!(
                    "unexpected RouteContext::Reprocess in insert_payload"
                );
                self.status_reporter.insert_failed(
                    &payload.rx_value,
                    "unexpected RouteContext::Reprocess",
                );
                return;
            }
        };

        let ltime = 0_u64; // XXX should come from Payload

        match rib.insert(&payload.rx_value, route_status, provenance, ltime) {
            Ok(report) => {
                let post_insert = std::time::Instant::now();
                let store_op_delay = pre_insert.duration_since(post_insert);
                let propagation_delay = payload.received.duration_since(post_insert);

                let change = if report.prefix_new {
                    StoreInsertionEffect::RouteAdded
                } else {
                    StoreInsertionEffect::RouteUpdated
                };

                self.status_reporter.insert_ok(
                    provenance.ingress_id,
                    store_op_delay,
                    propagation_delay,
                    report.cas_count.try_into().unwrap_or(u32::MAX),
                    change,
                );
                if route_status == RouteStatus::Withdrawn {
                    self.status_reporter.insert_ok(
                    provenance.ingress_id,
                    store_op_delay,
                    propagation_delay,
                    //num_retries,
                    report.cas_count.try_into().unwrap_or(u32::MAX),
                    StoreInsertionEffect::RoutesWithdrawn(1)
                    );
                }

                // XXX re-introduce sometime later
                //if let Some(ref roto_function) = self.roto_function_post {
                //    let mut insertion_info = report.into();
                //    let mut output_stream = RotoOutputStream::new();
                //    let _ = roto_function.call(
                //        roto::Val(&mut output_stream),
                //        roto::Val(payload.rx_value.clone()),
                //        roto::Val(insertion_info),
                //    );
                //    // TODO process outputstream
                //}
            }
            Err(err) => {
                self.status_reporter.insert_failed(&payload.rx_value, err);
            }
        }
    }

    async fn reprocess_query_results(
        &self,
        //res: QueryResult<RotondaRoute>,
        res: QueryResult<RotondaPaMap>,
    ) -> QueryResult<RotondaPaMap> {
        let mut processed_res = QueryResult::<RotondaPaMap> {
            match_type: res.match_type,
            prefix: res.prefix,
            prefix_meta: vec![],
            less_specifics: None,
            more_specifics: None,
        };

        let is_in_prefix_meta_set = !res.prefix_meta.is_empty();

        for record in res.prefix_meta {
            let (mui, ltime, status) =
                (record.multi_uniq_id, record.ltime, record.status);

            if let Some(meta) = self.reprocess_rib_value(record.meta).await {
                processed_res.prefix_meta.push(
                    rotonda_store::PublicRecord::<RotondaPaMap>::new(
                        mui, ltime, status, meta,
                    ),
                );
            }
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
            let is_out_prefix_meta_set =
                !processed_res.prefix_meta.is_empty();
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
        //rib_value: RotondaRoute,
        rib_value: RotondaPaMap,
    ) -> Option<RotondaPaMap> {
        /*
        let mut new_values = HashedSet::with_capacity_and_hasher(
            1,
            HashBuildHasher::default(),
        );
        */

        let tracer = BoundTracer::new(self.tracer.clone(), self.gate.id());

        // XXX where is our ingress_id ?
        //let prov = self.ingresses.get(

        // XXX What is the RouteContext for a re-processed RibValue?
        // We can add a bool to RouteContext signalling we are
        // re-processing a value and the bgp_pdu is (likely) not available
        // in this context. Then, Roto filters can act upon that bool.
        // This might cause discrepancies between 'normal' processing and
        // re-processing, though.

        //let ctx = RouteContext::for_reprocessing(
        //    NlriStatus::UpToDate, // XXX is this
        //    provenance,
        //);
        let _ctx = RouteContext::for_reprocessing();

        trace!("Re-processing route");
        todo!(); // figure out how to construct a Payload when we do not have
                 // the RotondaRoute anymore, only the RotondaPamap.
                 // This will depend on what type the roto function expects.
                 // If it needs the RotondaRoute, we need to change the
                 // signature of fn reprocess_rib_value and call it
                 // differently from reprocess_record_set and
                 // reprocess_query_results
                 /*
                 let payload = Payload::new(
                     rib_value,
                     ctx.clone(),
                     None
                 );
                 */

        todo!() // filter using new roto

        // LH:
        // Let's see if I get this straight. It seems that payload.filter(..)
        // comes from trait Filterable which always returns a
        // SmallVec<Payload> even if the input is a single Payload.
        // However, we work on a single RibValue here, so whatever comes out
        // of the filter is a SmallVec of either 0 or 1 items.
        // or perhaps not: can a single Payload going in result in both the
        // input Payload (PrefixRoute) _AND_ a OutputStreamMessage (is that
        // something going out South?)

        //let res: Option<RibValue>;

        /*
            if let Ok(filtered_payloads) = Self::VM.with(|vm| {
                payload.filter(
                    |value, received, trace_id, context| {
                        self.roto_scripts.exec_with_tracer(
                            vm,
                            &self.filter_name.load(),
                            value,
                            received,
                            tracer.clone(),
                            trace_id,
                            context
                        )
                    },
                    |_source_id| {
                        /* TODO:
                         * self.status_reporter.message_filtered(source_id) */
                    }
                )
            }) {
                // LH: so, filtered_payloads is a mixed SmallVec where a Payload
                // can be either a Output Stream Message ('south') and/or a
                // Payload to be passed on east-wards.
                filtered_payloads
                    .into_iter()
                    //.filter(|payload| {
                    .find(|payload| {
                        !matches!(
                            payload.rx_value,
                            TypeValue::OutputStreamMessage(_)
                        )
                    })
                .map(|payload| {
                    // Add this processed query result route into the new query result
                    //let hash = self
                    //    .rib
                    //    .load()
                    //    .precompute_hash_code(&payload.rx_value);
                    /*
                       PreHashedTypeValue::new(
                       payload.rx_value,
                       route.provenance(),
                    /*hash*/)
                    .into()
                        */
                        payload.rx_value.try_into().unwrap()
                })
            } else {
                None
            }//;

            //res

            //if new_values.is_empty() {
            //    None
            //} else {
            //    Some(new_values.into())
            //}
        */
    }

    async fn reprocess_record_set(
        &self,
        //record_set: &RecordSet<RotondaRoute>,
        record_set: &RecordSet<RotondaPaMap>,
        //) -> Option<RecordSet<RotondaRoute>> {
    ) -> Option<RecordSet<RotondaPaMap>> {
        //let mut new_record_set = RecordSet::<RotondaRoute>::new();
        let mut new_record_set = RecordSet::<RotondaPaMap>::new();

        for record in record_set.iter() {
            // XXX can we safely do this?
            for mut pub_rec in record.meta {
                if let Some(rib_value) =
                    self.reprocess_rib_value(pub_rec.meta).await
                {
                    pub_rec.meta = rib_value;
                    new_record_set.push(record.prefix, vec![pub_rec]);
                }
            }
        }

        if !new_record_set.is_empty() {
            Some(new_record_set)
        } else {
            None
        }
    }
}

// --- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore = "prefix-store now handles all keying"]
    #[test]
    fn default_rib_keys_are_as_expected() {
        /*
        let toml = r#"
        sources = ["some source"]
        "#;

        let config = mk_config_from_toml(toml).unwrap();

        assert_eq!(
            config.rib_keys.as_slice(),
            &[BasicRouteToken::PeerIp, BasicRouteToken::PeerAsn, BasicRouteToken::AsPath]
        );
        */
    }

    #[ignore = "prefix-store now handles all keying"]
    #[test]
    fn specified_rib_keys_are_received() {
        /*
        let toml = r#"
        sources = ["some source"]
        rib_keys = ["PeerIp", "NextHop"]
        "#;

        let config = mk_config_from_toml(toml).unwrap();

        assert_eq!(
            config.rib_keys.as_slice(),
            &[BasicRouteToken::PeerIp, BasicRouteToken::NextHop]
        );
        */
    }

    #[allow(dead_code)]
    fn mk_config_from_toml(toml: &str) -> Result<RibUnit, toml::de::Error> {
        toml::from_str::<RibUnit>(toml)
    }
}
