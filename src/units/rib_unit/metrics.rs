use std::{
    sync::{
        atomic::{AtomicI64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};

use arc_swap::{ArcSwap, ArcSwapAny};

use crate::{
    common::frim::FrimMap,
    comms::{Gate, GateMetrics},
    metrics::{
        self, util::append_per_router_metric, Metric, MetricType, MetricUnit,
    },
    payload::RouterId,
};

use super::statistics::RibMergeUpdateStatistics;

#[derive(Debug, Default)]
pub struct RibUnitMetrics {
    gate: Arc<GateMetrics>,
    pub num_unique_prefixes: AtomicUsize,
    pub num_items: AtomicUsize,
    pub num_insert_retries: AtomicUsize,
    pub num_insert_hard_failures: AtomicUsize,
    pub num_routes_announced: AtomicUsize,
    pub num_modified_route_announcements: AtomicUsize,
    pub num_routes_withdrawn: AtomicUsize,
    pub num_route_withdrawals_without_announcement: AtomicUsize,
    pub last_insert_duration: AtomicI64,
    pub last_update_duration: AtomicI64,
    routers: Arc<FrimMap<Arc<RouterId>, Arc<RouterMetrics>>>,
    pub rib_merge_update_stats: Arc<RibMergeUpdateStatistics>,
}

impl RibUnitMetrics {
    pub fn router_metrics(
        &self,
        router_id: Arc<RouterId>,
    ) -> Arc<RouterMetrics> {
        self.routers
            .entry(router_id)
            .or_insert_with(Default::default)
    }
}

#[derive(Debug)]
pub struct RouterMetrics {
    pub last_e2e_delay: Arc<AtomicI64>,
    pub last_e2e_delay_at: Arc<ArcSwap<Instant>>,
}

impl Default for RouterMetrics {
    fn default() -> Self {
        Self {
            last_e2e_delay: Arc::new(Default::default()),
            last_e2e_delay_at: Arc::new(ArcSwapAny::new(Arc::new(
                Instant::now(),
            ))),
        }
    }
}

impl RibUnitMetrics {
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const NUM_UNIQUE_PREFIXES_METRIC: Metric = Metric::new(
        "rib_unit_num_unique_prefixes",
        "the number of unique prefixes seen by the rib",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const NUM_ITEMS_METRIC: Metric = Metric::new(
        "rib_unit_num_items",
        "the total number of items (e.g. routes) stored (withdrawn or not) in the rib",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const NUM_INSERT_RETRIES_METRIC: Metric = Metric::new(
        "rib_unit_num_insert_retries",
        "the number of times prefix insertions had to be retried due to contention",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_INSERT_HARD_FAILURES_METRIC: Metric = Metric::new(
        "rib_unit_num_insert_hard_failures",
        "the number of prefix insertions that were given up after exhausting all retries",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const NUM_ROUTES_ANNOUNCED_METRIC: Metric = Metric::new(
        "rib_unit_num_routes_announced",
        "the number of announced routes stored in the rib",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_MODIFIED_ROUTE_ANNOUNCEMENTS_METRIC: Metric = Metric::new(
        "rib_unit_num_modified_route_announcements",
        "the number of modified route announcements processed",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const NUM_ROUTES_WITHDRAWN_METRIC: Metric = Metric::new(
        "rib_unit_num_routes_withdrawn",
        "the number of withdrawn routes stored in the rib",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const NUM_WITHDRAWN_ROUTES_WITHOUT_ANNOUNCEMENTS_METRIC: Metric = Metric::new(
        "rib_unit_num_route_withdrawals_without_announcements",
        "the number of route withdrawals processed without a corresponding announcement",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const LAST_INSERT_DURATION_METRIC: Metric = Metric::new(
        "rib_unit_insert_duration",
        "the time taken to insert the last prefix inserted into the RIB unit store",
        MetricType::Gauge,
        MetricUnit::Microsecond,
    );
    // TEST STATUS: [/] makes sense? [/] passes tests?
    const LAST_UPDATE_DURATION_METRIC: Metric = Metric::new(
        "rib_unit_update_duration",
        "the time taken to update the last prefix modified in the RIB unit store",
        MetricType::Gauge,
        MetricUnit::Microsecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const LAST_END_TO_END_DELAY_PER_ROUTER_METRIC: Metric = Metric::new(
        "rib_unit_e2e_duration",
        "the time taken from initial receipt to completed insertion for a prefix into the RIB unit store",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
}

impl RibUnitMetrics {
    pub fn new(
        gate: &Arc<Gate>,
        rib_merge_update_stats: Arc<RibMergeUpdateStatistics>,
    ) -> Self {
        RibUnitMetrics {
            gate: gate.metrics(),
            rib_merge_update_stats,
            ..Default::default()
        }
    }
}

impl metrics::Source for RibUnitMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        self.gate.append(unit_name, target);

        target.append_simple(
            &Self::NUM_UNIQUE_PREFIXES_METRIC,
            Some(unit_name),
            self.num_unique_prefixes.load(SeqCst),
        );
        target.append_simple(
            &Self::NUM_ITEMS_METRIC,
            Some(unit_name),
            self.num_items.load(SeqCst),
        );
        target.append_simple(
            &Self::NUM_INSERT_RETRIES_METRIC,
            Some(unit_name),
            self.num_insert_retries.load(SeqCst),
        );
        target.append_simple(
            &Self::NUM_INSERT_HARD_FAILURES_METRIC,
            Some(unit_name),
            self.num_insert_hard_failures.load(SeqCst),
        );
        target.append_simple(
            &Self::NUM_ROUTES_ANNOUNCED_METRIC,
            Some(unit_name),
            self.num_routes_announced.load(SeqCst),
        );
        target.append_simple(
            &Self::NUM_MODIFIED_ROUTE_ANNOUNCEMENTS_METRIC,
            Some(unit_name),
            self.num_modified_route_announcements.load(SeqCst),
        );
        target.append_simple(
            &Self::NUM_ROUTES_WITHDRAWN_METRIC,
            Some(unit_name),
            self.num_routes_withdrawn.load(SeqCst),
        );
        target.append_simple(
            &Self::NUM_WITHDRAWN_ROUTES_WITHOUT_ANNOUNCEMENTS_METRIC,
            Some(unit_name),
            self.num_route_withdrawals_without_announcement.load(SeqCst),
        );
        target.append_simple(
            &Self::LAST_INSERT_DURATION_METRIC,
            Some(unit_name),
            self.last_insert_duration.load(SeqCst),
        );
        target.append_simple(
            &Self::LAST_UPDATE_DURATION_METRIC,
            Some(unit_name),
            self.last_update_duration.load(SeqCst),
        );

        let max_age = Duration::from_secs(60);
        self.routers.retain(|_, metrics| {
            metrics.last_e2e_delay_at.load().elapsed() <= max_age
        });

        for (router_id, metrics) in self.routers.guard().iter() {
            let router_id = router_id.as_str();
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::LAST_END_TO_END_DELAY_PER_ROUTER_METRIC,
                metrics.last_e2e_delay.load(SeqCst),
            );
        }

        self.rib_merge_update_stats.append(unit_name, target);
    }
}
