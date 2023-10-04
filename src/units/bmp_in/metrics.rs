use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::{
    common::frim::FrimMap,
    comms::{Gate, GateMetrics},
    metrics::{self, util::append_per_router_metric, Metric, MetricType, MetricUnit},
    payload::RouterId,
};

#[derive(Debug, Default)]
pub struct BmpInMetrics {
    gate: Option<Arc<GateMetrics>>, // optional to make testing easier
    routers: Arc<FrimMap<Arc<RouterId>, Arc<RouterMetrics>>>,
}

#[derive(Debug, Default)]
pub struct RouterMetrics {
    pub num_invalid_bmp_messages: AtomicUsize,
}

impl BmpInMetrics {
    // TEST STATUS: [/] makes sense? [ ] passes tests?
    const NUM_INVALID_BMP_MESSAGES_METRIC: Metric = Metric::new(
        "bmp_in_num_invalid_bmp_messages",
        "the number of received BMP messages that were invalid (e.g. not RFC compliant, could not be parsed, etc)",
        MetricType::Counter,
        MetricUnit::Total,
    );
}

impl BmpInMetrics {
    pub fn new(gate: &Gate) -> Self {
        Self {
            gate: Some(gate.metrics()),
            ..Default::default()
        }
    }

    pub fn contains(&self, router_id: &Arc<RouterId>) -> bool {
        self.routers.contains_key(&router_id)
    }

    /// Warning: This fn will create a metric set for the given router id if
    /// it doesn't already exist. Use `contains()` to test if metrics exist
    /// for a given router id.
    pub fn router_metrics(&self, router_id: Arc<RouterId>) -> Arc<RouterMetrics> {
        self.routers
            .entry(router_id)
            .or_insert_with(Default::default)
    }
}

impl metrics::Source for BmpInMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        if let Some(gate) = &self.gate {
            gate.append(unit_name, target);
        }

        for (router_id, metrics) in self.routers.guard().iter() {
            let router_id = router_id.as_str();
            append_per_router_metric(
                unit_name,
                target,
                router_id,
                Self::NUM_INVALID_BMP_MESSAGES_METRIC,
                metrics.num_invalid_bmp_messages.load(Ordering::SeqCst),
            );
        }
    }
}
