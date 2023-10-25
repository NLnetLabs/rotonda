use futures::Future;
use tokio::task::JoinHandle;
use tokio_metrics::Instrumented;

use crate::metrics::{self, Metric, MetricType, MetricUnit};

pub(crate) fn spawn<T: Send + 'static>(
    _name: &str,
    future: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    tokio::task::spawn(future)
}

#[derive(Debug, Default)]
pub struct TokioTaskMetrics {
    task_monitor: tokio_metrics::TaskMonitor,
}

impl TokioTaskMetrics {
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const INSTRUMENTED_COUNT: Metric = Metric::new(
        "task_instrumented_count",
        "The number of tasks instrumented.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const DROPPED_COUNT: Metric = Metric::new(
        "task_dropped_count",
        "The number of tasks dropped.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const FIRST_POLL_COUNT: Metric = Metric::new(
        "task_first_poll_count",
        "The number of tasks polled for the first time.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_FIRST_POLL_DELAY: Metric = Metric::new(
        "task_total_first_poll_delay",
        "The total duration elapsed between the instant tasks are instrumented, and the instant they are first polled.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_IDLED_COUNT: Metric = Metric::new(
        "task_total_idled_count",
        "The total number of times that tasks idled, waiting to be awoken.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_IDLE_DURATION: Metric = Metric::new(
        "task_total_idle_duration",
        "The total duration that tasks idled.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_SCHEDULED_COUNT: Metric = Metric::new(
        "task_total_scheduled_count",
        "The total number of times that tasks were awoken (and then, presumably, scheduled for execution).",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_SCHEDULED_DURATION: Metric = Metric::new(
        "task_total_scheduled_duration",
        "The total duration that tasks spent waiting to be polled after awakening.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_POLL_COUNT: Metric = Metric::new(
        "task_total_poll_count",
        "The total number of times that tasks were polled.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_POLL_DURATION: Metric = Metric::new(
        "task_total_poll_duration",
        "The total duration elapsed during polls.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_FAST_POLL_COUNT: Metric = Metric::new(
        "task_total_fast_poll_count",
        "The total number of times that polling tasks completed swiftly.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_FAST_POLL_DURATION: Metric = Metric::new(
        "task_total_fast_poll_duration",
        "The total duration of fast polls.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_SLOW_POLL_COUNT: Metric = Metric::new(
        "task_total_slow_poll_count",
        "The total number of times that polling tasks completed slowly.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_SLOW_POLL_DURATION: Metric = Metric::new(
        "task_total_slow_poll_duration",
        "The total duration of slow polls.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_SHORT_DELAY_COUNT: Metric = Metric::new(
        "task_total_short_delay_count",
        "The total count of tasks with short scheduling delays.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_LONG_DELAY_COUNT: Metric = Metric::new(
        "task_total_long_delay_count",
        "The total count of tasks with long scheduling delays.",
        MetricType::Counter,
        MetricUnit::Total,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_SHORT_DELAY_DURATION: Metric = Metric::new(
        "task_total_short_delay_duration",
        "The total duration of tasks with short scheduling delays.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const TOTAL_LONG_DELAY_DURATION: Metric = Metric::new(
        "task_total_long_delay_duration",
        "The total number of times that a task had a long scheduling duration.",
        MetricType::Gauge,
        MetricUnit::Millisecond,
    );
}

impl TokioTaskMetrics {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn instrument<F>(&self, task: F) -> Instrumented<F> {
        self.task_monitor.instrument(task)
    }
}

impl metrics::Source for TokioTaskMetrics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        let metrics = self.task_monitor.intervals().next().unwrap();

        target.append_simple(
            &Self::INSTRUMENTED_COUNT,
            Some(unit_name),
            metrics.instrumented_count,
        );
        target.append_simple(
            &Self::DROPPED_COUNT,
            Some(unit_name),
            metrics.dropped_count,
        );
        target.append_simple(
            &Self::FIRST_POLL_COUNT,
            Some(unit_name),
            metrics.first_poll_count,
        );
        target.append_simple(
            &Self::TOTAL_FIRST_POLL_DELAY,
            Some(unit_name),
            metrics.total_first_poll_delay.as_millis(),
        );
        target.append_simple(
            &Self::TOTAL_IDLED_COUNT,
            Some(unit_name),
            metrics.total_idled_count,
        );
        target.append_simple(
            &Self::TOTAL_IDLE_DURATION,
            Some(unit_name),
            metrics.total_idle_duration.as_millis(),
        );
        target.append_simple(
            &Self::TOTAL_SCHEDULED_COUNT,
            Some(unit_name),
            metrics.total_scheduled_count,
        );
        target.append_simple(
            &Self::TOTAL_SCHEDULED_DURATION,
            Some(unit_name),
            metrics.total_scheduled_count,
        );
        target.append_simple(
            &Self::TOTAL_POLL_COUNT,
            Some(unit_name),
            metrics.total_poll_count,
        );
        target.append_simple(
            &Self::TOTAL_POLL_DURATION,
            Some(unit_name),
            metrics.total_poll_duration.as_millis(),
        );
        target.append_simple(
            &Self::TOTAL_FAST_POLL_COUNT,
            Some(unit_name),
            metrics.total_fast_poll_count,
        );
        target.append_simple(
            &Self::TOTAL_FAST_POLL_DURATION,
            Some(unit_name),
            metrics.total_fast_poll_duration.as_millis(),
        );
        target.append_simple(
            &Self::TOTAL_SLOW_POLL_COUNT,
            Some(unit_name),
            metrics.total_slow_poll_count,
        );
        target.append_simple(
            &Self::TOTAL_SLOW_POLL_DURATION,
            Some(unit_name),
            metrics.total_idled_count,
        );
        target.append_simple(
            &Self::TOTAL_SHORT_DELAY_COUNT,
            Some(unit_name),
            metrics.total_short_delay_count,
        );
        target.append_simple(
            &Self::TOTAL_LONG_DELAY_COUNT,
            Some(unit_name),
            metrics.total_long_delay_count,
        );
        target.append_simple(
            &Self::TOTAL_SHORT_DELAY_DURATION,
            Some(unit_name),
            metrics.total_short_delay_duration.as_millis(),
        );
        target.append_simple(
            &Self::TOTAL_LONG_DELAY_DURATION,
            Some(unit_name),
            metrics.total_long_delay_duration.as_millis(),
        );
    }
}
