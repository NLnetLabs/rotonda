use std::sync::RwLock;

use crate::metrics::{self, Metric, MetricType, MetricUnit};

#[derive(Debug, Default)]
pub struct CumAvg {
    ca: f64,
    n: u64,
}

impl CumAvg {
    pub fn value(&self) -> f64 {
        self.ca
    }

    pub fn count(&self) -> u64 {
        self.n
    }
}

impl std::fmt::Display for CumAvg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} µs", self.value() as u64)
    }
}

// The cumulative average per bucket where the bucket value is average time
// taken and the bucket range denotes the size of the HashSet that was being
// MergeUpdate'd into at the time.
#[derive(Debug, Default)]
#[allow(non_snake_case)]
pub struct TimingBuckets {
    le1: CumAvg,
    le10: CumAvg,
    le100: CumAvg,
    le1000: CumAvg,
    le10000: CumAvg,
    leInf: CumAvg,
}

impl std::fmt::Display for TimingBuckets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "le1: {}, le10: {}, le100: {}, le1000: {}, le10000: {}, leInf: {}",
            self.le1, self.le10, self.le100, self.le1000, self.le10000, self.leInf
        )
    }
}

#[derive(Debug, Default)]
pub struct RibMergeUpdateStatistics {
    withdraw: RwLock<TimingBuckets>,
    other: RwLock<TimingBuckets>,
}


impl std::fmt::Display for RibMergeUpdateStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "withdraw: [{}], other: [{}]",
            self.withdraw.read().unwrap(),
            self.other.read().unwrap()
        )
    }
}

impl RibMergeUpdateStatistics {
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const RIB_MERGE_UPDATE_WITHDRAW_DURATION_MICROSECONDS: Metric = Metric::new(
        "rib_merge_update_withdrawal_duration",
        "a histogram of seconds per prefix hashset size for RIB merge update operations to insert withdrawals",
        MetricType::Histogram,
        MetricUnit::Microsecond,
    );
    // TEST STATUS: [ ] makes sense? [ ] passes tests?
    const RIB_MERGE_UPDATE_ANNOUNCE_DURATION_MICROSECONDS: Metric = Metric::new(
        "rib_merge_update_announce_duration",
        "a histogram of seconds per prefix hashset size for RIB merge update operations to insert announcements",
        MetricType::Histogram,
        MetricUnit::Microsecond,
    );
}

impl metrics::Source for RibMergeUpdateStatistics {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        let withdraw = self.withdraw.read().unwrap();
        let other = self.other.read().unwrap();

        #[rustfmt::skip]
        let metric_data = [
            (&Self::RIB_MERGE_UPDATE_WITHDRAW_DURATION_MICROSECONDS, withdraw),
            (&Self::RIB_MERGE_UPDATE_ANNOUNCE_DURATION_MICROSECONDS, other),
        ];

        for (metric, data) in metric_data {
            target.append(metric, Some(unit_name), |records| {
                let mut cum_v = data.le1.value();
                let mut cum_n = data.le1.count();
                records.suffixed_label_value(
                    &[("le", "1")],
                    cum_v,
                    Some("bucket"),
                );

                cum_v += data.le10.value();
                cum_n += data.le10.count();
                records.suffixed_label_value(
                    &[("le", "10")],
                    cum_v,
                    Some("bucket"),
                );

                cum_v += data.le100.value();
                cum_n += data.le100.count();
                records.suffixed_label_value(
                    &[("le", "100")],
                    cum_v,
                    Some("bucket"),
                );

                cum_v += data.le1000.value();
                cum_n += data.le1000.count();
                records.suffixed_label_value(
                    &[("le", "1000")],
                    cum_v,
                    Some("bucket"),
                );

                cum_v += data.le10000.value();
                cum_n += data.le10000.count();
                records.suffixed_label_value(
                    &[("le", "10000")],
                    cum_v,
                    Some("bucket"),
                );

                cum_v += data.leInf.value();
                cum_n += data.leInf.count();
                records.suffixed_label_value(
                    &[("le", "+Inf")],
                    cum_v,
                    Some("bucket"),
                );

                records.suffixed_value(cum_n, Some("sum"));
                records.suffixed_value(cum_v, Some("count"));
            });
        }
    }
}
