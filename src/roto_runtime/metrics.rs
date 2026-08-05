use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use log::debug;

//pub type MutMetrics = Arc<RwLock<Metrics>>;
#[derive(Clone, Default)]
pub struct MutMetrics(Arc<RwLock<Metrics>>);

#[derive(Eq, Hash, PartialEq)]
pub struct MetricKey {
    name: String,
    tags: Vec<(String, String)>,
}

#[derive(Default)]
pub struct Metrics {
    counters: HashMap<String, AtomicU64>,
    gauges: HashMap<String, AtomicU64>,
}

impl PartialEq for MutMetrics {
    fn eq(&self, _other: &Self) -> bool {
        //XXX double check
        false
    }
}

impl MutMetrics {
    pub fn read(
        &self,
    ) -> std::result::Result<
        std::sync::RwLockReadGuard<'_, Metrics>,
        std::sync::PoisonError<std::sync::RwLockReadGuard<'_, Metrics>>,
    > {
        self.0.read()
    }
    pub fn write(
        &self,
    ) -> std::result::Result<
        std::sync::RwLockWriteGuard<'_, Metrics>,
        std::sync::PoisonError<std::sync::RwLockWriteGuard<'_, Metrics>>,
    > {
        self.0.write()
    }
}

impl Metrics {
    pub fn inc_counter(&mut self, name: impl AsRef<str>, value: u64) {
        self.counters
            .entry(name.as_ref().to_string())
            .and_modify(|counter| {
                counter.fetch_add(value, Ordering::Relaxed);
            })
            .or_insert(value.into());
    }

    pub fn try_inc_counter(
        &self,
        name: impl AsRef<str>,
        value: u64,
    ) -> Result<(), &str> {
        if let Some(counter) = self.counters.get(name.as_ref()) {
            counter.fetch_add(value, Ordering::Relaxed);
            //debug!("inc_counter for {}, +{value}, now at {}",
            //    name,
            //    counter.load(Ordering::Relaxed)
            //);
            Ok(())
        } else {
            debug!("no counter {} yet (value: {value})", name.as_ref());
            Err("no key for this name in metrics")
        }
    }

    pub fn set_gauge(&mut self, name: impl AsRef<str>, value: u64) {
        self.gauges
            .entry(name.as_ref().to_string())
            .and_modify(|gauge| {
                gauge.store(value, Ordering::Relaxed);
            })
            .or_insert(value.into());
    }

    pub fn try_set_gauge(
        &self,
        name: impl AsRef<str>,
        value: u64,
    ) -> Result<(), &str> {
        if let Some(gauge) = self.gauges.get(name.as_ref()) {
            gauge.store(value, Ordering::Relaxed);
            Ok(())
        } else {
            debug!("could not gauge {}, returning Err", name.as_ref());
            Err("no key for this name in metrics")
        }
    }
}

//impl crate::metrics::Source for Metrics {
//    fn append(&self, _unit_name: &str, target: &mut crate::metrics::Target) {
//        for (k,v) in self.counters.iter() {
//            target.append_raw(format!("roto_user_defined_{} {}", k, v.load(Ordering::Relaxed)));
//        }
//    }
//}

#[derive(Default)]
pub struct RotoMetricsWrapper {
    pub metrics: MutMetrics,
}

impl crate::metrics::Source for RotoMetricsWrapper {
    fn append(&self, _unit_name: &str, target: &mut crate::metrics::Target) {
        let mut counters;
        let mut gauges;
        {
            let metrics = self.metrics.read().unwrap();
            counters = metrics
                .counters
                .iter()
                .map(|(k, v)| (k.to_string(), v.load(Ordering::Relaxed)))
                .collect::<Vec<_>>();
            gauges = metrics
                .gauges
                .iter()
                .map(|(k, v)| (k.to_string(), v.load(Ordering::Relaxed)))
                .collect::<Vec<_>>();
        }
        counters.sort_by(|a, b| a.0.cmp(&b.0));
        gauges.sort_by(|a, b| a.0.cmp(&b.0));

        let mut printed_counter_names: Vec<String> = vec![];
        let mut printed_gauge_names: Vec<String> = vec![];

        for (name, cnt) in counters {
            let base_name: String = name.split('{').next().unwrap().into();
            if !printed_counter_names.contains(&base_name) {
                target.append_raw(format!(
                    "# TYPE roto_user_defined_{base_name} counter"
                ));
                printed_counter_names.push(base_name);
            }

            target.append_raw(format!("roto_user_defined_{} {}", name, cnt));
        }
        for (name, val) in gauges {
            let base_name: String = name.split('{').next().unwrap().into();
            if !printed_gauge_names.contains(&base_name) {
                target.append_raw(format!("# TYPE {base_name} gauge"));
                printed_gauge_names.push(base_name);
            }

            target.append_raw(format!("roto_user_defined_{} {}", name, val));
        }
    }
}
