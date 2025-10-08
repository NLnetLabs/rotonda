use std::{collections::HashMap, sync::{atomic::{AtomicU64, Ordering}, Arc}};

use log::debug;


#[derive(Eq, Hash, PartialEq)]
pub struct MetricKey {
    name: String,
    tags: Vec<(Arc<str>, Arc<str>)>,
}


#[derive(Default)]
pub struct Metrics {
    counters: HashMap<Arc<str>, AtomicU64>,
    gauges: HashMap<Arc<str>, AtomicU64>,
}

impl Metrics {

    pub fn inc_counter(&mut self, name: Arc<str>, value: u64) {
        self.counters.entry(name).and_modify(|counter| {
            counter.fetch_add(value, Ordering::Relaxed); 
        }).or_insert(value.into());
    }

    pub fn try_inc_counter(&self, name: Arc<str>, value: u64) -> Result<(), &str> {
        if let Some(counter) = self.counters.get(&*name) {
            counter.fetch_add(value, Ordering::Relaxed);
            //debug!("inc_counter for {}, +{value}, now at {}",
            //    name,
            //    counter.load(Ordering::Relaxed)
            //);
            Ok(())
        } else {
            debug!("no counter {name} yet (value: {value})");
            Err("no key for this name in metrics")
        }
    }

    pub fn set_gauge(&mut self, name: Arc<str>, value: u64) {
        self.gauges.entry(name).and_modify(|gauge| {
            gauge.store(value, Ordering::Relaxed); 
        }).or_insert(value.into());
    }

    pub fn try_set_gauge(&self, name: Arc<str>, value: u64) -> Result<(), &str> {
        if let Some(gauge) = self.gauges.get(&*name) {
            gauge.store(value, Ordering::Relaxed);
            Ok(())
        } else {
            debug!("could not gauge {name}, returning Err");
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
    pub metrics: super::MutMetrics,
}

impl crate::metrics::Source for RotoMetricsWrapper {
    fn append(&self, _unit_name: &str, target: &mut crate::metrics::Target) {
        let mut counters;
        let mut gauges;
        {
            let metrics = self.metrics.read().unwrap();
            counters = metrics.counters.iter().map(|(k,v)| (k.to_string(), v.load(Ordering::Relaxed))).collect::<Vec<_>>();
            gauges = metrics.gauges.iter().map(|(k,v)| (k.to_string(), v.load(Ordering::Relaxed))).collect::<Vec<_>>();
        }
        counters.sort_by(|a, b| a.0.cmp(&b.0));
        gauges.sort_by(|a, b| a.0.cmp(&b.0));

        let mut printed_counter_names: Vec<String> = vec![];
        let mut printed_gauge_names: Vec<String> = vec![];

        for (name, cnt) in counters {
            let base_name: String = name.split('{').next().unwrap().into();
            if !printed_counter_names.contains(&base_name) {
                target.append_raw(format!("# TYPE roto_user_defined_{base_name} counter"));
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

