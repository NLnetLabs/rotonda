use allocator_api2::alloc::{Allocator, Layout, System};

use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

use crate::metrics::{self, Metric, MetricType, MetricUnit};

// ---

#[derive(Copy, Clone, Debug, Default)]
pub struct Stats {
    pub bytes_allocated: usize,
}

// ---

#[derive(Clone, Debug)]
pub struct TrackingAllocator<T: Allocator> {
    allocator: Arc<T>,
    bytes_allocated: Arc<AtomicUsize>,
}

// ---

impl<T: Allocator> TrackingAllocator<T> {
    #[allow(dead_code)]
    pub fn new(allocator: T) -> Self {
        Self {
            allocator: Arc::new(allocator),
            bytes_allocated: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub fn allocator(&self) -> Arc<T> {
        self.allocator.clone()
    }

    #[allow(dead_code)]
    pub fn reset(&self) {
        self.bytes_allocated.store(0, SeqCst);
    }

    pub fn record_alloc(&self, layout: Layout) {
        self.bytes_allocated.fetch_add(layout.size(), SeqCst);
    }

    pub fn record_dealloc(&self, layout: Layout) {
        self.bytes_allocated.fetch_sub(layout.size(), SeqCst);
    }

    pub fn stats(&self) -> Stats {
        Stats {
            bytes_allocated: self.bytes_allocated.load(SeqCst),
        }
    }
}

// ---

impl Default for TrackingAllocator<System> {
    fn default() -> Self {
        Self {
            allocator: Arc::new(System),
            bytes_allocated: Default::default(),
        }
    }
}

// ---

unsafe impl<T: Allocator> Allocator for TrackingAllocator<T> {
    fn allocate(
        &self,
        layout: Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, allocator_api2::alloc::AllocError>
    {
        let p = self.allocator.allocate(layout);
        self.record_alloc(layout);
        p
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: Layout) {
        self.allocator.deallocate(ptr, layout);
        self.record_dealloc(layout);
    }
}

// ---

impl<T: Allocator> TrackingAllocator<T> {
    const NUM_BYTES_ALLOCATED_METRIC: Metric = Metric::new(
        "tracking_allocator_num_bytes_allocated",
        "the amount of memory currently allocated",
        MetricType::Gauge,
        MetricUnit::Total,
    );
}

impl<T: Allocator + Send + Sync> metrics::Source for TrackingAllocator<T> {
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        let stats = self.stats();

        target.append_simple(
            &Self::NUM_BYTES_ALLOCATED_METRIC,
            Some(unit_name),
            stats.bytes_allocated,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{alloc::System, sync::Arc};

    use hashbrown::DefaultHashBuilder;

    use crate::tests::util::internal::get_testable_metrics_snapshot;

    use super::TrackingAllocator;

    type TestSet<T> =
        hashbrown::HashSet<T, DefaultHashBuilder, TrackingAllocator<System>>;

    #[test]
    fn test_allocation_metrics() {
        let allocator = TrackingAllocator::new(System);

        let mut test_set =
            TestSet::<usize>::with_capacity_in(0, allocator.clone());

        // needed for the calls to `get_num_bytes_allocated_metric_value()`.
        let arc_allocator = Arc::new(allocator);

        let metric_value_at_start =
            get_num_bytes_allocated_metric_value(&arc_allocator);
        assert_eq!(metric_value_at_start, 0,);

        test_set.insert(1);

        let metric_value_after_first_alloc =
            get_num_bytes_allocated_metric_value(&arc_allocator);
        assert!(metric_value_after_first_alloc > 0);

        // Hopefully 1000 is more than the HashSet currently allocated space
        // for so that we trigger another allocation.
        test_set.reserve(1000);

        let metric_value_after_second_alloc =
            get_num_bytes_allocated_metric_value(&arc_allocator);
        assert!(
            metric_value_after_second_alloc > metric_value_after_first_alloc
        );

        // Hopefully the internal capacity management policy of the HashSet
        // will cause this to deallocate some of its pre-allocated space.
        test_set.shrink_to_fit();

        let metric_value_after_shrink =
            get_num_bytes_allocated_metric_value(&arc_allocator);
        assert!(metric_value_after_shrink < metric_value_after_second_alloc);

        drop(test_set);

        let metric_value_after_drop =
            get_num_bytes_allocated_metric_value(&arc_allocator);
        assert_eq!(metric_value_after_drop, 0);
    }

    fn get_num_bytes_allocated_metric_value(
        arc_allocator: &Arc<TrackingAllocator<System>>,
    ) -> usize {
        let metrics = get_testable_metrics_snapshot(arc_allocator);
        metrics.with_name::<usize>("tracking_allocator_num_bytes_allocated")
    }
}
