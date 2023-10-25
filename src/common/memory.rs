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
