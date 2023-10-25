use crate::metrics;

#[derive(Debug, Default)]
pub struct BmpFsOutMetrics;

impl BmpFsOutMetrics {
    pub fn new() -> Self {
        Self
    }
}

// TODO: should we actually be publishing some metrics here???
impl metrics::Source for BmpFsOutMetrics {
    fn append(&self, _unit_name: &str, _target: &mut metrics::Target) {}
}
