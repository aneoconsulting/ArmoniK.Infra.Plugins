use crate::metrics::MetricsValue;

pub mod age;

/// Trait to specify how the metrics should be converted into a cost
pub trait CostModel {
    /// Convert the metrics into a cost
    fn metrics_to_cost(&self, metrics: &MetricsValue) -> i32;
}
