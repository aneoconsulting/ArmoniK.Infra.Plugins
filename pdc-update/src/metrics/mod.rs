pub mod prometheus;

/// Encode a metrics value for a specific worker
#[derive(Debug, Default, Clone, PartialEq, PartialOrd)]
pub struct MetricsValue {
    /// Name of the worker
    pub name: String,
    /// Namespace of the worker
    pub namespace: String,
    /// Instant where the metrics has been produced
    pub timestamp: f64,
    /// Value of the metrics
    pub value: f64,
}

/// Trait to scrap metrics for multiple workers
#[async_trait::async_trait]
pub trait MetricsScraper {
    /// Scrap metrics for the multiple workers
    async fn scrap_metrics(&self) -> Result<Vec<MetricsValue>, eyre::Report>;
}
