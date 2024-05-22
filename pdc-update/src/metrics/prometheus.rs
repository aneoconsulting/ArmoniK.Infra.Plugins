use serde::{Deserialize, Serialize};

use super::{MetricsScraper, MetricsValue};

/// [`MetricsScraper`] that fetch metrics from Prometheus
#[derive(Debug, Clone)]
pub struct PrometheusScraper {
    /// Query URL for Prometheus
    query: reqwest::Url,
}

impl PrometheusScraper {
    /// Create a new [`PrometheusScraper`]
    pub fn new(
        url: &str,
        metrics_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut query = reqwest::Url::parse(url)?.join("api/v1/query")?;
        query.query_pairs_mut().append_pair("query", metrics_name);
        Ok(Self { query })
    }
}

#[async_trait::async_trait]
impl MetricsScraper for PrometheusScraper {
    async fn scrap_metrics(
        &self,
    ) -> Result<Vec<MetricsValue>, Box<dyn std::error::Error + Send + Sync>> {
        log::debug!("Scrap Prometheus metrics: {}", self.query);

        // Fetch the metrics from Prometheus
        let resp = reqwest::get(self.query.clone()).await?.text().await?;

        // Parse the json response
        let resp: PrometheusResponse<'_> = serde_json::from_str(&resp)?;

        // Convert raw Prometheus results into `MetricsValue`
        let metrics = resp
            .data
            .result
            .into_iter()
            .filter_map(|result| {
                // Parse the result value
                let value = match result.value.1.parse::<f64>() {
                    Ok(value) => value,
                    Err(err) => {
                        // Ignore metrics that could not be parsed
                        log::error!(
                            "Could not parse metrics for {}: {}: {}",
                            result.metric.kubernetes_pod_name,
                            err,
                            result.value.1,
                        );
                        return None;
                    }
                };

                Some(MetricsValue {
                    name: result.metric.kubernetes_pod_name.to_owned(),
                    namespace: result.metric.kubernetes_namespace.to_owned(),
                    timestamp: result.value.0,
                    value,
                })
            })
            .collect();

        log::trace!("Fetched metrics: {metrics:#?}");

        Ok(metrics)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PrometheusResponse<'a> {
    #[serde(borrow = "'a")]
    status: &'a str,
    data: PrometheusData<'a>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PrometheusData<'a> {
    #[serde(borrow = "'a")]
    #[serde(rename = "resultType")]
    result_type: &'a str,
    result: Vec<PrometheusResult<'a>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PrometheusResult<'a> {
    #[serde(borrow = "'a")]
    metric: PrometheusMetric<'a>,
    value: PrometheusValue<'a>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
struct PrometheusMetric<'a> {
    #[serde(borrow = "'a")]
    #[serde(rename = "__name__")]
    name: &'a str,
    instance: &'a str,
    job: &'a str,
    kubernetes_namespace: &'a str,
    kubernetes_pod_name: &'a str,
    kubernetes_pod_node_name: &'a str,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PrometheusValue<'a>(pub f64, pub &'a str);
