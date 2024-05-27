use crate::metrics::MetricsValue;

use super::CostModel;

/// [`CostModel`] using the age of the processing
#[derive(Debug, Default, Clone, PartialEq, PartialOrd)]
pub struct AgeCost {
    /// Threshold below which the actual age is ignored
    age_threshold: f64,
    /// Scaling factor for the cost before conversion to i32
    scaling: f64,
}

impl AgeCost {
    /// Build a new [`AgeCost`]
    pub fn new(age_threshold: f64, granularity: f64) -> Self {
        Self {
            age_threshold,
            scaling: 1.0 / granularity,
        }
    }
}

impl CostModel for AgeCost {
    fn metrics_to_cost(&self, metrics: &MetricsValue) -> i32 {
        let duration = metrics.timestamp - metrics.value;
        let cost = if duration <= 0.0 {
            -1
        } else {
            let cost = if duration <= self.age_threshold {
                self.age_threshold * self.scaling
            } else {
                f64::from(i32::MAX) - metrics.value * self.scaling
            };

            cost.round().min(f64::from(i32::MAX)).max(1.0) as i32
        };

        log::trace!(
            "Compute cost for {}/{} = {} @{} -> {}",
            metrics.namespace,
            metrics.name,
            metrics.value,
            metrics.timestamp,
            cost
        );

        cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics(timestamp: f64, value: f64) -> MetricsValue {
        MetricsValue {
            timestamp,
            value,
            ..Default::default()
        }
    }

    #[test]
    fn correctness() {
        let max = f64::from(i32::MAX);
        let m = AgeCost::new(10.0, 2.0);

        assert_eq!(m.metrics_to_cost(&metrics(4. * max, 3. * max)), 1);
        assert_eq!(m.metrics_to_cost(&metrics(0., 1000.)), -1);
        assert_eq!(m.metrics_to_cost(&metrics(1000., 1000.)), -1);
        assert_eq!(m.metrics_to_cost(&metrics(1001., 1000.)), 5);
        assert_eq!(m.metrics_to_cost(&metrics(1009., 1000.)), 5);
        assert_eq!(m.metrics_to_cost(&metrics(1010., 1000.)), 5);
        assert_eq!(m.metrics_to_cost(&metrics(1011., 1000.)), i32::MAX - 500);
        assert_eq!(m.metrics_to_cost(&metrics(2000., 1000.)), i32::MAX - 500);
    }
}
