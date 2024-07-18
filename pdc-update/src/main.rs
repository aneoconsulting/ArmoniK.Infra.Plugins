use std::{collections::HashMap, time::Duration};

use clap::Parser;

mod cost;
mod metrics;
mod updater;

use cost::CostModel;
use metrics::MetricsScraper;
use updater::{WorkerUpdate, WorkerUpdater};

#[derive(Debug, Parser)]
struct Cli {
    /// URL of the Prometheus endpoint
    #[arg(
        short('P'),
        long,
        env = "PrometheusUrl",
        default_value = "http://prometheus.svc:9090/"
    )]
    prometheus_url: String,

    /// Name of the metrics that will be read
    #[arg(
        short,
        long,
        env = "MetricsName",
        default_value = "TaskStartTime_Seconds"
    )]
    metrics_name: String,

    /// Period in seconds at which the pod-deletion-cost is updated
    #[arg(short, long, env = "Period", default_value_t = 10.0)]
    period: f64,

    /// Ignore precise value of the metric if the tasks are younger than the specified value
    #[arg(short, long, env = "IgnoreYoungerThan", default_value_t = 10.0)]
    ignore_younger_than: f64,

    /// Number of concurrent requests to Kubernetes
    #[arg(short, long, env = "Concurrency", default_value_t = 10)]
    concurrency: u32,

    /// Granularity of the computed cost in seconds
    #[arg(short, long, env = "Granularity", default_value_t = 2.0)]
    granularity: f64,
}

/// Wait for termination signal (either SIGINT or SIGTERM)
#[cfg(unix)]
async fn wait_terminate() {
    use futures::{stream::FuturesUnordered, StreamExt};
    use tokio::signal::unix::{signal, SignalKind};
    let mut signals = Vec::new();

    // Register signal handlers
    for sig in [SignalKind::terminate(), SignalKind::interrupt()] {
        match signal(sig) {
            Ok(sig) => signals.push(sig),
            Err(err) => log::error!("Could not register signal handler: {err}"),
        }
    }

    // Wait for the first signal to trigger
    let mut signals = signals
        .iter_mut()
        .map(|sig| sig.recv())
        .collect::<FuturesUnordered<_>>();

    loop {
        match signals.next().await {
            // One of the signal triggered -> stop waiting
            Some(Some(())) => break,
            // One of the signal handler has been stopped -> continue waiting for the others
            Some(None) => (),
            // No more signal handlers are available, so wait indefinitely
            None => futures::future::pending::<()>().await,
        }
    }
}

#[cfg(windows)]
macro_rules! win_signal {
    ($($sig:ident),*$(,)?) => {
        $(
            let $sig = async {
                match tokio::signal::windows::$sig() {
                    Ok(mut $sig) => {
                        if $sig.recv().await.is_some() {
                            return;
                        }
                    }
                    Err(err) => log::error!(
                        "Could not register signal handler for {}: {err}",
                        stringify!($sig),
                    ),
                }
                futures::future::pending::<()>().await;
            };
        )*
        tokio::select! {
            $(
                _ = $sig => {}
            )*
        }
    }
}

/// Wait for termination signal (either SIGINT or SIGTERM)
#[cfg(windows)]
async fn wait_terminate() {
    win_signal!(ctrl_c, ctrl_close, ctrl_logoff, ctrl_shutdown);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    let cli = Cli::parse();

    let cost_model: Box<dyn CostModel> = Box::new(cost::age::AgeCost::new(
        cli.ignore_younger_than,
        cli.granularity,
    ));
    let metrics_scraper: Box<dyn MetricsScraper> = Box::new(
        metrics::prometheus::PrometheusScraper::new(&cli.prometheus_url, &cli.metrics_name)?,
    );
    let worker_updater: Box<dyn WorkerUpdater + Sync> =
        Box::new(updater::pod::PodUpdater::new(cli.concurrency as usize).await?);
    let mut cost_map = HashMap::new();

    let mut interval = tokio::time::interval(Duration::from_secs_f64(cli.period));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut wait_terminate = std::pin::pin!(wait_terminate());

    loop {
        // Wait for next tick or termination
        tokio::select! {
            _ = interval.tick() => {}
            _ = &mut wait_terminate => break,
        }

        // Scrap metrics
        let metrics = metrics_scraper.scrap_metrics().await?;

        // Convert metrics to updates
        let updates = metrics
            .into_iter()
            .filter_map(|metric| {
                // Compute actual cost for worker
                let cost = cost_model.metrics_to_cost(&metric);

                // Check last cost for this worker
                match cost_map.entry((metric.name.clone(), metric.namespace.clone())) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if *entry.get() != cost {
                            entry.insert(cost);
                        } else {
                            // Last cost was the same, no need to update the worker again
                            return None;
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(cost);
                    }
                }
                Some(WorkerUpdate {
                    name: metric.name,
                    namespace: metric.namespace,
                    cost,
                })
            })
            .collect::<Vec<_>>();

        let n = updates.len();

        // Apply updates
        worker_updater.update_many(updates).await?;

        match n {
            0 => {}
            1 => log::info!("{n} pod has been updated"),
            _ => log::info!("{n} pods have been updated"),
        }
    }

    Ok(())
}
