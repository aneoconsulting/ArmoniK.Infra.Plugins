use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use armonik::reexports::tonic;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing as _;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub mod bag;
pub mod cluster;
pub mod service;
pub mod utils;

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LbConfig {
    pub clusters: HashMap<String, cluster::ClusterConfig<armonik::client::ClientConfigArgs>>,
    #[serde(default)]
    pub listen_ip: String,
    #[serde(default)]
    pub listen_port: u16,
    #[serde(default)]
    pub refresh_delay: String,
    #[serde(flatten)]
    pub service_options: service::ServiceOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Parser)]
pub struct Cli {
    /// Filename of the config file
    #[arg(short, long, default_value = "")]
    pub config: String,
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
            Err(err) => tracing::error!("Could not register signal handler: {err}"),
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
                    Err(err) => tracing::error!(
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
async fn main() -> Result<(), eyre::Report> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        ))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let mut conf = config::Config::builder()
        .add_source(
            config::Environment::with_prefix("LoadBalancer")
                .convert_case(config::Case::Snake)
                .separator("__"),
        )
        .set_default("listen_ip", "0.0.0.0")?
        .set_default("listen_port", 8081)?
        .set_default("refresh_delay", "10")?;

    if !cli.config.is_empty() {
        conf = conf.add_source(config::File::with_name(&cli.config));
    }

    let conf: LbConfig = conf.build()?.try_deserialize()?;

    tracing::trace!("{conf:?}");

    let mut clusters = HashMap::with_capacity(conf.clusters.len());
    let mut fallbacks = HashSet::new();

    for (name, cluster_config) in conf.clusters {
        if cluster_config.fallback {
            fallbacks.insert(name.clone());
        }
        clusters.insert(
            name.clone(),
            cluster::Cluster::new(
                name,
                cluster_config.try_map_client(armonik::ClientConfig::from_config_args)?,
            ),
        );
    }

    let service = Arc::new(service::Service::new(clusters, fallbacks, conf.service_options).await);
    let refresh_delay = std::time::Duration::from_secs_f64(conf.refresh_delay.parse()?);

    let router = tonic::transport::Server::builder()
        .trace_fn(|r| tracing::info_span!("gRPC", "path" = r.uri().path()))
        .http2_max_pending_accept_reset_streams(Some(65536))
        .add_service(
            armonik::api::v3::applications::applications_server::ApplicationsServer::from_arc(
                service.clone(),
            ),
        )
        .add_service(
            armonik::api::v3::auth::authentication_server::AuthenticationServer::from_arc(
                service.clone(),
            ),
        )
        .add_service(
            armonik::api::v3::events::events_server::EventsServer::from_arc(service.clone()),
        )
        .add_service(
            armonik::api::v3::partitions::partitions_server::PartitionsServer::from_arc(
                service.clone(),
            ),
        )
        .add_service(
            armonik::api::v3::health_checks::health_checks_service_server::HealthChecksServiceServer::from_arc(
                service.clone(),
            ),
        )
        .add_service(
            armonik::api::v3::results::results_server::ResultsServer::from_arc(service.clone()),
        )
        .add_service(
            armonik::api::v3::sessions::sessions_server::SessionsServer::from_arc(service.clone()),
        )
        .add_service(
            armonik::api::v3::submitter::submitter_server::SubmitterServer::from_arc(
                service.clone(),
            ),
        )
        .add_service(armonik::api::v3::tasks::tasks_server::TasksServer::from_arc(service.clone()))
        .add_service(
            armonik::api::v3::versions::versions_server::VersionsServer::from_arc(service.clone()),
        );

    let mut background_future = tokio::spawn({
        let service = service.clone();

        async move {
            let mut timer = tokio::time::interval(refresh_delay);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                timer.tick().await;
                if let Err(err) = service.update_sessions().await {
                    tracing::error!("Error while fetching sessions from clusters:\n{err:?}");
                }
            }
        }
    });

    let mut service_future =
        tokio::spawn(router.serve(SocketAddr::new(conf.listen_ip.parse()?, conf.listen_port)));

    tracing::info!("Application running");

    tokio::select! {
        output = &mut background_future => {
            if let Err(err) = output {
                tracing::error!("Background future had an error: {err:?}");
            }
        }
        output = &mut service_future => {
            match output {
                Ok(Ok(())) => (),
                Ok(Err(err)) => {
                    tracing::error!("Service had an error: {err:?}");
                }
                Err(err) => {
                    tracing::error!("Service future had an error: {err:?}");
                }
            }
        }
        _ = wait_terminate() => {
            tracing::info!("Application stopping");
        }
    }

    background_future.abort();
    service_future.abort();

    _ = background_future.await;
    _ = service_future.await;

    tracing::info!("Application stopped");

    Ok(())
}
