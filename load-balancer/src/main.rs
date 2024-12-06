use std::sync::Arc;

use armonik::reexports::tonic;
use tower_http::trace::TraceLayer;

pub mod cluster;
pub mod ref_guard;
pub mod service;
pub mod utils;

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
async fn main() {
    env_logger::init();

    let service = Arc::new(
        service::Service::new([(
            String::from("A"),
            cluster::Cluster::new(armonik::ClientConfig::from_env().unwrap()),
        )])
        .await,
    );

    let router = tonic::transport::Server::builder()
        .layer(TraceLayer::new_for_grpc())
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
            let mut timer = tokio::time::interval(tokio::time::Duration::from_secs(10));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                timer.tick().await;
                if let Err(err) = service.update_sessions().await {
                    log::error!("Error while fetching sessions from clusters:\n{err:?}");
                }
            }
        }
    });

    let mut service_future = tokio::spawn(router.serve("0.0.0.0:1337".parse().unwrap()));

    log::info!("Application running");

    tokio::select! {
        output = &mut background_future => {
            if let Err(err) = output {
                log::error!("Background future had an error: {err:?}");
            }
        }
        output = &mut service_future => {
            match output {
                Ok(Ok(())) => (),
                Ok(Err(err)) => {
                    log::error!("Service had an error: {err:?}");
                }
                Err(err) => {
                    log::error!("Service future had an error: {err:?}");
                }
            }
        }
        _ = wait_terminate() => {
            log::info!("Application stopping");
        }
    }

    background_future.abort();
    service_future.abort();

    _ = background_future.await;
    _ = service_future.await;

    log::info!("Application stopped");
}
