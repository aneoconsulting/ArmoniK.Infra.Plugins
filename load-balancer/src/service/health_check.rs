use std::{collections::HashMap, sync::Arc};

use armonik::{
    health_checks,
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{HealthChecksService, RequestContext},
};
use futures::stream::FuturesUnordered;

use crate::utils::{IntoStatus, RecoverableResult};

use super::Service;

impl HealthChecksService for Service {
    async fn check(
        self: Arc<Self>,
        _request: health_checks::check::Request,
        _context: RequestContext,
    ) -> std::result::Result<health_checks::check::Response, tonic::Status> {
        let mut services = HashMap::<String, (health_checks::Status, String)>::new();

        let mut healths = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .health_checks()
                    .check()
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let mut error = RecoverableResult::new();
        while let Some(health) = healths.next().await {
            match health {
                Ok(health) => {
                    error.success(());
                    for service in health {
                        match services.entry(service.name) {
                            std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                                let health = occupied_entry.get_mut();
                                if health.0 < service.health {
                                    *health = (service.health, service.message);
                                }
                            }
                            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert((service.health, service.message));
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!("Error while checking health, listing could be partial: {err}");
                    error.error(err);
                }
            }
        }
        error.to_result(|| Err(tonic::Status::internal("No cluster")))?;

        Ok(health_checks::check::Response {
            services: services
                .into_iter()
                .map(|(name, (health, message))| health_checks::ServiceHealth {
                    name,
                    message,
                    health,
                })
                .collect(),
        })
    }
}
