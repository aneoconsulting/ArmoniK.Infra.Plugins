use std::{collections::HashMap, sync::Arc};

use armonik::{
    health_checks,
    reexports::{tokio_util, tonic},
    server::HealthChecksService,
};

use crate::utils::{run_with_cancellation, IntoStatus};

use super::Service;

impl HealthChecksService for Service {
    async fn check(
        self: Arc<Self>,
        _request: health_checks::check::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<health_checks::check::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

            let mut services = HashMap::<String, (health_checks::Status, String)>::new();

            for cluster in self.clusters.values() {
                let health = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .health_checks()
                    .check()
                    .await
                    .map_err(IntoStatus::into_status)?;

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
}
