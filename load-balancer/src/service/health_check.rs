use std::{collections::HashMap, sync::Arc};

use armonik::{
    health_checks,
    reexports::{tonic, tracing_futures::Instrument},
    server::HealthChecksService,
};

use crate::utils::IntoStatus;

use super::Service;

impl HealthChecksService for Service {
    async fn check(
        self: Arc<Self>,
        _request: health_checks::check::Request,
    ) -> std::result::Result<health_checks::check::Response, tonic::Status> {
        let mut services = HashMap::<String, (health_checks::Status, String)>::new();

        for cluster in self.clusters.values() {
            let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
            let span = client.span();
            let health = client
                .health_checks()
                .check()
                .instrument(span)
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
