use std::sync::Arc;

use armonik::{
    reexports::{tonic, tracing_futures::Instrument},
    server::VersionsService,
    versions,
};

use crate::utils::IntoStatus;

use super::Service;

impl VersionsService for Service {
    async fn list(
        self: Arc<Self>,
        _request: versions::list::Request,
    ) -> std::result::Result<versions::list::Response, tonic::Status> {
        let mut cluster_versions = Vec::new();

        for cluster in self.clusters.values() {
            let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
            let span = client.span();
            let versions = client
                .versions()
                .list()
                .instrument(span)
                .await
                .map_err(IntoStatus::into_status)?;

            cluster_versions.push(versions);
        }

        let mut cluster_versions = cluster_versions.into_iter();

        let Some(versions) = cluster_versions.next() else {
            return Err(tonic::Status::internal("No cluster"));
        };

        for other in cluster_versions {
            if versions != other {
                return Err(tonic::Status::internal("Mismatch between clusters"));
            }
        }

        Ok(versions)
    }
}
