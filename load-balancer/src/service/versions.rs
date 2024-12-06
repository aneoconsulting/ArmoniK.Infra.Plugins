use std::sync::Arc;

use armonik::{
    reexports::{tokio_util, tonic},
    server::VersionsService,
    versions,
};

use crate::utils::{run_with_cancellation, IntoStatus};

use super::Service;

impl VersionsService for Service {
    async fn list(
        self: Arc<Self>,
        _request: versions::list::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<versions::list::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

            let mut cluster_versions = Vec::new();

            for cluster in self.clusters.values() {
                let versions = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .versions()
                    .list()
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
}
