use std::sync::Arc;

use armonik::{
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{RequestContext, VersionsService},
    versions,
};
use futures::stream::FuturesUnordered;

use crate::utils::IntoStatus;

use super::Service;

impl VersionsService for Service {
    async fn list(
        self: Arc<Self>,
        _request: versions::list::Request,
        _context: RequestContext,
    ) -> std::result::Result<versions::list::Response, tonic::Status> {
        let mut cluster_versions = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .versions()
                    .list()
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let Some(versions) = cluster_versions.try_next().await? else {
            return Err(tonic::Status::internal("No cluster"));
        };

        while let Some(other) = cluster_versions.try_next().await? {
            if versions != other {
                return Err(tonic::Status::internal("Mismatch between clusters"));
            }
        }

        Ok(versions)
    }
}
