use std::sync::Arc;

use armonik::{
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{RequestContext, VersionsService},
    versions,
};
use futures::stream::FuturesUnordered;

use crate::utils::{try_rpc, IntoStatus, RecoverableResult};

use super::Service;

impl VersionsService for Service {
    async fn list(
        self: Arc<Self>,
        _request: versions::list::Request,
        context: RequestContext,
    ) -> std::result::Result<versions::list::Response, tonic::Status> {
        let mut cluster_versions = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster
                    .client(&context)
                    .await
                    .map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .versions()
                    .list()
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let mut versions = RecoverableResult::new();
        while let Some(response) = cluster_versions.next().await {
            match response {
                Ok(response) => {
                    if let Some(versions) = versions.get_value() {
                        if *versions != response {
                            return Err(tonic::Status::internal("Mismatch between clusters"));
                        }
                    } else {
                        versions.success(response);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "Error while listing service versions, versions could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    versions.error(err);
                }
            }
        }
        let versions =
            versions.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(versions)
    }
}
