use std::sync::Arc;

use armonik::{
    partitions,
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{PartitionsService, RequestContext},
};
use futures::stream::FuturesUnordered;

use crate::utils::{merge_streams, try_rpc, IntoStatus, RecoverableResult};

use super::Service;

impl PartitionsService for Service {
    async fn list(
        self: Arc<Self>,
        request: partitions::list::Request,
        _context: RequestContext,
    ) -> std::result::Result<partitions::list::Response, tonic::Status> {
        let Ok(page) = usize::try_from(request.page) else {
            try_rpc!(bail tonic::Status::invalid_argument("Page should be positive"));
        };
        let Ok(page_size) = usize::try_from(request.page_size) else {
            try_rpc!(bail tonic::Status::invalid_argument(
                "Page size should be positive",
            ));
        };

        let mut partitions = Vec::new();

        let streams = self.clusters.values().map(|cluster| {
            let request = request.clone();
            Box::pin(async_stream::stream! {
                let mut client = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?;
                let span = client.span();
                let stream = client
                    .get_all_partitions(request.filters.clone(), request.sort.clone())
                    .instrument(span.clone())
                    .await?;
                let mut stream = std::pin::pin!(stream.instrument(span));

                while let Some(item) = stream.next().await {
                    yield item;
                }

                yield Ok(vec![]);
            })
        });
        let mut streams = std::pin::pin!(merge_streams(streams));

        let mut error = RecoverableResult::new();
        while let Some(chunk) = streams.next().await {
            match chunk {
                Ok(chunk) => {
                    error.success(());
                    partitions.extend(chunk);
                }
                Err(err) => {
                    tracing::warn!(
                        "Error while listing partitions, listing could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        match &request.sort.field {
            partitions::Field::Unspecified => (),
            partitions::Field::Id => partitions.sort_by(|a, b| a.partition_id.cmp(&b.partition_id)),
            partitions::Field::ParentPartitionIds => {
                partitions.sort_by(|a, b| a.parent_partition_ids.cmp(&b.parent_partition_ids))
            }
            partitions::Field::PodReserved => {
                partitions.sort_by(|a, b| a.pod_reserved.cmp(&b.pod_reserved))
            }
            partitions::Field::PodMax => partitions.sort_by(|a, b| a.pod_max.cmp(&b.pod_max)),
            partitions::Field::PreemptionPercentage => {
                partitions.sort_by(|a, b| a.preemption_percentage.cmp(&b.preemption_percentage))
            }
            partitions::Field::Priority => partitions.sort_by(|a, b| a.priority.cmp(&b.priority)),
        }

        if matches!(&request.sort.direction, armonik::SortDirection::Desc) {
            partitions.reverse();
        }

        let total = partitions.len() as i32;

        Ok(armonik::partitions::list::Response {
            partitions: partitions
                .into_iter()
                .skip(page * page_size)
                .take(page_size)
                .collect(),
            page: request.page,
            page_size: request.page_size,
            total,
        })
    }

    async fn get(
        self: Arc<Self>,
        request: partitions::get::Request,
        _context: RequestContext,
    ) -> std::result::Result<partitions::get::Response, tonic::Status> {
        let mut err = None;

        let mut partitions = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();

                client
                    .partitions()
                    .call(request.clone())
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(response) = partitions.next().await {
            match response {
                Ok(response) => return Ok(response),
                Err(error) => err = Some(error),
            }
        }

        match err {
            Some(err) => try_rpc!(bail err),
            None => try_rpc!(bail tonic::Status::internal("No cluster")),
        }
    }
}
