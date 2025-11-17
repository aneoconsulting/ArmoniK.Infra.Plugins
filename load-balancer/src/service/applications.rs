use std::sync::Arc;

use armonik::{
    applications,
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{ApplicationsService, RequestContext},
};

use crate::utils::{try_rpc, IntoStatus, RecoverableResult};

use super::Service;

impl ApplicationsService for Service {
    async fn list(
        self: Arc<Self>,
        request: applications::list::Request,
        _context: RequestContext,
    ) -> std::result::Result<applications::list::Response, tonic::Status> {
        let Ok(page) = usize::try_from(request.page) else {
            try_rpc!(bail tonic::Status::invalid_argument("Page should be positive"));
        };
        let Ok(page_size) = usize::try_from(request.page_size) else {
            try_rpc!(bail tonic::Status::invalid_argument("Page size should be positive"));
        };

        let mut applications = Vec::new();

        let streams = self.clusters.values().map(|cluster| {
            let request = request.clone();
            Box::pin(async_stream::stream! {
                let mut client = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?;
                let span = client.span();
                let stream = client
                    .get_all_applications(request.filters.clone(), request.sort.clone())
                    .instrument(span)
                    .await?;
                let mut stream = std::pin::pin!(stream);

                while let Some(item) = stream.next().await {
                    yield item;
                }

                yield Ok(vec![]);
            })
        });
        let mut streams = std::pin::pin!(futures::stream::select_all(streams));

        let mut error = RecoverableResult::new();
        while let Some(chunk) = streams.next().await {
            match chunk {
                Ok(chunk) => {
                    error.success(());
                    applications.extend(chunk);
                }
                Err(err) => {
                    tracing::warn!(
                        "Error while listing applications, listing could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        if !request.sort.fields.is_empty() {
            applications.sort_by(|a, b| {
                for field in &request.sort.fields {
                    let ordering = match field {
                        applications::Field::Unspecified => a.name.cmp(&b.name),
                        applications::Field::Name => a.name.cmp(&b.name),
                        applications::Field::Version => a.version.cmp(&b.version),
                        applications::Field::Namespace => a.namespace.cmp(&b.namespace),
                        applications::Field::Service => a.service.cmp(&b.service),
                    };

                    match (ordering, &request.sort.direction) {
                        (
                            std::cmp::Ordering::Less,
                            armonik::SortDirection::Unspecified | armonik::SortDirection::Asc,
                        ) => return std::cmp::Ordering::Less,
                        (std::cmp::Ordering::Less, armonik::SortDirection::Desc) => {
                            return std::cmp::Ordering::Greater
                        }
                        (std::cmp::Ordering::Equal, _) => (),
                        (
                            std::cmp::Ordering::Greater,
                            armonik::SortDirection::Unspecified | armonik::SortDirection::Asc,
                        ) => return std::cmp::Ordering::Greater,
                        (std::cmp::Ordering::Greater, armonik::SortDirection::Desc) => {
                            return std::cmp::Ordering::Less
                        }
                    }
                }

                std::cmp::Ordering::Equal
            });
        }

        let total = applications.len() as i32;

        Ok(armonik::applications::list::Response {
            applications: applications
                .into_iter()
                .skip(page * page_size)
                .take(page_size)
                .collect(),
            page: request.page,
            page_size: request.page_size,
            total,
        })
    }
}
