use std::{collections::HashMap, sync::Arc};

use armonik::{
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{RequestContext, TasksService},
    tasks,
};
use futures::stream::FuturesUnordered;

use crate::utils::IntoStatus;

use super::Service;

impl TasksService for Service {
    async fn list(
        self: Arc<Self>,
        request: tasks::list::Request,
        _context: RequestContext,
    ) -> std::result::Result<tasks::list::Response, tonic::Status> {
        let mut requested_sessions = Vec::new();

        for and in &request.filters.or {
            let mut has_check = false;

            for field in and {
                if let armonik::tasks::filter::Field {
                    field: armonik::tasks::Field::Summary(armonik::tasks::SummaryField::SessionId),
                    condition:
                        armonik::tasks::filter::Condition::String(armonik::FilterString {
                            value,
                            operator: armonik::FilterStringOperator::Equal,
                        }),
                } = field
                {
                    requested_sessions.push(value.as_str());
                    has_check = true;
                }
            }

            if !has_check {
                return Err(armonik::reexports::tonic::Status::invalid_argument(String::from("Cannot determine the cluster from the filter, missing condition on session_id")));
            }
        }

        let mut sessions = self
            .get_cluster_from_sessions(&requested_sessions)
            .await?
            .into_iter();

        let Some((cluster, _)) = sessions.next() else {
            return Ok(tasks::list::Response {
                tasks: Vec::new(),
                page: request.page,
                page_size: request.page_size,
                total: 0,
            });
        };

        if sessions.next().is_some() {
            return Err(tonic::Status::invalid_argument(
                "Cannot determine the cluster from the filter, multiple clusters targeted",
            ));
        }

        let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
        let span = client.span();
        client
            .tasks()
            .call(request)
            .instrument(span)
            .await
            .map_err(|err| match err {
                armonik::client::RequestError::Grpc { source, .. } => *source,
                err => tonic::Status::internal(err.to_string()),
            })
    }

    async fn list_detailed(
        self: Arc<Self>,
        request: tasks::list_detailed::Request,
        _context: RequestContext,
    ) -> std::result::Result<tasks::list_detailed::Response, tonic::Status> {
        let mut requested_sessions = Vec::new();

        for and in &request.filters.or {
            let mut has_check = false;

            for field in and {
                if let armonik::tasks::filter::Field {
                    field: armonik::tasks::Field::Summary(armonik::tasks::SummaryField::SessionId),
                    condition:
                        armonik::tasks::filter::Condition::String(armonik::FilterString {
                            value,
                            operator: armonik::FilterStringOperator::Equal,
                        }),
                } = field
                {
                    requested_sessions.push(value.as_str());
                    has_check = true;
                }
            }

            if !has_check {
                return Err(armonik::reexports::tonic::Status::invalid_argument(String::from("Cannot determine the cluster from the filter, missing condition on session_id")));
            }
        }

        let mut sessions = self
            .get_cluster_from_sessions(&requested_sessions)
            .await?
            .into_iter();

        let Some((cluster, _)) = sessions.next() else {
            return Ok(tasks::list_detailed::Response {
                tasks: Vec::new(),
                page: request.page,
                page_size: request.page_size,
                total: 0,
            });
        };

        if sessions.next().is_some() {
            return Err(tonic::Status::invalid_argument(
                "Cannot determine the cluster from the filter, multiple clusters targeted",
            ));
        }

        let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
        let span = client.span();
        client
            .tasks()
            .call(request)
            .instrument(span)
            .await
            .map_err(|err| match err {
                armonik::client::RequestError::Grpc { source, .. } => *source,
                err => tonic::Status::internal(err.to_string()),
            })
    }

    async fn get(
        self: Arc<Self>,
        request: tasks::get::Request,
        _context: RequestContext,
    ) -> std::result::Result<tasks::get::Response, tonic::Status> {
        crate::utils::impl_unary!(self.tasks, request, task)
    }

    async fn cancel(
        self: Arc<Self>,
        request: tasks::cancel::Request,
        _context: RequestContext,
    ) -> std::result::Result<tasks::cancel::Response, tonic::Status> {
        let mut futures = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                Result::<_, tonic::Status>::Ok(
                    client
                        .tasks()
                        .call(request.clone())
                        .instrument(span)
                        .await
                        .map_err(IntoStatus::into_status)?
                        .tasks,
                )
            })
            .collect::<FuturesUnordered<_>>();

        let mut tasks = Vec::new();
        while let Some(chunk) = futures.try_next().await? {
            tasks.extend(chunk.into_iter())
        }

        Ok(tasks::cancel::Response { tasks })
    }

    async fn get_result_ids(
        self: Arc<Self>,
        request: tasks::get_result_ids::Request,
        _context: RequestContext,
    ) -> std::result::Result<tasks::get_result_ids::Response, tonic::Status> {
        let mut futures = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                Result::<_, tonic::Status>::Ok(
                    client
                        .tasks()
                        .call(request.clone())
                        .instrument(span)
                        .await
                        .map_err(IntoStatus::into_status)?
                        .task_results,
                )
            })
            .collect::<FuturesUnordered<_>>();

        let mut task_results = HashMap::<String, Vec<String>>::new();
        while let Some(response) = futures.try_next().await? {
            for (task_id, result_ids) in response {
                task_results.entry(task_id).or_default().extend(result_ids);
            }
        }

        Ok(tasks::get_result_ids::Response { task_results })
    }

    async fn count_status(
        self: Arc<Self>,
        request: tasks::count_status::Request,
        _context: RequestContext,
    ) -> std::result::Result<tasks::count_status::Response, tonic::Status> {
        let mut futures = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                Result::<_, tonic::Status>::Ok(
                    client
                        .tasks()
                        .call(request.clone())
                        .instrument(span)
                        .await
                        .map_err(IntoStatus::into_status)?
                        .status,
                )
            })
            .collect::<FuturesUnordered<_>>();

        let mut status = HashMap::<armonik::TaskStatus, i32>::new();
        while let Some(response) = futures.try_next().await? {
            for count in response {
                *status.entry(count.status).or_default() += count.count;
            }
        }

        Ok(armonik::tasks::count_status::Response {
            status: status
                .into_iter()
                .map(|(status, count)| armonik::StatusCount { status, count })
                .collect(),
        })
    }

    async fn submit(
        self: Arc<Self>,
        request: tasks::submit::Request,
        _context: RequestContext,
    ) -> std::result::Result<tasks::submit::Response, tonic::Status> {
        crate::utils::impl_unary!(self.tasks, request, session)
    }
}
