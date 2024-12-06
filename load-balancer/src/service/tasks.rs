use std::{collections::HashMap, sync::Arc};

use armonik::{
    reexports::{tokio_util, tonic},
    server::TasksService,
    tasks,
};

use crate::utils::{run_with_cancellation, IntoStatus};

use super::Service;

impl TasksService for Service {
    async fn list(
        self: Arc<Self>,
        request: tasks::list::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<tasks::list::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

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

            match cluster
                .client()
                .await
                .map_err(IntoStatus::into_status)?
                .tasks()
                .call(request)
                .await
            {
                Ok(response) => Ok(response),
                Err(err) => match err {
                    armonik::client::RequestError::Grpc { source, .. } => Err(*source),
                    err => Err(tonic::Status::internal(err.to_string())),
                },
            }
        }
    }

    async fn list_detailed(
        self: Arc<Self>,
        request: tasks::list_detailed::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<tasks::list_detailed::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

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

            match cluster
                .client()
                .await
                .map_err(IntoStatus::into_status)?
                .tasks()
                .call(request)
                .await
            {
                Ok(response) => Ok(response),
                Err(err) => match err {
                    armonik::client::RequestError::Grpc { source, .. } => Err(*source),
                    err => Err(tonic::Status::internal(err.to_string())),
                },
            }
        }
    }

    async fn get(
        self: Arc<Self>,
        request: tasks::get::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<tasks::get::Response, tonic::Status> {
        crate::utils::impl_unary!(self.tasks, request, cancellation_token, task)
    }

    async fn cancel(
        self: Arc<Self>,
        request: tasks::cancel::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<tasks::cancel::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

            let mut tasks = Vec::new();
            for cluster in self.clusters.values() {
                tasks.extend(
                    cluster
                        .client()
                        .await
                        .map_err(IntoStatus::into_status)?
                        .tasks()
                        .call(request.clone())
                        .await
                        .map_err(IntoStatus::into_status)?
                        .tasks
                        .into_iter(),
                );
            }

            Ok(tasks::cancel::Response { tasks })
        }
    }

    async fn get_result_ids(
        self: Arc<Self>,
        request: tasks::get_result_ids::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<tasks::get_result_ids::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

            let mut task_results = HashMap::<String, Vec<String>>::new();
            for cluster in self.clusters.values() {
                let response = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .tasks()
                    .call(request.clone())
                    .await
                    .map_err(IntoStatus::into_status)?;

                for (task_id, result_ids) in response.task_results {
                    task_results.entry(task_id).or_default().extend(result_ids);
                }
            }

            Ok(tasks::get_result_ids::Response { task_results })
        }
    }

    async fn count_status(
        self: Arc<Self>,
        request: tasks::count_status::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<tasks::count_status::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

            let mut status = HashMap::<armonik::TaskStatus, i32>::new();

            for cluster in self.clusters.values() {
                let response = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .tasks()
                    .call(request.clone())
                    .await
                    .map_err(IntoStatus::into_status)?
                    .status;

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
    }

    async fn submit(
        self: Arc<Self>,
        request: tasks::submit::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<tasks::submit::Response, tonic::Status> {
        crate::utils::impl_unary!(self.tasks, request, cancellation_token, session)
    }
}
