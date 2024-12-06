#![allow(deprecated)]

use std::{collections::HashMap, sync::Arc};

use armonik::{
    reexports::{tokio, tokio_util, tonic},
    server::SubmitterService,
    submitter,
};
use futures::StreamExt as _;

use crate::utils::{impl_unary, run_with_cancellation, IntoStatus};

use super::Service;

impl SubmitterService for Service {
    async fn get_service_configuration(
        self: Arc<Self>,
        _request: submitter::get_service_configuration::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::get_service_configuration::Response, tonic::Status> {
        log::warn!("SubmitterService::GetServiceConfiguration is deprecated, please use ResultsService::GetServiceConfiguration instead");

        crate::utils::run_with_cancellation! {
            use cancellation_token;

            let mut min = 1 << 24;

            for (_, cluster) in self.clusters.iter() {
                let conf = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .results()
                    .get_service_configuration()
                    .await
                    .map_err(IntoStatus::into_status)?;

                min = min.min(conf.data_chunk_max_size);
            }

            Ok(submitter::get_service_configuration::Response {
                data_chunk_max_size: min,
            })
        }
    }

    async fn create_session(
        self: Arc<Self>,
        request: submitter::create_session::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::create_session::Response, tonic::Status> {
        log::warn!("SubmitterService::CreateSession is deprecated, please use SessionsService::CreateSession instead");

        let n = self.clusters.len();
        let i = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut err = None;

        for (_, cluster) in self.clusters.iter().cycle().skip(i % n).take(n) {
            match run_with_cancellation!(cancellation_token, cluster.client()) {
                Ok(client) => {
                    let response = run_with_cancellation!(
                        cancellation_token,
                        client.submitter().call(request.clone())
                    );

                    match response {
                        Ok(response) => return Ok(response),
                        Err(error) => err = Some(error.into_status()),
                    }
                }
                Err(error) => err = Some(error.into_status()),
            }
        }

        match err {
            Some(err) => Err(err),
            None => Err(tonic::Status::internal("No cluster")),
        }
    }

    async fn cancel_session(
        self: Arc<Self>,
        request: submitter::cancel_session::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::cancel_session::Response, tonic::Status> {
        log::warn!("SubmitterService::CancelSession is deprecated, please use SessionsService::CancelSession instead");

        impl_unary!(self.submitter, request, cancellation_token, session)
    }

    async fn list_tasks(
        self: Arc<Self>,
        request: submitter::list_tasks::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::list_tasks::Response, tonic::Status> {
        log::warn!(
            "SubmitterService::ListTasks is deprecated, please use TasksService::ListTasks instead"
        );

        run_with_cancellation! {
            use cancellation_token;

            let mut task_ids = Vec::new();

            for cluster in self.clusters.values() {
                let response = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .submitter()
                    .call(request.clone())
                    .await
                    .map_err(IntoStatus::into_status)?;

                task_ids.extend(response.task_ids);
            }

            Ok(submitter::list_tasks::Response { task_ids })
        }
    }

    async fn list_sessions(
        self: Arc<Self>,
        request: submitter::list_sessions::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::list_sessions::Response, tonic::Status> {
        log::warn!("SubmitterService::ListSessions is deprecated, please use SessionsService::ListSessions instead");

        run_with_cancellation! {
            use cancellation_token;

            let mut session_ids = Vec::new();

            for cluster in self.clusters.values() {
                let response = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .submitter()
                    .call(request.clone())
                    .await
                    .map_err(IntoStatus::into_status)?;

                session_ids.extend(response.session_ids);
            }

            Ok(submitter::list_sessions::Response { session_ids })
        }
    }

    async fn count_tasks(
        self: Arc<Self>,
        request: submitter::count_tasks::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::count_tasks::Response, tonic::Status> {
        log::warn!(
            "SubmitterService::CountTasks is deprecated, please use TasksService::CountTasksByStatus instead"
        );

        run_with_cancellation! {
            use cancellation_token;

            let mut status_count = HashMap::<armonik::TaskStatus, i32>::new();

            for cluster in self.clusters.values() {
                let response = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .submitter()
                    .call(request.clone())
                    .await
                    .map_err(IntoStatus::into_status)?
                    .values;

                for (status, count) in response {
                    *status_count.entry(status).or_default() += count;
                }
            }

            Ok(armonik::submitter::count_tasks::Response {
                values: status_count,
            })
        }
    }

    async fn try_get_task_output(
        self: Arc<Self>,
        request: submitter::try_get_task_output::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::try_get_task_output::Response, tonic::Status> {
        log::warn!(
            "SubmitterService::TryGetTaskOutput is deprecated, please use TasksService::GetTask instead"
        );
        crate::utils::impl_unary!(self.submitter, request, cancellation_token, session)
    }

    async fn wait_for_availability(
        self: Arc<Self>,
        request: submitter::wait_for_availability::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::wait_for_availability::Response, tonic::Status> {
        log::warn!("SubmitterService::WaitForAvailability is deprecated, please use EventsService::GetEvents instead");
        crate::utils::impl_unary!(self.submitter, request, cancellation_token, session)
    }

    async fn wait_for_completion(
        self: Arc<Self>,
        request: submitter::wait_for_completion::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::wait_for_completion::Response, tonic::Status> {
        log::warn!("SubmitterService::WaitForCompletion is deprecated, please use EventsService::GetEvents instead");
        run_with_cancellation! {
            use cancellation_token.clone();

            let mut status_count = HashMap::new();

            let mut wait_all = self
                .clusters
                .values()
                .map(|cluster| async {
                    cluster
                        .client()
                        .await
                        .map_err(IntoStatus::into_status)?
                        .submitter()
                        .call(request.clone())
                        .await
                        .map_err(IntoStatus::into_status)
                })
                .collect::<futures::stream::FuturesUnordered<_>>();

            while let Some(completion) = wait_all.next().await {
                let mut is_error = false;
                let mut is_cancelled = false;
                for (status, count) in completion?.values {
                    match status {
                        armonik::TaskStatus::Error => is_error = true,
                        armonik::TaskStatus::Cancelling | armonik::TaskStatus::Cancelled => {
                            is_cancelled = true
                        }
                        _ => (),
                    }
                    *status_count.entry(status).or_default() += count;
                }

                if (is_error && request.stop_on_first_task_error)
                    || (is_cancelled && request.stop_on_first_task_cancellation)
                {
                    std::mem::drop(wait_all);

                    return self
                        .count_tasks(
                            armonik::submitter::count_tasks::Request {
                                filter: request.filter,
                            },
                            cancellation_token,
                        )
                        .await;
                }
            }

            Ok(armonik::submitter::wait_for_completion::Response {
                values: status_count,
            })
        }
    }

    async fn cancel_tasks(
        self: Arc<Self>,
        request: submitter::cancel_tasks::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::cancel_tasks::Response, tonic::Status> {
        log::warn!(
            "SubmitterService::CancelTasks is deprecated, please use TasksService::CancelTasks instead"
        );
        run_with_cancellation! {
            use cancellation_token;

            for cluster in self.clusters.values() {
                cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .submitter()
                    .call(request.clone())
                    .await
                    .map_err(IntoStatus::into_status)?;
            }

            Ok(submitter::cancel_tasks::Response { })
        }
    }

    async fn task_status(
        self: Arc<Self>,
        request: submitter::task_status::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::task_status::Response, tonic::Status> {
        log::warn!(
            "SubmitterService::TaskStatus is deprecated, please use TasksService::ListTasks instead"
        );
        run_with_cancellation! {
            use cancellation_token;

            let mut task_status = HashMap::<String, armonik::TaskStatus>::new();

            for cluster in self.clusters.values() {
                let response = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .submitter()
                    .call(request.clone())
                    .await
                    .map_err(IntoStatus::into_status)?
                    .statuses;

                for (task_id, status) in response {
                    task_status.insert(task_id, status);
                }
            }

            Ok(submitter::task_status::Response {
                statuses: task_status
            })
        }
    }

    async fn result_status(
        self: Arc<Self>,
        request: submitter::result_status::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<submitter::result_status::Response, tonic::Status> {
        log::warn!("SubmitterService::ResultStatus is deprecated, please use ResultsService::ListResults instead");
        crate::utils::impl_unary!(self.submitter, request, cancellation_token, session)
    }

    async fn try_get_result(
        self: Arc<Self>,
        request: submitter::try_get_result::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Result<
        impl tonic::codegen::tokio_stream::Stream<
                Item = Result<submitter::try_get_result::Response, tonic::Status>,
            > + Send,
        tonic::Status,
    > {
        log::warn!(
            "SubmitterService::TryGetResult is deprecated, please use ResultsService::DownloadResultData instead"
        );
        crate::utils::run_with_cancellation! {
            use cancellation_token.clone();

            let Some(cluster) = self.get_cluster_from_session(&request.session_id).await? else {
                return Err(tonic::Status::not_found(format!(
                    "Session {} was not found",
                    request.session_id
                )));
            };

            let mut stream = cluster
                .client()
                .await
                .map_err(IntoStatus::into_status)?
                .submitter()
                .try_get_result(request.session_id, request.result_id)
                .await
                .map_err(IntoStatus::into_status)?;

            Ok(async_stream::try_stream! {
                while let Some(Some(item)) = cancellation_token.run_until_cancelled(stream.next()).await {
                    let item = item.map_err(IntoStatus::into_status)?;
                    yield item;
                }
            })
        }
    }

    async fn create_small_tasks(
        self: Arc<Self>,
        request: submitter::create_tasks::SmallRequest,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Result<submitter::create_tasks::Response, tonic::Status> {
        log::warn!(
            "SubmitterService::CreateSmallTasks is deprecated, please use a combination of ResultsService::CreateResults and TasksService::SubmitTasks instead"
        );
        crate::utils::impl_unary!(self.submitter, request, cancellation_token, session)
    }

    async fn create_large_tasks(
        self: Arc<Self>,
        request: impl tonic::codegen::tokio_stream::Stream<
                Item = Result<submitter::create_tasks::LargeRequest, tonic::Status>,
            > + Send
            + 'static,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Result<submitter::create_tasks::Response, tonic::Status> {
        log::warn!(
            "SubmitterService::CreateLargeTasks is deprecated, please use a combination of ResultsService::CreateResults and TasksService::SubmitTasks instead"
        );
        let mut request = Box::pin(request);

        match crate::utils::run_with_cancellation!(cancellation_token, request.next()) {
            Some(Ok(submitter::create_tasks::LargeRequest::InitRequest(
                submitter::create_tasks::InitRequest {
                    session_id,
                    task_options,
                },
            ))) => {
                let Some(cluster) = self.get_cluster_from_session(&session_id).await? else {
                    return Err(tonic::Status::not_found(format!(
                        "Session {} was not found",
                        session_id
                    )));
                };

                let (tx, rx) = tokio::sync::oneshot::channel();
                let mut tx = Some(tx);

                let stream = async_stream::stream! {
                    yield submitter::create_tasks::LargeRequest::InitRequest(
                        submitter::create_tasks::InitRequest {
                            session_id: session_id.clone(),
                            task_options: task_options.clone(),
                        },
                    );

                    while let Some(item) = request.next().await {
                        match item {
                            Ok(item) => yield item,
                            Err(err) => {
                                if let Some(tx) = tx.take() {
                                    _ = tx.send(err);
                                }
                                break;
                            }
                        }
                    }
                };

                let mut submitter_client = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .submitter();

                tokio::select! {
                    result = submitter_client.create_large_tasks(stream) => match result {
                        Ok(result) => Ok(armonik::submitter::create_tasks::Response::Status(result)),
                        Err(err) => Err(err.into_status()),
                    },
                    Ok(invalid) = rx => {
                        Err(invalid)
                    }
                    _ = cancellation_token.cancelled() => Err(tonic::Status::aborted("Cancellation token has been triggered"))
                }
            }
            Some(Ok(_)) => Err(tonic::Status::invalid_argument(
                "Could not create tasks, data sent before identifier",
            )),
            Some(Err(err)) => Err(err),
            None => Err(tonic::Status::invalid_argument(
                "Could not create tasks, no identifier nor data sent",
            )),
        }
    }
}
