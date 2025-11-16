#![allow(deprecated)]

use std::{collections::HashMap, sync::Arc};

use armonik::{
    reexports::{tokio, tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{RequestContext, SubmitterService},
    submitter,
};
use futures::stream::FuturesUnordered;

use crate::utils::{impl_unary, try_rpc, IntoStatus, RecoverableResult};

use super::Service;

impl SubmitterService for Service {
    async fn get_service_configuration(
        self: Arc<Self>,
        _request: submitter::get_service_configuration::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::get_service_configuration::Response, tonic::Status> {
        tracing::warn!("SubmitterService::GetServiceConfiguration is deprecated, please use ResultsService::GetServiceConfiguration instead");

        // Try to get the cached value
        let size = self
            .submitter_preferred_size
            .load(std::sync::atomic::Ordering::Relaxed);
        if size > 0 {
            return Ok(submitter::get_service_configuration::Response {
                data_chunk_max_size: size,
            });
        }

        let mut configurations = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .submitter()
                    .get_service_configuration()
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let mut size = RecoverableResult::<i32, _>::new();
        while let Some(conf) = configurations.next().await {
            match conf {
                Ok(conf) => {
                    if let Some(size) = size.get_mut_value() {
                        *size = conf.data_chunk_max_size.min(*size);
                    } else {
                        size.success(conf.data_chunk_max_size);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "Error while getting result service configuration, configuration could be partial: {err}"
                    );
                    size.error(err);
                }
            }
        }
        let size = size.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        // As all clients should get the same result, it is safe to store it unconditionally
        self.submitter_preferred_size
            .store(size, std::sync::atomic::Ordering::Relaxed);

        Ok(submitter::get_service_configuration::Response {
            data_chunk_max_size: size,
        })
    }

    async fn create_session(
        self: Arc<Self>,
        request: submitter::create_session::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::create_session::Response, tonic::Status> {
        tracing::warn!("SubmitterService::CreateSession is deprecated, please use SessionsService::CreateSession instead");

        let n = self.clusters.len();
        let i = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut err = None;

        for (_, cluster) in self.clusters.iter().cycle().skip(i % n).take(n) {
            match cluster.client().await {
                Ok(mut client) => {
                    let span = client.span();
                    let response = client
                        .submitter()
                        .call(request.clone())
                        .instrument(span)
                        .await;

                    match response {
                        Ok(response) => return Ok(response),
                        Err(error) => err = Some(error.into_status()),
                    }
                }
                Err(error) => err = Some(error.into_status()),
            }
        }

        match err {
            Some(err) => try_rpc!(bail err),
            None => try_rpc!(bail tonic::Status::internal("No cluster")),
        }
    }

    async fn cancel_session(
        self: Arc<Self>,
        request: submitter::cancel_session::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::cancel_session::Response, tonic::Status> {
        tracing::warn!("SubmitterService::CancelSession is deprecated, please use SessionsService::CancelSession instead");

        impl_unary!(self.submitter, request, session)
    }

    async fn list_tasks(
        self: Arc<Self>,
        request: submitter::list_tasks::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::list_tasks::Response, tonic::Status> {
        tracing::warn!(
            "SubmitterService::ListTasks is deprecated, please use TasksService::ListTasks instead"
        );

        let mut task_ids = Vec::new();

        let mut responses = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .submitter()
                    .call(request.clone())
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let mut error = RecoverableResult::new();
        while let Some(response) = responses.next().await {
            match response {
                Ok(response) => {
                    error.success(());
                    task_ids.extend(response.task_ids);
                }
                Err(err) => {
                    tracing::warn!("Error while listing tasks, listing could be partial: {err}");
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(submitter::list_tasks::Response { task_ids })
    }

    async fn list_sessions(
        self: Arc<Self>,
        request: submitter::list_sessions::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::list_sessions::Response, tonic::Status> {
        tracing::warn!("SubmitterService::ListSessions is deprecated, please use SessionsService::ListSessions instead");

        let mut session_ids = Vec::new();

        let mut responses = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .submitter()
                    .call(request.clone())
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let mut error = RecoverableResult::new();
        while let Some(response) = responses.next().await {
            match response {
                Ok(response) => {
                    error.success(());
                    session_ids.extend(response.session_ids);
                }
                Err(err) => {
                    tracing::warn!("Error while listing sessions, listing could be partial: {err}");
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(submitter::list_sessions::Response { session_ids })
    }

    async fn count_tasks(
        self: Arc<Self>,
        request: submitter::count_tasks::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::count_tasks::Response, tonic::Status> {
        tracing::warn!(
            "SubmitterService::CountTasks is deprecated, please use TasksService::CountTasksByStatus instead"
        );

        let mut status_count = HashMap::<armonik::TaskStatus, i32>::new();

        let mut responses = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .submitter()
                    .call(request.clone())
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let mut error = RecoverableResult::new();
        while let Some(response) = responses.next().await {
            match response {
                Ok(response) => {
                    error.success(());
                    for (status, count) in response.values {
                        *status_count.entry(status).or_default() += count;
                    }
                }
                Err(err) => {
                    tracing::warn!("Error while counting tasks, count could be partial: {err}");
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(armonik::submitter::count_tasks::Response {
            values: status_count,
        })
    }

    async fn try_get_task_output(
        self: Arc<Self>,
        request: submitter::try_get_task_output::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::try_get_task_output::Response, tonic::Status> {
        tracing::warn!(
            "SubmitterService::TryGetTaskOutput is deprecated, please use TasksService::GetTask instead"
        );
        crate::utils::impl_unary!(self.submitter, request, session)
    }

    async fn wait_for_availability(
        self: Arc<Self>,
        request: submitter::wait_for_availability::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::wait_for_availability::Response, tonic::Status> {
        tracing::warn!("SubmitterService::WaitForAvailability is deprecated, please use EventsService::GetEvents instead");
        crate::utils::impl_unary!(self.submitter, request, session)
    }

    async fn wait_for_completion(
        self: Arc<Self>,
        request: submitter::wait_for_completion::Request,
        context: RequestContext,
    ) -> std::result::Result<submitter::wait_for_completion::Response, tonic::Status> {
        tracing::warn!("SubmitterService::WaitForCompletion is deprecated, please use EventsService::GetEvents instead");
        let mut status_count = HashMap::new();

        let mut wait_all = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .submitter()
                    .call(request.clone())
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<futures::stream::FuturesUnordered<_>>();

        let mut error = RecoverableResult::new();
        while let Some(completion) = wait_all.next().await {
            let mut is_error = false;
            let mut is_cancelled = false;
            let completion = match completion {
                Ok(completion) => completion,
                Err(err) => {
                    tracing::warn!(
                        "Error while waiting for task completion, waiting could be partial: {err}"
                    );
                    error.error(err);
                    continue;
                }
            };
            error.success(());
            for (status, count) in completion.values {
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

                return try_rpc!(map self
                    .count_tasks(
                        armonik::submitter::count_tasks::Request {
                            filter: request.filter,
                        },
                        context,
                    )
                    .await);
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(armonik::submitter::wait_for_completion::Response {
            values: status_count,
        })
    }

    async fn cancel_tasks(
        self: Arc<Self>,
        request: submitter::cancel_tasks::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::cancel_tasks::Response, tonic::Status> {
        tracing::warn!(
            "SubmitterService::CancelTasks is deprecated, please use TasksService::CancelTasks instead"
        );

        let mut responses = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .submitter()
                    .call(request.clone())
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let mut success = false;
        let mut error = None;
        while let Some(response) = responses.next().await {
            match response {
                Ok(_) => success = true,
                Err(err) => {
                    tracing::warn!(
                        "Error while cancelling tasks, cancelling could be partial: {err}"
                    );
                    error.get_or_insert(err);
                }
            }
        }
        if let Some(error) = error {
            if !success {
                try_rpc!(bail error);
            }
        }

        Ok(submitter::cancel_tasks::Response {})
    }

    async fn task_status(
        self: Arc<Self>,
        request: submitter::task_status::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::task_status::Response, tonic::Status> {
        tracing::warn!(
            "SubmitterService::TaskStatus is deprecated, please use TasksService::ListTasks instead"
        );
        let mut task_status = HashMap::<String, armonik::TaskStatus>::new();
        let mut error = None;

        for cluster in self.clusters.values() {
            let mut client = match cluster.client().await {
                Ok(client) => client,
                Err(err) => {
                    tracing::warn!("Error while counting tasks, count could be partial: {err}");
                    error.get_or_insert(err.into_status());
                    continue;
                }
            };
            let span = client.span();
            let response = try_rpc!(try client
                .submitter()
                .call(request.clone())
                .instrument(span)
                .await)
            .statuses;

            for (task_id, status) in response {
                task_status.insert(task_id, status);
            }
        }
        if let Some(error) = error {
            if task_status.is_empty() {
                try_rpc!(bail error);
            }
        }

        Ok(submitter::task_status::Response {
            statuses: task_status,
        })
    }

    async fn result_status(
        self: Arc<Self>,
        request: submitter::result_status::Request,
        _context: RequestContext,
    ) -> std::result::Result<submitter::result_status::Response, tonic::Status> {
        tracing::warn!("SubmitterService::ResultStatus is deprecated, please use ResultsService::ListResults instead");
        crate::utils::impl_unary!(self.submitter, request, session)
    }

    async fn try_get_result(
        self: Arc<Self>,
        request: submitter::try_get_result::Request,
        _context: RequestContext,
    ) -> Result<
        impl tonic::codegen::tokio_stream::Stream<
                Item = Result<submitter::try_get_result::Response, tonic::Status>,
            > + Send,
        tonic::Status,
    > {
        tracing::warn!(
            "SubmitterService::TryGetResult is deprecated, please use ResultsService::DownloadResultData instead"
        );
        let Some(cluster) = try_rpc!(try self
            .get_cluster_from_session(&request.session_id)
            .await)
        else {
            try_rpc!(bail tonic::Status::not_found(format!(
                "Session {} was not found",
                request.session_id
            )));
        };

        let span = tracing::Span::current();
        Ok(async_stream::try_stream! {
            let mut client = try_rpc!(map cluster
                .client()
                .instrument(span)
                .await)?;
            let span = client.span();
            let mut stream = try_rpc!(map client
                .submitter()
                .try_get_result(request.session_id, request.result_id)
                .instrument(span)
                .await)?;
            while let Some(item) = stream.next().await {
                yield try_rpc!(map item)?;
            }
        }
        .in_current_span())
    }

    async fn create_small_tasks(
        self: Arc<Self>,
        request: submitter::create_tasks::SmallRequest,
        _context: RequestContext,
    ) -> Result<submitter::create_tasks::Response, tonic::Status> {
        tracing::warn!(
            "SubmitterService::CreateSmallTasks is deprecated, please use a combination of ResultsService::CreateResults and TasksService::SubmitTasks instead"
        );
        crate::utils::impl_unary!(self.submitter, request, session)
    }

    async fn create_large_tasks(
        self: Arc<Self>,
        request: impl tonic::codegen::tokio_stream::Stream<
                Item = Result<submitter::create_tasks::LargeRequest, tonic::Status>,
            > + Send
            + 'static,
        _context: RequestContext,
    ) -> Result<submitter::create_tasks::Response, tonic::Status> {
        tracing::warn!(
            "SubmitterService::CreateLargeTasks is deprecated, please use a combination of ResultsService::CreateResults and TasksService::SubmitTasks instead"
        );
        let mut request = Box::pin(request);

        match request.next().await {
            Some(Ok(submitter::create_tasks::LargeRequest::InitRequest(
                submitter::create_tasks::InitRequest {
                    session_id,
                    task_options,
                },
            ))) => {
                let Some(cluster) = try_rpc!(try self.get_cluster_from_session(&session_id).await)
                else {
                    try_rpc!(bail tonic::Status::not_found(format!(
                        "Session {session_id} was not found",
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

                let mut client = try_rpc!(try cluster
                    .client()
                    .await);
                let span = client.span();
                let mut submitter_client = client.submitter();

                tokio::select! {
                    result = submitter_client.create_large_tasks(stream).instrument(span) => match result {
                        Ok(result) => Ok(armonik::submitter::create_tasks::Response::Status(result)),
                        Err(err) => try_rpc!(bail err),
                    },
                    Ok(invalid) = rx => {
                        try_rpc!(bail invalid)
                    }
                }
            }
            Some(Ok(_)) => try_rpc!(bail tonic::Status::invalid_argument(
                "Could not create tasks, data sent before identifier",
            )),
            Some(Err(err)) => try_rpc!(bail err),
            None => try_rpc!(bail tonic::Status::invalid_argument(
                "Could not create tasks, no identifier nor data sent",
            )),
        }
    }
}
