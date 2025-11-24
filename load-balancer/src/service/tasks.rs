use std::{collections::HashMap, sync::Arc};

use armonik::{
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{RequestContext, TasksService},
    tasks,
};
use futures::stream::FuturesUnordered;

use crate::{
    cluster::Cluster,
    utils::{try_rpc, IntoStatus, RecoverableResult},
};

use super::Service;

impl Service {
    pub async fn cluster_from_task_filter(
        self: Arc<Self>,
        filters: &tasks::filter::Or,
    ) -> Result<Option<Arc<Cluster>>, tonic::Status> {
        let mut requested_tasks = Vec::new();
        let mut requested_sessions = Vec::new();

        for and in &filters.or {
            let mut has_check = false;

            for field in and {
                match field {
                    armonik::tasks::filter::Field {
                        field:
                            armonik::tasks::Field::Summary(armonik::tasks::SummaryField::SessionId),
                        condition:
                            armonik::tasks::filter::Condition::String(armonik::FilterString {
                                value,
                                operator: armonik::FilterStringOperator::Equal,
                            }),
                    } => {
                        requested_sessions.push(value.as_str());
                        has_check = true;
                    }
                    armonik::tasks::filter::Field {
                        field: armonik::tasks::Field::Summary(armonik::tasks::SummaryField::TaskId),
                        condition:
                            armonik::tasks::filter::Condition::String(armonik::FilterString {
                                value,
                                operator: armonik::FilterStringOperator::Equal,
                            }),
                    } => {
                        requested_tasks.push(value.as_str());
                        has_check = true;
                    }
                    _ => {}
                }
            }

            if !has_check {
                try_rpc!(bail tonic::Status::invalid_argument(
                    "Cannot determine the cluster from the filter, missing condition on session_id"
                ));
            }
        }

        let (sessions, results) = tokio::join!(
            self.get_cluster_from_sessions(&requested_sessions),
            self.get_cluster_from_tasks(&requested_tasks)
        );

        let (mut sessions, mut tasks) = (
            try_rpc!(try sessions).into_iter(),
            try_rpc!(try results).into_iter(),
        );

        let cluster = match (sessions.next(), tasks.next()) {
            (None, None) => {
                return Ok(None);
            }
            (None, Some(task_cluster)) => task_cluster.0,
            (Some(ses_cluster), None) => ses_cluster.0,
            (Some(ses_cluster), Some(task_cluster)) => {
                if task_cluster != ses_cluster {
                    try_rpc!(bail tonic::Status::invalid_argument(
                        "Cannot determine the cluster from the filter, multiple clusters targeted",
                    ));
                }
                ses_cluster.0
            }
        };
        match (sessions.next(), tasks.next()) {
            (None, None) => Ok(Some(cluster)),
            _ => try_rpc!(bail tonic::Status::invalid_argument(
                "Cannot determine the cluster from the filter, multiple clusters targeted",
            )),
        }
    }
}

impl TasksService for Service {
    async fn list(
        self: Arc<Self>,
        request: tasks::list::Request,
        context: RequestContext,
    ) -> std::result::Result<tasks::list::Response, tonic::Status> {
        let Some(cluster) = try_rpc!(try self
            .cluster_from_task_filter(&request.filters)
            .await)
        else {
            return Ok(tasks::list::Response {
                tasks: Vec::new(),
                page: request.page,
                page_size: request.page_size,
                total: 0,
            });
        };

        let mut client = try_rpc!(try cluster
            .client(&context)
            .await);
        let span = client.span();
        try_rpc!(map client
            .tasks()
            .call(request)
            .instrument(span)
            .await
            .map_err(|err| match err {
                armonik::client::RequestError::Grpc { source, .. } => *source,
                err => tonic::Status::internal(err.to_string()),
            })
        )
    }

    async fn list_detailed(
        self: Arc<Self>,
        request: tasks::list_detailed::Request,
        context: RequestContext,
    ) -> std::result::Result<tasks::list_detailed::Response, tonic::Status> {
        let Some(cluster) = try_rpc!(try self
            .cluster_from_task_filter(&request.filters)
            .await)
        else {
            return Ok(tasks::list_detailed::Response {
                tasks: Vec::new(),
                page: request.page,
                page_size: request.page_size,
                total: 0,
            });
        };

        let mut client = try_rpc!(try cluster.client(&context).await);
        let span = client.span();
        try_rpc!(map client
            .tasks()
            .call(request)
            .instrument(span)
            .await
            .map_err(|err| match err {
                armonik::client::RequestError::Grpc { source, .. } => *source,
                err => tonic::Status::internal(err.to_string()),
            })
        )
    }

    async fn get(
        self: Arc<Self>,
        request: tasks::get::Request,
        context: RequestContext,
    ) -> std::result::Result<tasks::get::Response, tonic::Status> {
        crate::utils::impl_unary!(self.tasks, request, context, task)
    }

    async fn cancel(
        self: Arc<Self>,
        request: tasks::cancel::Request,
        context: RequestContext,
    ) -> std::result::Result<tasks::cancel::Response, tonic::Status> {
        let mut futures = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster
                    .client(&context)
                    .await
                    .map_err(IntoStatus::into_status)?;
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
        let mut error = RecoverableResult::new();
        while let Some(chunk) = futures.next().await {
            match chunk {
                Ok(chunk) => {
                    error.success(());
                    tasks.extend(chunk.into_iter());
                }
                Err(err) => {
                    tracing::warn!(
                        "Error while cancelling tasks, cancelling could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(tasks::cancel::Response { tasks })
    }

    async fn get_result_ids(
        self: Arc<Self>,
        request: tasks::get_result_ids::Request,
        context: RequestContext,
    ) -> std::result::Result<tasks::get_result_ids::Response, tonic::Status> {
        let mut futures = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster
                    .client(&context)
                    .await
                    .map_err(IntoStatus::into_status)?;
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
        let mut error = RecoverableResult::new();
        while let Some(response) = futures.next().await {
            match response {
                Ok(response) => {
                    error.success(());
                    for (task_id, result_ids) in response {
                        task_results.entry(task_id).or_default().extend(result_ids);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "Error while getting result ids, listing could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(tasks::get_result_ids::Response { task_results })
    }

    async fn count_status(
        self: Arc<Self>,
        request: tasks::count_status::Request,
        context: RequestContext,
    ) -> std::result::Result<tasks::count_status::Response, tonic::Status> {
        let mut futures = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster
                    .client(&context)
                    .await
                    .map_err(IntoStatus::into_status)?;
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
        let mut error = RecoverableResult::new();
        while let Some(response) = futures.next().await {
            match response {
                Ok(response) => {
                    error.success(());
                    for count in response {
                        *status.entry(count.status).or_default() += count.count;
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "Error while counting tasks, count could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    error.error(err);
                }
            }
        }
        error.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

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
        context: RequestContext,
    ) -> std::result::Result<tasks::submit::Response, tonic::Status> {
        crate::utils::impl_unary!(self.tasks, request, context, session)
    }
}
