use std::sync::Arc;

use armonik::{
    reexports::{tokio, tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    results,
    server::{RequestContext, ResultsService},
};
use futures::stream::FuturesUnordered;

use crate::{
    cluster::Cluster,
    utils::{try_rpc, IntoStatus, RecoverableResult},
};

use super::Service;

impl Service {
    pub async fn cluster_from_result_filter(
        self: Arc<Self>,
        filters: &results::filter::Or,
    ) -> Result<Option<Arc<Cluster>>, tonic::Status> {
        let mut requested_results = Vec::new();
        let mut requested_sessions = Vec::new();

        for and in &filters.or {
            let mut has_check = false;

            for field in and {
                match field {
                    armonik::results::filter::Field {
                        field: armonik::results::Field::SessionId,
                        condition:
                            armonik::results::filter::Condition::String(armonik::FilterString {
                                value,
                                operator: armonik::FilterStringOperator::Equal,
                            }),
                    } => {
                        requested_sessions.push(value.as_str());
                        has_check = true;
                    }
                    armonik::results::filter::Field {
                        field: armonik::results::Field::ResultId,
                        condition:
                            armonik::results::filter::Condition::String(armonik::FilterString {
                                value,
                                operator: armonik::FilterStringOperator::Equal,
                            }),
                    } => {
                        requested_results.push(value.as_str());
                        has_check = true;
                    }
                    _ => {}
                }
            }

            if !has_check {
                try_rpc!(bail armonik::reexports::tonic::Status::invalid_argument(String::from("Cannot determine the cluster from the filter, missing condition on session_id")));
            }
        }

        let (sessions, results) = tokio::join!(
            self.get_cluster_from_sessions(&requested_sessions),
            self.get_cluster_from_results(&requested_results)
        );

        let (mut sessions, mut results) = (sessions?.into_iter(), results?.into_iter());

        let cluster = match (sessions.next(), results.next()) {
            (None, None) => {
                return Ok(None);
            }
            (None, Some(res_cluster)) => res_cluster.0,
            (Some(ses_cluster), None) => ses_cluster.0,
            (Some(ses_cluster), Some(res_cluster)) => {
                if res_cluster != ses_cluster {
                    try_rpc!(bail tonic::Status::invalid_argument(
                        "Cannot determine the cluster from the filter, multiple clusters targeted",
                    ));
                }
                ses_cluster.0
            }
        };
        match (sessions.next(), results.next()) {
            (None, None) => Ok(Some(cluster)),
            _ => try_rpc!(bail tonic::Status::invalid_argument(
                "Cannot determine the cluster from the filter, multiple clusters targeted",
            )),
        }
    }
}

impl ResultsService for Service {
    async fn list(
        self: Arc<Self>,
        request: results::list::Request,
        context: RequestContext,
    ) -> std::result::Result<results::list::Response, tonic::Status> {
        let Some(cluster) = try_rpc!(try self
            .cluster_from_result_filter(&request.filters)
            .await)
        else {
            return Ok(results::list::Response {
                results: Vec::new(),
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
            .results()
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
        request: results::get::Request,
        context: RequestContext,
    ) -> std::result::Result<results::get::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, context, {get_cluster_from_result, id, "Result {} was not found"})
    }

    async fn get_owner_task_id(
        self: Arc<Self>,
        request: results::get_owner_task_id::Request,
        context: RequestContext,
    ) -> std::result::Result<results::get_owner_task_id::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, context, session)
    }

    async fn create_metadata(
        self: Arc<Self>,
        request: results::create_metadata::Request,
        context: RequestContext,
    ) -> std::result::Result<results::create_metadata::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, context, session)
    }

    async fn create(
        self: Arc<Self>,
        request: results::create::Request,
        context: RequestContext,
    ) -> std::result::Result<results::create::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, context, session)
    }

    async fn delete_data(
        self: Arc<Self>,
        request: results::delete_data::Request,
        context: RequestContext,
    ) -> std::result::Result<results::delete_data::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, context, session)
    }

    async fn import(
        self: Arc<Self>,
        request: results::import::Request,
        context: RequestContext,
    ) -> std::result::Result<results::import::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, context, session)
    }

    async fn get_service_configuration(
        self: Arc<Self>,
        _request: results::get_service_configuration::Request,
        context: RequestContext,
    ) -> std::result::Result<results::get_service_configuration::Response, tonic::Status> {
        // Try to get the cached value
        let size = self
            .result_preferred_size
            .load(std::sync::atomic::Ordering::Relaxed);
        if size > 0 {
            return Ok(results::get_service_configuration::Response {
                data_chunk_max_size: size,
            });
        }

        let mut configurations = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster
                    .client(&context)
                    .await
                    .map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .results()
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
                        "Error while getting result service configuration, configuration could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    size.error(err);
                }
            }
        }
        let size = size.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        // As all clients should get the same result, it is safe to store it unconditionally
        self.result_preferred_size
            .store(size, std::sync::atomic::Ordering::Relaxed);

        Ok(results::get_service_configuration::Response {
            data_chunk_max_size: size,
        })
    }

    async fn download(
        self: Arc<Self>,
        request: results::download::Request,
        context: RequestContext,
    ) -> Result<
        impl tonic::codegen::tokio_stream::Stream<
                Item = Result<results::download::Response, tonic::Status>,
            > + Send,
        tonic::Status,
    > {
        let Some(cluster) = try_rpc!(try self
            .get_cluster_from_session(&request.session_id)
            .await)
        else {
            return Err(tonic::Status::not_found(format!(
                "Session {} was not found",
                request.session_id
            )));
        };

        let span = tracing::Span::current();
        Ok(async_stream::try_stream! {
            let mut client = try_rpc!(map cluster
                .client(&context)
                .instrument(span)
                .await)?;
            let span = client.span();

            let mut stream = try_rpc!(map client
                .results()
                .download(request.session_id, request.result_id)
                .instrument(span)
                .await)?;

            while let Some(chunk) = stream.next().await {
                let chunk = try_rpc!(map chunk)?;
                yield results::download::Response{ data_chunk: chunk };
            }
        })
    }

    async fn upload(
        self: Arc<Self>,
        request: impl tonic::codegen::tokio_stream::Stream<
                Item = Result<results::upload::Request, tonic::Status>,
            > + Send
            + 'static,
        context: RequestContext,
    ) -> Result<results::upload::Response, tonic::Status> {
        let mut request = Box::pin(request);

        match request.next().await {
            Some(Ok(results::upload::Request::Identifier {
                session_id,
                result_id,
            })) => {
                let Some(cluster) = try_rpc!(try self.get_cluster_from_session(&session_id).await)
                else {
                    try_rpc!(bail tonic::Status::not_found(format!(
                        "Session {session_id} was not found",
                    )));
                };

                let (tx, rx) = tokio::sync::oneshot::channel();
                let mut tx = Some(tx);

                let stream = request.map_while(move |r| match r {
                    Ok(results::upload::Request::DataChunk(vec)) => Some(vec),
                    invalid => {
                        if let Some(tx) = tx.take() {
                            _ = tx.send(invalid);
                        }
                        None
                    }
                });

                let mut client = try_rpc!(try cluster
                    .client(&context)
                    .await);
                let span = client.span();
                let mut result_client = client.results();

                tokio::select! {
                    result = result_client.upload(session_id, result_id, stream).instrument(span) => {
                        match result {
                            Ok(result) => Ok(results::upload::Response { result }),
                            Err(err) => try_rpc!(bail err),
                        }
                    }
                    Ok(invalid) = rx => {
                        match invalid {
                            Ok(results::upload::Request::DataChunk(_)) => unreachable!(),
                            Ok(results::upload::Request::Identifier { .. }) => {
                                try_rpc!(bail tonic::Status::invalid_argument("Invalid upload request, identifier sent multiple times"))
                            }
                            Err(err) => try_rpc!(bail err),
                        }
                    }
                }
            }
            Some(Ok(results::upload::Request::DataChunk(_))) => {
                try_rpc!(bail tonic::Status::invalid_argument(
                    "Could not upload result, data sent before identifier",
                ))
            }
            Some(Err(err)) => try_rpc!(bail err),
            None => try_rpc!(bail tonic::Status::invalid_argument(
                "Could not upload result, no identifier nor data sent",
            )),
        }
    }
}
