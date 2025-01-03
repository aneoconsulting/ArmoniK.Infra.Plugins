use std::sync::Arc;

use armonik::{
    reexports::{tokio, tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    results,
    server::ResultsService,
};

use crate::utils::IntoStatus;

use super::Service;

impl ResultsService for Service {
    async fn list(
        self: Arc<Self>,
        request: results::list::Request,
    ) -> std::result::Result<results::list::Response, tonic::Status> {
        let mut requested_results = Vec::new();
        let mut requested_sessions = Vec::new();

        for and in &request.filters.or {
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
                return Err(armonik::reexports::tonic::Status::invalid_argument(String::from("Cannot determine the cluster from the filter, missing condition on session_id")));
            }
        }

        let (sessions, results) = tokio::join!(
            self.get_cluster_from_sessions(&requested_sessions),
            self.get_cluster_from_results(&requested_results)
        );

        let (mut sessions, mut results) = (sessions?.into_iter(), results?.into_iter());

        let cluster = match (sessions.next(), results.next()) {
            (None, None) => {
                return Ok(results::list::Response {
                    results: Vec::new(),
                    page: request.page,
                    page_size: request.page_size,
                    total: 0,
                });
            }
            (None, Some(res_cluster)) => res_cluster.0,
            (Some(ses_cluster), None) => ses_cluster.0,
            (Some(ses_cluster), Some(res_cluster)) => {
                if res_cluster != ses_cluster {
                    return Err(tonic::Status::invalid_argument(
                        "Cannot determine the cluster from the filter, multiple clusters targeted",
                    ));
                }
                ses_cluster.0
            }
        };
        match (sessions.next(), results.next()) {
            (None, None) => {}
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "Cannot determine the cluster from the filter, multiple clusters targeted",
                ));
            }
        }

        let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
        let span = client.span();
        client
            .results()
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
        request: results::get::Request,
    ) -> std::result::Result<results::get::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, {get_cluster_from_result, id, "Result {} was not found"})
    }

    async fn get_owner_task_id(
        self: Arc<Self>,
        request: results::get_owner_task_id::Request,
    ) -> std::result::Result<results::get_owner_task_id::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, session)
    }

    async fn create_metadata(
        self: Arc<Self>,
        request: results::create_metadata::Request,
    ) -> std::result::Result<results::create_metadata::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, session)
    }

    async fn create(
        self: Arc<Self>,
        request: results::create::Request,
    ) -> std::result::Result<results::create::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, session)
    }

    async fn delete_data(
        self: Arc<Self>,
        request: results::delete_data::Request,
    ) -> std::result::Result<results::delete_data::Response, tonic::Status> {
        crate::utils::impl_unary!(self.results, request, session)
    }

    async fn get_service_configuration(
        self: Arc<Self>,
        _request: results::get_service_configuration::Request,
    ) -> std::result::Result<results::get_service_configuration::Response, tonic::Status> {
        let mut min = 1 << 24;

        for (_, cluster) in self.clusters.iter() {
            let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
            let span = client.span();
            let conf = client
                .results()
                .get_service_configuration()
                .instrument(span)
                .await
                .map_err(IntoStatus::into_status)?;

            min = min.min(conf.data_chunk_max_size);
        }

        Ok(results::get_service_configuration::Response {
            data_chunk_max_size: min,
        })
    }

    async fn download(
        self: Arc<Self>,
        request: results::download::Request,
    ) -> Result<
        impl tonic::codegen::tokio_stream::Stream<
                Item = Result<results::download::Response, tonic::Status>,
            > + Send,
        tonic::Status,
    > {
        let Some(cluster) = self.get_cluster_from_session(&request.session_id).await? else {
            return Err(tonic::Status::not_found(format!(
                "Session {} was not found",
                request.session_id
            )));
        };

        let span = tracing::Span::current();
        Ok(async_stream::try_stream! {
            let mut client = cluster.client().instrument(span).await.map_err(IntoStatus::into_status)?;
            let span = client.span();

            let mut stream = client
                .results()
                .download(request.session_id, request.result_id)
                .instrument(span)
                .await
                .map_err(IntoStatus::into_status)?;

            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(IntoStatus::into_status)?;
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
    ) -> Result<results::upload::Response, tonic::Status> {
        let mut request = Box::pin(request);

        match request.next().await {
            Some(Ok(results::upload::Request::Identifier {
                session_id,
                result_id,
            })) => {
                let Some(cluster) = self.get_cluster_from_session(&session_id).await? else {
                    return Err(tonic::Status::not_found(format!(
                        "Session {} was not found",
                        session_id
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

                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                let mut result_client = client.results();

                tokio::select! {
                    result = result_client.upload(session_id, result_id, stream).instrument(span) => {
                        match result {
                            Ok(result) => Ok(results::upload::Response { result }),
                            Err(err) => Err(err.into_status())
                        }
                    }
                    Ok(invalid) = rx => {
                        match invalid {
                            Ok(results::upload::Request::DataChunk(_)) => unreachable!(),
                            Ok(results::upload::Request::Identifier { .. }) => {
                                Err(tonic::Status::invalid_argument("Invalid upload request, identifier sent multiple times"))
                            }
                            Err(err) => Err(err),
                        }
                    }
                }
            }
            Some(Ok(results::upload::Request::DataChunk(_))) => {
                Err(tonic::Status::invalid_argument(
                    "Could not upload result, data sent before identifier",
                ))
            }
            Some(Err(err)) => Err(err),
            None => Err(tonic::Status::invalid_argument(
                "Could not upload result, no identifier nor data sent",
            )),
        }
    }
}
