use std::sync::Arc;

use armonik::{
    events,
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::EventsService,
};

use crate::utils::IntoStatus;

use super::Service;

impl EventsService for Service {
    async fn subscribe(
        self: Arc<Self>,
        request: events::subscribe::Request,
    ) -> Result<
        impl tonic::codegen::tokio_stream::Stream<
                Item = Result<events::subscribe::Response, tonic::Status>,
            > + Send,
        tonic::Status,
    > {
        let events::subscribe::Request {
            session_id,
            task_filters,
            result_filters,
            returned_events,
        } = request;

        let cluster = self
            .get_cluster_from_session(&session_id)
            .await?
            .ok_or_else(|| {
                tonic::Status::not_found(format!("Session {} was not found", session_id))
            })?;

        let span = tracing::Span::current();
        let stream = async_stream::stream! {
            let mut client = cluster
                .client()
                .instrument(span)
                .await
                .map_err(IntoStatus::into_status)?;
            let span = client.span();

            let stream = client
                .events()
                .subscribe(session_id, task_filters, result_filters, returned_events)
                .instrument(span.clone())
                .await
                .map_err(IntoStatus::into_status)?;

            let mut stream = std::pin::pin!(stream);

            while let Some(event) = stream.next().await {
                yield event.map_err(IntoStatus::into_status);
            }
        };

        Ok(stream)
    }
}
