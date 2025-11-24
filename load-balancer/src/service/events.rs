use std::sync::Arc;

use armonik::{
    events,
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{EventsService, RequestContext},
};

use crate::utils::try_rpc;

use super::Service;

impl EventsService for Service {
    async fn subscribe(
        self: Arc<Self>,
        request: events::subscribe::Request,
        context: RequestContext,
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

        let cluster = try_rpc!(try self
            .get_cluster_from_session(&session_id)
            .await);
        let cluster = try_rpc!(try cluster
            .ok_or_else(|| tonic::Status::not_found(format!("Session {session_id} was not found"))));

        let span = tracing::Span::current();
        let stream = async_stream::stream! {
            let mut client = try_rpc!(map cluster
                .client(&context)
                .instrument(span)
                .await)?;
            let span = client.span();

            let stream = try_rpc!(map client
                .events()
                .subscribe(session_id, task_filters, result_filters, returned_events)
                .instrument(span.clone())
                .await)?;

            let mut stream = std::pin::pin!(stream);

            while let Some(event) = stream.next().await {
                yield try_rpc!(map event);
            }
        };

        Ok(stream)
    }
}
