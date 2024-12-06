use std::sync::Arc;

use armonik::{
    events,
    reexports::{tokio_stream::StreamExt, tokio_util, tonic},
    server::EventsService,
};

use crate::utils::{run_with_cancellation, IntoStatus};

use super::Service;

impl EventsService for Service {
    async fn subscribe(
        self: Arc<Self>,
        request: events::subscribe::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Result<
        impl tonic::codegen::tokio_stream::Stream<
                Item = Result<events::subscribe::Response, tonic::Status>,
            > + Send,
        tonic::Status,
    > {
        run_with_cancellation! {
            use cancellation_token.clone();

            let events::subscribe::Request {
                session_id,
                task_filters,
                result_filters,
                returned_events,
            } = request;

            let client = self
                .get_cluster_from_session(&session_id)
                .await?
                .ok_or_else(|| tonic::Status::not_found(format!("Session {} was not found", session_id)))?
                .client()
                .await
                .map_err(IntoStatus::into_status)?;

            let stream = client
                .events()
                .subscribe(session_id, task_filters, result_filters, returned_events)
                .await
                .map_err(IntoStatus::into_status)?;

            let stream = async_stream::try_stream! {
                let mut stream = std::pin::pin!(stream);

                while let Some(Some(event)) = cancellation_token.run_until_cancelled(stream.next()).await {
                    yield event.map_err(IntoStatus::into_status)?;
                }
            };

            Ok(stream)
        }
    }
}
