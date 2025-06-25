use std::sync::Arc;

use armonik::{
    auth,
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{AuthService, RequestContext},
};
use futures::stream::FuturesUnordered;

use crate::utils::IntoStatus;

use super::Service;

impl AuthService for Service {
    async fn current_user(
        self: Arc<Self>,
        _request: auth::current_user::Request,
        _context: RequestContext,
    ) -> std::result::Result<auth::current_user::Response, tonic::Status> {
        let mut users = self
            .clusters
            .values()
            .map(|cluster| async {
                let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
                let span = client.span();
                client
                    .auth()
                    .current_user()
                    .instrument(span)
                    .await
                    .map_err(IntoStatus::into_status)
            })
            .collect::<FuturesUnordered<_>>();

        let Some(user) = users.try_next().await? else {
            return Err(tonic::Status::internal("No cluster"));
        };

        while let Some(other) = users.try_next().await? {
            if user != other {
                return Err(tonic::Status::internal("Mismatch between clusters"));
            }
        }

        Ok(auth::current_user::Response { user })
    }
}
