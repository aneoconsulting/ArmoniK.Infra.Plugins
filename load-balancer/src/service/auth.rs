use std::sync::Arc;

use armonik::{
    auth,
    reexports::{tokio_stream::StreamExt, tonic, tracing_futures::Instrument},
    server::{AuthService, RequestContext},
};
use futures::stream::FuturesUnordered;

use crate::utils::{try_rpc, IntoStatus, RecoverableResult};

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

        let mut user = RecoverableResult::new();

        while let Some(candidate) = users.next().await {
            match candidate {
                Ok(candidate) => match &user {
                    RecoverableResult::Unknown => user.success(candidate),
                    RecoverableResult::Recovered(user) => {
                        if *user != candidate {
                            try_rpc!(bail tonic::Status::internal("Mismatch between clusters"));
                        }
                    }
                    RecoverableResult::Error(_) => user.success(candidate),
                },
                Err(err) => {
                    tracing::warn!(
                        "Error while getting curring user, user permissions could be partial: {:?}: {}",
                        err.code(),
                        err.message(),
                    );
                    user.error(err);
                }
            }
        }
        let user = user.to_result(|| try_rpc!(bail tonic::Status::internal("No cluster")))?;

        Ok(auth::current_user::Response { user })
    }
}
