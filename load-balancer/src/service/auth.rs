use std::sync::Arc;

use armonik::{
    auth,
    reexports::{tokio_util, tonic},
    server::AuthService,
};

use crate::utils::{run_with_cancellation, IntoStatus};

use super::Service;

impl AuthService for Service {
    async fn current_user(
        self: Arc<Self>,
        _request: auth::current_user::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<auth::current_user::Response, tonic::Status> {
        run_with_cancellation! {
            use cancellation_token;

            let mut users = Vec::new();

            for cluster in self.clusters.values() {
                let user = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .auth()
                    .current_user()
                    .await
                    .map_err(IntoStatus::into_status)?;

                users.push(user);
            }

            let mut users = users.into_iter();

            let Some(user) = users.next() else {
                return Err(tonic::Status::internal("No cluster"));
            };

            for other in users {
                if user != other {
                    return Err(tonic::Status::internal("Mismatch between clusters"));
                }
            }

            Ok(auth::current_user::Response { user })
        }
    }
}
