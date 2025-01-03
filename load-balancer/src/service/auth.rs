use std::sync::Arc;

use armonik::{
    auth,
    reexports::{tonic, tracing_futures::Instrument},
    server::AuthService,
};

use crate::utils::IntoStatus;

use super::Service;

impl AuthService for Service {
    async fn current_user(
        self: Arc<Self>,
        _request: auth::current_user::Request,
    ) -> std::result::Result<auth::current_user::Response, tonic::Status> {
        let mut users = Vec::new();

        for cluster in self.clusters.values() {
            let mut client = cluster.client().await.map_err(IntoStatus::into_status)?;
            let span = client.span();
            let user = client
                .auth()
                .current_user()
                .instrument(span)
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
