use armonik::reexports::tonic::Status;

macro_rules! run_with_cancellation {
    (use $ct:expr; $($body:tt)*) => {
        crate::utils::run_with_cancellation!($ct, async move { $($body)* })
    };
    ($ct:expr, $fut:expr) => {
        match $ct.run_until_cancelled($fut).await {
            Some(res) => res,
            None => {
                Err(tonic::Status::aborted("Cancellation token has been triggered"))?
            }
        }
    }
}
pub(crate) use run_with_cancellation;

macro_rules! impl_unary {
    ($self:ident.$service:ident, $request:ident, $ct:ident, session) => {
        crate::utils::impl_unary!($self.$service, $request, $ct, {get_cluster_from_session, session_id, "Session {} was not found"})
    };
    ($self:ident.$service:ident, $request:ident, $ct:ident, result) => {
        crate::utils::impl_unary!($self.$service, $request, $ct, {get_cluster_from_result, result_id, "Result {} was not found"})
    };
    ($self:ident.$service:ident, $request:ident, $ct:ident, task) => {
        crate::utils::impl_unary!($self.$service, $request, $ct, {get_cluster_from_task, task_id, "Task {} was not found"})
    };

    ($self:ident.$service:ident, $request:ident, $ct:ident, {$get_cluster:ident, $id:ident, $msg:literal}) => {
        crate::utils::run_with_cancellation! {
            use $ct;

            let Some(cluster) = $self.$get_cluster(&$request.$id).await? else {
                return Err(tonic::Status::not_found(format!(
                    $msg,
                    $request.$id
                )));
            };

            cluster
                .client()
                .await
                .map_err(crate::utils::IntoStatus::into_status)?
                .$service()
                .call($request)
                .await
                .map_err(crate::utils::IntoStatus::into_status)
        }
    };
}
pub(crate) use impl_unary;

pub trait IntoStatus {
    fn into_status(self) -> Status;
}

impl IntoStatus for armonik::client::RequestError {
    fn into_status(self) -> Status {
        match self {
            armonik::client::RequestError::Grpc { source, .. } => *source,
            err => Status::internal(err.to_string()),
        }
    }
}

impl IntoStatus for armonik::client::ConnectionError {
    fn into_status(self) -> Status {
        Status::unavailable(self.to_string())
    }
}

impl IntoStatus for armonik::client::ConfigError {
    fn into_status(self) -> Status {
        Status::failed_precondition(self.to_string())
    }
}

impl IntoStatus for armonik::client::ReadEnvError {
    fn into_status(self) -> Status {
        Status::failed_precondition(self.to_string())
    }
}

impl IntoStatus for rusqlite::Error {
    fn into_status(self) -> Status {
        Status::failed_precondition(self.to_string())
    }
}
