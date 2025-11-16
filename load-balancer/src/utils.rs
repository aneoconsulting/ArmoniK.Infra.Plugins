use armonik::reexports::tonic::Status;

use futures::{stream::futures_unordered, Stream, StreamExt};

macro_rules! impl_unary {
    ($self:ident.$service:ident, $request:ident, session) => {
        crate::utils::impl_unary!($self.$service, $request, {get_cluster_from_session, session_id, "Session {} was not found"})
    };
    ($self:ident.$service:ident, $request:ident, result) => {
        crate::utils::impl_unary!($self.$service, $request, {get_cluster_from_result, result_id, "Result {} was not found"})
    };
    ($self:ident.$service:ident, $request:ident, task) => {
        crate::utils::impl_unary!($self.$service, $request, {get_cluster_from_task, task_id, "Task {} was not found"})
    };

    ($self:ident.$service:ident, $request:ident, {$get_cluster:ident, $id:ident, $msg:literal}) => {
        {
            let Some(cluster) = crate::utils::try_rpc!(try $self.$get_cluster(&$request.$id).await) else {
                crate::utils::try_rpc!(bail tonic::Status::not_found(format!(
                    $msg,
                    $request.$id
                )));
            };

            let mut client = crate::utils::try_rpc!(try cluster
                .client()
                .await);
            let span = client.span();
            crate::utils::try_rpc!(map client.$service()
                .call($request)
                .instrument(span)
                .await)
        }
    };
}
pub(crate) use impl_unary;

macro_rules! try_rpc {
    (try $res:expr) => {
        match $res {
            Ok(result) => result,
            Err(err) => crate::utils::try_rpc!(bail err),
        }
    };
    (bail $err:expr) => {
        return Err(crate::utils::try_rpc!(warn $err))
    };
    (map $res:expr) => {
        match $res {
            Ok(result) => Result::<_, armonik::reexports::tonic::Status>::Ok(result),
            Err(err) => Err(crate::utils::try_rpc!(warn err)),
        }
    };
    (warn $err:expr) => {{
        let err = crate::utils::IntoStatus::into_status($err);
        tracing::warn!("{:?}: {}", err.code(), err.message());
        err
    }};
}

pub(crate) use try_rpc;

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

impl IntoStatus for Status {
    fn into_status(self) -> Status {
        self
    }
}

async fn stream_next<S: Stream + Unpin>(mut stream: S) -> (S, Option<<S as Stream>::Item>)
where
    <S as Stream>::Item: 'static,
{
    let res = stream.next().await;
    (stream, res)
}

pub fn merge_streams<'a, S>(
    streams: impl IntoIterator<Item = S>,
) -> impl Stream<Item = <S as Stream>::Item> + 'a
where
    S: Stream + Unpin + 'a,
    <S as Stream>::Item: 'static,
{
    let mut futures = futures_unordered::FuturesUnordered::new();
    for stream in streams {
        futures.push(stream_next(stream));
    }
    async_stream::stream! {
        while let Some((stream, res)) = futures.next().await {
            if let Some(item) = res {
                futures.push(stream_next(stream));
                yield item;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecoverableResult<T, E> {
    Unknown,
    Recovered(T),
    Error(E),
}

impl<T, E> Default for RecoverableResult<T, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, E> RecoverableResult<T, E> {
    pub fn new() -> Self {
        Self::Unknown
    }

    pub fn error(&mut self, error: E) {
        match self {
            RecoverableResult::Unknown => *self = RecoverableResult::Error(error),
            RecoverableResult::Recovered(_) => (),
            RecoverableResult::Error(_) => (),
        }
    }

    pub fn success(&mut self, value: T) {
        *self = RecoverableResult::Recovered(value)
    }

    pub fn to_result<F: FnOnce() -> Result<T, E>>(self, unknown: F) -> Result<T, E> {
        match self {
            RecoverableResult::Unknown => unknown(),
            RecoverableResult::Recovered(value) => Ok(value),
            RecoverableResult::Error(error) => Err(error),
        }
    }

    pub fn ok(self) -> Result<T, E>
    where
        T: Default,
    {
        self.to_result(|| Ok(Default::default()))
    }

    pub fn get_value(&self) -> Option<&T> {
        match self {
            RecoverableResult::Unknown => None,
            RecoverableResult::Recovered(value) => Some(value),
            RecoverableResult::Error(_) => None,
        }
    }
    pub fn get_mut_value(&mut self) -> Option<&mut T> {
        match self {
            RecoverableResult::Unknown => None,
            RecoverableResult::Recovered(value) => Some(value),
            RecoverableResult::Error(_) => None,
        }
    }
}
