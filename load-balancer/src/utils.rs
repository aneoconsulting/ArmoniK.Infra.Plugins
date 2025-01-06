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
            let Some(cluster) = $self.$get_cluster(&$request.$id).await? else {
                return Err(tonic::Status::not_found(format!(
                    $msg,
                    $request.$id
                )));
            };

            let mut client = cluster
                .client()
                .await
                .map_err(crate::utils::IntoStatus::into_status)?;
            let span = client.span();
            client.$service()
                .call($request)
                .instrument(span)
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
