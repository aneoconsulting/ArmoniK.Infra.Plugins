use std::{
    collections::HashSet,
    hash::Hash,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
};

use armonik::{
    reexports::{
        http::{HeaderName, HeaderValue},
        hyper, tokio_stream, tonic,
    },
    server::RequestContext,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::bag::Bag;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterConfig<C = armonik::ClientConfig> {
    #[serde(flatten)]
    pub client: C,
    /// Size of the connection pool to this cluster
    #[serde(default)]
    pub pool_size: Option<usize>,
    /// Number of requests sent on a connection before it is recreated
    #[serde(default)]
    pub requests_per_connection: Option<usize>,
    /// Whether a connection can process multiple requests simultaneously
    #[serde(default)]
    pub multiplex: bool,
    /// Set the cluster as a fallback if no cluster could be matched
    #[serde(default)]
    pub fallback: bool,
    /// Headers to forward to the cluster if present
    #[serde(default)]
    pub forward_headers: Option<Vec<String>>,
}

impl<C> ClusterConfig<C> {
    pub fn try_map_client<T, E>(
        self,
        f: impl FnOnce(C) -> Result<T, E>,
    ) -> Result<ClusterConfig<T>, E> {
        Ok(ClusterConfig {
            client: f(self.client)?,
            pool_size: self.pool_size,
            requests_per_connection: self.requests_per_connection,
            multiplex: self.multiplex,
            fallback: self.fallback,
            forward_headers: self.forward_headers,
        })
    }
}

pub struct Cluster {
    pub name: String,
    pub config: armonik::ClientConfig,
    pub client: Bag<(armonik::Client, Option<NonZeroUsize>)>,
    pub requests_per_connection: Option<NonZeroUsize>,
    pub semaphore: Option<Semaphore>,
    pub multiplex: bool,
    pub forward_headers: HashSet<String>,
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("name", &self.name)
            .field("config", &self.config)
            .finish()
    }
}

impl PartialEq for Cluster {
    fn eq(&self, other: &Self) -> bool {
        self.config.endpoint == other.config.endpoint
            && self.config.identity == other.config.identity
            && self.config.override_target == other.config.override_target
    }
}

impl Eq for Cluster {}

impl Hash for Cluster {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config.endpoint.hash(state);
        self.config
            .identity
            .as_ref()
            .map(|identity| identity.0.as_ref())
            .hash(state);
        self.config.override_target.hash(state);
    }
}

impl Cluster {
    pub fn new(name: String, config: ClusterConfig) -> Self {
        Self {
            name,
            config: config.client,
            client: Default::default(),
            requests_per_connection: NonZeroUsize::new(
                config.requests_per_connection.unwrap_or(800),
            ),
            semaphore: NonZeroUsize::new(
                config
                    .pool_size
                    .unwrap_or(4 * tokio::runtime::Handle::current().metrics().num_workers()),
            )
            .map(|size| Semaphore::new(size.get())),
            multiplex: config.multiplex,
            forward_headers: config
                .forward_headers
                .unwrap_or(vec![
                    String::from("X-Certificate-Client-CN"),
                    String::from("X-Certificate-Client-Fingerprint"),
                ])
                .into_iter()
                .collect(),
        }
    }

    pub async fn client(
        &self,
        context: &RequestContext,
    ) -> Result<ClusterClient<'_>, armonik::client::ConnectionError> {
        let permit = match &self.semaphore {
            Some(semaphore) => Some(semaphore.acquire().await.unwrap()),
            None => None,
        };
        let (client, requests) = match self.client.pop() {
            Some(client) => client,
            None => {
                let client = armonik::Client::with_config(self.config.clone()).await?;
                (client, self.requests_per_connection)
            }
        };

        let internal = armonik::Client::with_channel(ClusterClientInternal {
            client: client.clone(),
            headers: context
                .headers()
                .iter()
                .filter_map(|(key, value)| {
                    if self.forward_headers.contains(key.as_str()) {
                        Some((key.clone(), value.clone()))
                    } else {
                        None
                    }
                })
                .collect(),
        });

        let mut client = ClusterClient {
            internal,
            client,
            requests,
            bag: &self.client,
            permit,
            span: tracing::debug_span!("Cluster", name = self.name),
        };

        if self.multiplex {
            client.release();
        }

        Ok(client)
    }
}

#[derive(Clone)]
pub struct ClusterClientInternal {
    client: armonik::Client,
    headers: Vec<(HeaderName, HeaderValue)>,
}

impl tonic::client::GrpcService<tonic::body::Body> for ClusterClientInternal {
    type ResponseBody =
        <armonik::Client as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody;
    type Error = <armonik::Client as tonic::client::GrpcService<tonic::body::Body>>::Error;
    type Future = <armonik::Client as tonic::client::GrpcService<tonic::body::Body>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.client.poll_ready(cx)
    }

    fn call(&mut self, mut request: hyper::http::Request<tonic::body::Body>) -> Self::Future {
        for (key, value) in &self.headers {
            request.headers_mut().insert(key.clone(), value.clone());
        }

        self.client.call(request)
    }
}

pub struct ClusterClient<'a> {
    internal: armonik::Client<ClusterClientInternal>,
    client: armonik::Client,
    requests: Option<NonZeroUsize>,
    bag: &'a Bag<(armonik::Client, Option<NonZeroUsize>)>,
    permit: Option<SemaphorePermit<'a>>,
    span: tracing::Span,
}

impl Deref for ClusterClient<'_> {
    type Target = armonik::Client<ClusterClientInternal>;

    fn deref(&self) -> &Self::Target {
        &self.internal
    }
}
impl DerefMut for ClusterClient<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.internal
    }
}

impl AsRef<armonik::Client<ClusterClientInternal>> for ClusterClient<'_> {
    fn as_ref(&self) -> &armonik::Client<ClusterClientInternal> {
        &self.internal
    }
}

impl Drop for ClusterClient<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

impl ClusterClient<'_> {
    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }
    fn release(&mut self) {
        match self.requests {
            Some(size) => {
                if let Some(size) = NonZeroUsize::new(size.get() - 1) {
                    self.bag.push((self.client.clone(), Some(size)));
                }
            }
            None => self.bag.push((self.client.clone(), None)),
        }
        std::mem::drop(self.permit.take());
    }
    pub async fn get_all_sessions(
        &mut self,
        filters: armonik::sessions::filter::Or,
        sort: armonik::sessions::Sort,
    ) -> Result<
        impl tokio_stream::Stream<Item = Result<Vec<armonik::sessions::Raw>, tonic::Status>> + '_,
        tonic::Status,
    > {
        let mut client = self.sessions();
        let page_size = 1000;
        let mut page_index = 0;

        Ok(armonik::reexports::tracing_futures::Instrument::instrument(
            async_stream::try_stream! {
                loop {
                    let page = client
                        .list(
                            filters.clone(),
                            sort.clone(),
                            true,
                            page_index,
                            page_size,
                        )
                        .await
                        .map_err(crate::utils::IntoStatus::into_status)?;

                    if page.sessions.is_empty() {
                        break;
                    }

                    page_index += 1;

                    yield page.sessions;
                }
            },
            tracing::trace_span!("get_all_sessions"),
        ))
    }

    pub async fn get_all_partitions(
        &mut self,
        filters: armonik::partitions::filter::Or,
        sort: armonik::partitions::Sort,
    ) -> Result<
        impl tokio_stream::Stream<Item = Result<Vec<armonik::partitions::Raw>, tonic::Status>> + '_,
        tonic::Status,
    > {
        let mut client = self.partitions();
        let page_size = 1000;
        let mut page_index = 0;

        Ok(armonik::reexports::tracing_futures::Instrument::instrument(
            async_stream::try_stream! {
                loop {
                    let page = client
                        .list(
                            filters.clone(),
                            sort.clone(),
                            page_index,
                            page_size,
                        )
                        .await
                        .map_err(crate::utils::IntoStatus::into_status)?;

                    if page.partitions.is_empty() {
                        break;
                    }

                    page_index += 1;

                    yield page.partitions;
                }
            },
            tracing::trace_span!("get_all_partitions"),
        ))
    }

    pub async fn get_all_applications(
        &mut self,
        filters: armonik::applications::filter::Or,
        sort: armonik::applications::Sort,
    ) -> Result<
        impl tokio_stream::Stream<Item = Result<Vec<armonik::applications::Raw>, tonic::Status>> + '_,
        tonic::Status,
    > {
        let mut client = self.applications();
        let page_size = 1000;
        let mut page_index = 0;

        Ok(armonik::reexports::tracing_futures::Instrument::instrument(
            async_stream::try_stream! {
                loop {
                    let page = client
                        .list(
                            filters.clone(),
                            sort.clone(),
                            page_index,
                            page_size,
                        )
                        .await
                        .map_err(crate::utils::IntoStatus::into_status)?;

                    if page.applications.is_empty() {
                        break;
                    }

                    page_index += 1;

                    yield page.applications;
                }
            },
            tracing::trace_span!("get_all_applications"),
        ))
    }
}
