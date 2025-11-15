use std::{
    hash::Hash,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
};

use armonik::reexports::{tokio_stream, tonic};
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
    /// Whether a connection can process mutliple requests simultaneously
    #[serde(default)]
    pub multiplex: bool,
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
        }
    }

    pub async fn client(&self) -> Result<ClusterClient<'_>, armonik::client::ConnectionError> {
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

        let mut client = ClusterClient {
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

pub struct ClusterClient<'a> {
    client: armonik::Client,
    requests: Option<NonZeroUsize>,
    bag: &'a Bag<(armonik::Client, Option<NonZeroUsize>)>,
    permit: Option<SemaphorePermit<'a>>,
    span: tracing::Span,
}

impl Deref for ClusterClient<'_> {
    type Target = armonik::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
impl DerefMut for ClusterClient<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl AsRef<armonik::Client> for ClusterClient<'_> {
    fn as_ref(&self) -> &armonik::Client {
        &self.client
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
