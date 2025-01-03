use std::{
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use armonik::reexports::{tokio_stream, tonic};
use lockfree_object_pool::LinearReusable;

use crate::{
    async_pool::{AsyncPool, PoolAwaitable},
    ref_guard::RefGuard,
};

#[derive(Clone)]
pub struct Cluster {
    pub name: String,
    pub endpoint: armonik::ClientConfig,
    pub pool: Arc<AsyncPool<Option<armonik::Client>>>,
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl PartialEq for Cluster {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint.endpoint == other.endpoint.endpoint
            && self.endpoint.identity == other.endpoint.identity
            && self.endpoint.override_target == other.endpoint.override_target
    }
}

impl Eq for Cluster {}

impl Hash for Cluster {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.endpoint.endpoint.hash(state);
        self.endpoint
            .identity
            .as_ref()
            .map(|identity| identity.0.as_ref())
            .hash(state);
        self.endpoint.override_target.hash(state);
    }
}

impl Cluster {
    pub fn new(name: String, config: armonik::ClientConfig) -> Self {
        Self {
            name,
            endpoint: config,
            pool: Arc::new(AsyncPool::new(|| async { None })),
        }
    }

    pub async fn client(&self) -> Result<ClusterClient, armonik::client::ConnectionError> {
        let span = tracing::debug_span!("Cluster", name = self.name);
        let client = self
            .pool
            .pull()
            .await
            .map_async(|reference| async move {
                match reference {
                    Some(x) => Ok(x),
                    None => {
                        tracing::debug!(
                            "Creating new client for cluster {}: {:?}",
                            self.name,
                            self.endpoint
                        );

                        let endpoint = self.endpoint.clone();

                        // Somehow, armonik::Client::with_config() is not Send
                        // So we starting a blocking executor that blocks on the future to ensure it stays on the same thread
                        tokio::task::spawn_blocking(move || {
                            futures::executor::block_on(armonik::Client::with_config(endpoint))
                        })
                        .await
                        .unwrap()
                        .map(|client| reference.insert(client))
                    }
                }
            })
            .await
            .into_result()?;
        Ok(ClusterClient(client, span))
    }
}

pub struct ClusterClient<'a>(
    RefGuard<LinearReusable<'a, PoolAwaitable<Option<armonik::Client>>>, &'a mut armonik::Client>,
    tracing::Span,
);

unsafe impl Send for ClusterClient<'_> {}

impl Deref for ClusterClient<'_> {
    type Target = armonik::Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for ClusterClient<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<armonik::Client> for ClusterClient<'_> {
    fn as_ref(&self) -> &armonik::Client {
        &self.0
    }
}

impl ClusterClient<'_> {
    pub fn span(&self) -> tracing::Span {
        self.1.clone()
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
                        .await.unwrap();

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
