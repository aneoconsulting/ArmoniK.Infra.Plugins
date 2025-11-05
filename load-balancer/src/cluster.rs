use std::{
    hash::Hash,
    ops::{Deref, DerefMut},
};

use armonik::reexports::{tokio_stream, tonic};

pub struct Cluster {
    pub name: String,
    pub config: armonik::ClientConfig,
    pub client: async_once_cell::OnceCell<armonik::Client>,
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
    pub fn new(name: String, config: armonik::ClientConfig) -> Self {
        Self {
            name,
            config,
            client: Default::default(),
        }
    }

    pub async fn client(&self) -> Result<ClusterClient, armonik::client::ConnectionError> {
        let span = tracing::debug_span!("Cluster", name = self.name);

        let client = self
            .client
            .get_or_try_init(armonik::Client::with_config(self.config.clone()))
            .await?;
        Ok(ClusterClient(client.clone(), span))
    }
}

pub struct ClusterClient(armonik::Client, tracing::Span);

unsafe impl Send for ClusterClient {}

impl Deref for ClusterClient {
    type Target = armonik::Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for ClusterClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<armonik::Client> for ClusterClient {
    fn as_ref(&self) -> &armonik::Client {
        &self.0
    }
}

impl ClusterClient {
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
