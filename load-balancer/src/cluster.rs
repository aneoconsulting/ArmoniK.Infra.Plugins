use std::{hash::Hash, ops::Deref};

use armonik::reexports::{tokio_stream, tonic};

#[derive(Debug, Default, Clone)]
pub struct Cluster {
    pub name: String,
    pub endpoint: armonik::ClientConfig,
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
        }
    }

    pub async fn client(&self) -> Result<ClusterClient, armonik::client::ConnectionError> {
        Ok(ClusterClient(
            armonik::Client::with_config(self.endpoint.clone()).await?,
        ))
    }
}

pub struct ClusterClient(armonik::Client);

impl Deref for ClusterClient {
    type Target = armonik::Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<armonik::Client> for ClusterClient {
    fn as_ref(&self) -> &armonik::Client {
        &self.0
    }
}

impl ClusterClient {
    pub async fn get_all_sessions(
        &self,
        filters: armonik::sessions::filter::Or,
        sort: armonik::sessions::Sort,
    ) -> Result<
        impl tokio_stream::Stream<Item = Result<Vec<armonik::sessions::Raw>, tonic::Status>>,
        tonic::Status,
    > {
        let mut client = self.sessions();
        let page_size = 1000;
        let mut page_index = 0;

        Ok(async_stream::try_stream! {
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
        })
    }

    pub async fn get_all_partitions(
        &self,
        filters: armonik::partitions::filter::Or,
        sort: armonik::partitions::Sort,
    ) -> Result<
        impl tokio_stream::Stream<Item = Result<Vec<armonik::partitions::Raw>, tonic::Status>>,
        tonic::Status,
    > {
        let mut client = self.partitions();
        let page_size = 1000;
        let mut page_index = 0;

        Ok(async_stream::try_stream! {
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
        })
    }

    pub async fn get_all_applications(
        &self,
        filters: armonik::applications::filter::Or,
        sort: armonik::applications::Sort,
    ) -> Result<
        impl tokio_stream::Stream<Item = Result<Vec<armonik::applications::Raw>, tonic::Status>>,
        tonic::Status,
    > {
        let mut client = self.applications();
        let page_size = 1000;
        let mut page_index = 0;

        Ok(async_stream::try_stream! {
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
        })
    }
}
