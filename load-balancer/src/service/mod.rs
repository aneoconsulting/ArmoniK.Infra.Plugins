#![allow(clippy::mutable_key_type)]

use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use armonik::reexports::{tokio::sync::RwLock, tokio_stream::StreamExt, tonic::Status};

use crate::{cluster::Cluster, utils::IntoStatus};

mod applications;
mod auth;
mod events;
mod health_check;
mod partitions;
mod results;
mod sessions;
mod submitter;
mod tasks;
mod versions;

pub struct Service {
    clusters: HashMap<String, Arc<Cluster>>,
    mapping_session: RwLock<HashMap<String, (Arc<Cluster>, armonik::sessions::Raw)>>,
    mapping_result: RwLock<HashMap<String, Arc<Cluster>>>,
    mapping_task: RwLock<HashMap<String, Arc<Cluster>>>,
    counter: AtomicUsize,
}

impl Service {
    pub async fn new(clusters: impl IntoIterator<Item = (String, Cluster)>) -> Self {
        Self {
            clusters: clusters
                .into_iter()
                .map(|(name, cluster)| (name, Arc::new(cluster)))
                .collect(),
            mapping_session: RwLock::new(Default::default()),
            mapping_result: RwLock::new(Default::default()),
            mapping_task: RwLock::new(Default::default()),
            counter: AtomicUsize::new(0),
        }
    }

    pub async fn get_cluster_from_sessions<'a>(
        &'a self,
        session_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        let mut missing_ids = Vec::new();
        let mut mapping = HashMap::<Arc<Cluster>, Vec<String>>::new();

        {
            let guard = self.mapping_session.read().await;

            for &session_id in session_ids {
                if let Some(cluster) = guard.get(session_id) {
                    match mapping.entry(cluster.0.clone()) {
                        std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                            occupied_entry.get_mut().push(String::from(session_id));
                        }
                        std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert(vec![String::from(session_id)]);
                        }
                    }
                } else {
                    missing_ids.push(session_id);
                }
            }
        }

        if !missing_ids.is_empty() {
            let filter = missing_ids
                .iter()
                .map(|&session_id| {
                    [armonik::sessions::filter::Field {
                        field: armonik::sessions::Field::Raw(
                            armonik::sessions::RawField::SessionId,
                        ),
                        condition: armonik::sessions::filter::Condition::String(
                            armonik::FilterString {
                                value: String::from(session_id),
                                operator: armonik::FilterStringOperator::Equal,
                            },
                        ),
                    }]
                })
                .collect::<Vec<_>>();

            for cluster in self.clusters.values() {
                let sessions = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .sessions()
                    .list(
                        filter.clone(),
                        Default::default(),
                        true,
                        0,
                        filter.len() as i32,
                    )
                    .await
                    .map_err(IntoStatus::into_status)?
                    .sessions;

                if !sessions.is_empty() {
                    let cluster_mapping = mapping.entry(cluster.clone()).or_default();
                    let mut guard = self.mapping_session.write().await;
                    for session in sessions {
                        let session_id = session.session_id.clone();
                        guard
                            .entry(session_id.clone())
                            .or_insert_with(|| (cluster.clone(), session));
                        cluster_mapping.push(session_id);
                    }
                }
            }
        }

        Ok(mapping)
    }

    pub async fn get_cluster_from_session(
        &self,
        session_id: &str,
    ) -> Result<Option<Arc<Cluster>>, Status> {
        let sessions = self.get_cluster_from_sessions(&[session_id]).await?;

        Ok(sessions.into_keys().next())
    }

    pub async fn get_cluster_from_results<'a>(
        &'a self,
        result_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        let mut missing_ids = Vec::new();
        let mut mapping = HashMap::<Arc<Cluster>, Vec<String>>::new();

        {
            let guard = self.mapping_result.read().await;

            for &result_id in result_ids {
                if let Some(cluster) = guard.get(result_id) {
                    match mapping.entry(cluster.clone()) {
                        std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                            occupied_entry.get_mut().push(String::from(result_id));
                        }
                        std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert(vec![String::from(result_id)]);
                        }
                    }
                } else {
                    missing_ids.push(result_id);
                }
            }
        }

        if !missing_ids.is_empty() {
            let filter = missing_ids
                .iter()
                .map(|&result_id| {
                    [armonik::results::filter::Field {
                        field: armonik::results::Field::ResultId,
                        condition: armonik::results::filter::Condition::String(
                            armonik::FilterString {
                                value: String::from(result_id),
                                operator: armonik::FilterStringOperator::Equal,
                            },
                        ),
                    }]
                })
                .collect::<Vec<_>>();

            for cluster in self.clusters.values() {
                let results = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .results()
                    .list(filter.clone(), Default::default(), 0, filter.len() as i32)
                    .await
                    .map_err(IntoStatus::into_status)?
                    .results;

                if !results.is_empty() {
                    let cluster_mapping = mapping.entry(cluster.clone()).or_default();
                    let mut guard = self.mapping_result.write().await;
                    for result in results {
                        guard
                            .entry(result.result_id.clone())
                            .or_insert_with(|| cluster.clone());
                        cluster_mapping.push(result.result_id);
                    }
                }
            }
        }

        Ok(mapping)
    }

    pub async fn get_cluster_from_result(
        &self,
        result_id: &str,
    ) -> Result<Option<Arc<Cluster>>, Status> {
        let results = self.get_cluster_from_results(&[result_id]).await?;

        Ok(results.into_keys().next())
    }

    pub async fn get_cluster_from_tasks<'a>(
        &'a self,
        task_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        let mut missing_ids = Vec::new();
        let mut mapping = HashMap::<Arc<Cluster>, Vec<String>>::new();

        {
            let guard = self.mapping_task.read().await;

            for &task_id in task_ids {
                if let Some(cluster) = guard.get(task_id) {
                    match mapping.entry(cluster.clone()) {
                        std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                            occupied_entry.get_mut().push(String::from(task_id));
                        }
                        std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert(vec![String::from(task_id)]);
                        }
                    }
                } else {
                    missing_ids.push(task_id);
                }
            }
        }

        if !missing_ids.is_empty() {
            let filter = missing_ids
                .iter()
                .map(|&result_id| {
                    [armonik::tasks::filter::Field {
                        field: armonik::tasks::Field::Summary(armonik::tasks::SummaryField::TaskId),
                        condition: armonik::tasks::filter::Condition::String(
                            armonik::FilterString {
                                value: String::from(result_id),
                                operator: armonik::FilterStringOperator::Equal,
                            },
                        ),
                    }]
                })
                .collect::<Vec<_>>();

            for cluster in self.clusters.values() {
                let tasks = cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .tasks()
                    .list(
                        filter.clone(),
                        Default::default(),
                        false,
                        0,
                        filter.len() as i32,
                    )
                    .await
                    .map_err(IntoStatus::into_status)?
                    .tasks;

                if !tasks.is_empty() {
                    let cluster_mapping = mapping.entry(cluster.clone()).or_default();
                    let mut guard = self.mapping_task.write().await;
                    for task in tasks {
                        guard
                            .entry(task.task_id.clone())
                            .or_insert_with(|| cluster.clone());
                        cluster_mapping.push(task.task_id);
                    }
                }
            }
        }

        Ok(mapping)
    }

    pub async fn get_cluster_from_task(
        &self,
        task_id: &str,
    ) -> Result<Option<Arc<Cluster>>, Status> {
        let results = self.get_cluster_from_tasks(&[task_id]).await?;

        Ok(results.into_keys().next())
    }

    pub async fn update_sessions(&self) -> Result<(), Status> {
        for cluster in self.clusters.values() {
            let mut stream = std::pin::pin!(
                cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .get_all_sessions(Default::default(), Default::default())
                    .await?
            );

            while let Some(chunk) = stream.try_next().await? {
                let mut guard = self.mapping_session.write().await;

                for session in chunk {
                    match guard.entry(session.session_id.clone()) {
                        std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                            occupied_entry.get_mut().1 = session
                        }
                        std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert((cluster.clone(), session));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
