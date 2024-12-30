#![allow(clippy::mutable_key_type)]

use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicUsize, Arc},
};

use sessions::Session;
use tokio_rusqlite::Connection;

use armonik::reexports::{tokio::sync::RwLock, tokio_stream::StreamExt, tonic::Status};

use crate::{async_pool::AsyncPool, cluster::Cluster, utils::IntoStatus};

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
    db: AsyncPool<Connection>,
    mapping_result: RwLock<HashMap<String, Arc<Cluster>>>,
    mapping_task: RwLock<HashMap<String, Arc<Cluster>>>,
    counter: AtomicUsize,
}

impl Service {
    pub async fn new(clusters: impl IntoIterator<Item = (String, Cluster)>) -> Self {
        let pool = AsyncPool::new(|| async {
            Connection::open("file::memory:?cache=shared")
                .await
                .unwrap()
        });
        pool.execute_batch(
            "BEGIN;
            CREATE TABLE session(
                session_id TEXT PRIMARY KEY NOT NULL,
                cluster TEXT NOT NULL,
                status TINYINT NOT NULL,
                client_submission BOOL NOT NULL,
                worker_submission BOOL NOT NULL,
                partition_ids JSONB,
                default_task_options JSONB,
                created_at REAL,
                cancelled_at REAL,
                closed_at REAL,
                purged_at REAL,
                deleted_at REAL,
                duration REAL
            );
            CREATE INDEX session_status ON session(status);
            CREATE INDEX session_client_submission ON session(client_submission);
            CREATE INDEX session_worker_submission ON session(worker_submission);
            CREATE INDEX session_created_at ON session(created_at);
            CREATE INDEX session_cancelled_at ON session(cancelled_at);
            CREATE INDEX session_closed_at ON session(closed_at);
            CREATE INDEX session_purged_at ON session(purged_at);
            CREATE INDEX session_deleted_at ON session(deleted_at);
            CREATE INDEX session_duration ON session(duration);
            COMMIT;",
        )
        .await
        .unwrap();
        Self {
            clusters: clusters
                .into_iter()
                .map(|(name, cluster)| (name, Arc::new(cluster)))
                .collect(),
            db: pool,
            mapping_result: RwLock::new(Default::default()),
            mapping_task: RwLock::new(Default::default()),
            counter: AtomicUsize::new(0),
        }
    }

    pub async fn add_sessions(
        &self,
        sessions: Vec<armonik::sessions::Raw>,
        cluster_name: String,
    ) -> Result<(), Status> {
        self.db
            .call(move |conn| {
                let mut stmt = conn.prepare_cached(
                    "WITH data AS (
                        SELECT
                            e.value ->> 'session_id' as session_id,
                            e.value ->> 'cluster' as cluster,
                            e.value ->> 'status' as status,
                            e.value ->> 'client_submission' as client_submission,
                            e.value ->> 'worker_submission' as worker_submission,
                            e.value ->> 'partition_ids' as partition_ids,
                            e.value ->> 'default_task_options' as default_task_options,
                            e.value ->> 'created_at' as created_at,
                            e.value ->> 'cancelled_at' as cancelled_at,
                            e.value ->> 'closed_at' as closed_at,
                            e.value ->> 'purged_at' as purged_at,
                            e.value ->> 'deleted_at' as deleted_at,
                            e.value ->> 'duration' as duration
                        FROM json_each(?) e
                    )
                    INSERT OR REPLACE INTO session(
                        session_id,
                        cluster,
                        status,
                        client_submission,
                        worker_submission,
                        partition_ids,
                        default_task_options,
                        created_at,
                        cancelled_at,
                        closed_at,
                        purged_at,
                        deleted_at,
                        duration
                    ) SELECT
                        session_id,
                        cluster,
                        status,
                        client_submission,
                        worker_submission,
                        partition_ids,
                        default_task_options,
                        created_at,
                        cancelled_at,
                        closed_at,
                        purged_at,
                        deleted_at,
                        duration
                    FROM data",
                )?;

                stmt.execute([serde_json::to_string(
                    &sessions
                        .into_iter()
                        .map(|session| Session::from_grpc(session, cluster_name.clone()))
                        .collect::<Vec<_>>(),
                )
                .unwrap()])?;

                Result::<(), rusqlite::Error>::Ok(())
            })
            .await
            .map_err(IntoStatus::into_status)
    }

    pub async fn get_cluster_from_sessions<'a>(
        &'a self,
        session_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        let mut missing_ids: HashSet<_> = session_ids.iter().copied().map(String::from).collect();

        let (mapping, missing_ids) = self.db.call(move |conn| {
            let mut mapping = HashMap::<String, Vec<String>>::new();

            let mut stmt = conn.prepare_cached("SELECT session_id, cluster FROM session WHERE session_id IN (SELECT e.value FROM json_each(?) e)")?;
            let mut rows = stmt.query([serde_json::to_string(&missing_ids).unwrap()])?;

            while let Some(row) = rows.next()? {
                let session_id: String = row.get(0)?;
                let cluster: String = row.get(1)?;

                missing_ids.remove(session_id.as_str());
                match mapping.entry(cluster) {
                    std::collections::hash_map::Entry::Occupied(mut occupied_entry) => occupied_entry.get_mut().push(session_id),
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {vacant_entry.insert(vec![session_id]);},
                }
            }

            Result::<_, rusqlite::Error>::Ok((mapping, missing_ids))
        }).await.map_err(IntoStatus::into_status)?;

        let mut mapping = mapping
            .into_iter()
            .map(|(cluster_name, session_ids)| (self.clusters[&cluster_name].clone(), session_ids))
            .collect::<HashMap<_, _>>();

        if !missing_ids.is_empty() {
            let filter = missing_ids
                .into_iter()
                .map(|session_id| {
                    [armonik::sessions::filter::Field {
                        field: armonik::sessions::Field::Raw(
                            armonik::sessions::RawField::SessionId,
                        ),
                        condition: armonik::sessions::filter::Condition::String(
                            armonik::FilterString {
                                value: session_id,
                                operator: armonik::FilterStringOperator::Equal,
                            },
                        ),
                    }]
                })
                .collect::<Vec<_>>();

            for (cluster_name, cluster) in &self.clusters {
                let cluster_name = cluster_name.clone();
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
                    for session in &sessions {
                        cluster_mapping.push(session.session_id.clone());
                    }

                    self.add_sessions(sessions, cluster_name).await?;
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
        for (name, cluster) in &self.clusters {
            log::debug!("Refreshing sessions from {}\n  {:?}", name, cluster);
            let mut stream = std::pin::pin!(
                cluster
                    .client()
                    .await
                    .map_err(IntoStatus::into_status)?
                    .get_all_sessions(Default::default(), Default::default())
                    .await?
            );

            while let Some(chunk) = stream.try_next().await? {
                self.add_sessions(chunk, name.clone()).await?;
            }
        }

        Ok(())
    }
}
