#![allow(clippy::mutable_key_type)]

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicI32, AtomicUsize},
        Arc,
    },
};

use quick_cache::sync::Cache;
use sessions::Session;
use tokio_rusqlite::Connection;

use armonik::reexports::{tokio_stream::StreamExt, tonic::Status, tracing_futures::Instrument};

use crate::{
    async_pool::AsyncPool,
    cluster::Cluster,
    utils::{merge_streams, IntoStatus},
};

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
    mapping_session: Cache<String, Arc<Cluster>>,
    mapping_result: Cache<String, Arc<Cluster>>,
    mapping_task: Cache<String, Arc<Cluster>>,
    counter: AtomicUsize,
    result_preferred_size: AtomicI32,
    submitter_preferred_size: AtomicI32,
}

impl Service {
    pub async fn new(clusters: impl IntoIterator<Item = (String, Cluster)>) -> Self {
        let pool = AsyncPool::new(|| async {
            Connection::open("file::memory:?cache=shared&psow=1")
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
            tracing::trace_span!("create_table"),
        )
        .await
        .unwrap();
        Self {
            clusters: clusters
                .into_iter()
                .map(|(name, cluster)| (name, Arc::new(cluster)))
                .collect(),
            db: pool,
            mapping_session: Cache::new(1000000),
            mapping_result: Cache::new(10000000),
            mapping_task: Cache::new(10000000),
            counter: AtomicUsize::new(0),
            result_preferred_size: AtomicI32::new(0),
            submitter_preferred_size: AtomicI32::new(0),
        }
    }

    pub async fn add_sessions(
        &self,
        sessions: Vec<armonik::sessions::Raw>,
        cluster: Arc<Cluster>,
    ) -> Result<(), Status> {
        let span = tracing::trace_span!("add_sessions");

        self.db
            .call(span.clone(), move |conn| {
                let prepare_span = tracing::trace_span!(parent: &span, "prepare").entered();
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
                std::mem::drop(prepare_span);

                let _execute_span = tracing::trace_span!(parent: &span, "execute").entered();
                stmt.execute([serde_json::to_string(
                    &sessions
                        .into_iter()
                        .map(|session| Session::from_grpc(session, cluster.name.clone()))
                        .collect::<Vec<_>>(),
                )
                .unwrap()])?;

                Result::<(), rusqlite::Error>::Ok(())
            })
            .await
            .map_err(IntoStatus::into_status)
    }

    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_sessions(
        &self,
        session_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        let mut missing_ids = HashSet::new();
        let mut mapping = HashMap::<Arc<Cluster>, Vec<String>>::new();

        for &session_id in session_ids {
            if let Some(cluster) = self.mapping_session.get(session_id) {
                match mapping.entry(cluster) {
                    std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                        occupied_entry.get_mut().push(String::from(session_id));
                    }
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(vec![String::from(session_id)]);
                    }
                }
            } else {
                missing_ids.insert(String::from(session_id));
            }
        }

        if !missing_ids.is_empty() {
            let name_mapping;
            (name_mapping, missing_ids) = self.db.call(tracing::Span::current(), move |conn| {
                let mut name_mapping = HashMap::<String, Vec<String>>::new();

                let prepare_span = tracing::trace_span!("prepare");
                let mut stmt = conn.prepare_cached("SELECT session_id, cluster FROM session WHERE session_id IN (SELECT e.value FROM json_each(?) e)")?;
                std::mem::drop(prepare_span);

                let _execute_span = tracing::trace_span!("execute");
                let mut rows = stmt.query([serde_json::to_string(&missing_ids).unwrap()])?;

                while let Some(row) = rows.next()? {
                    let session_id: String = row.get(0)?;
                    let cluster: String = row.get(1)?;

                    missing_ids.remove(session_id.as_str());
                    match name_mapping.entry(cluster) {
                        std::collections::hash_map::Entry::Occupied(mut occupied_entry) => occupied_entry.get_mut().push(session_id),
                        std::collections::hash_map::Entry::Vacant(vacant_entry) => {vacant_entry.insert(vec![session_id]);},
                    }
                }

                Result::<_, rusqlite::Error>::Ok((name_mapping, missing_ids))
            }).await.map_err(IntoStatus::into_status)?;

            for (cluster_name, mut sessions_ids) in name_mapping {
                let cluster = self.clusters[&cluster_name].clone();
                self.mapping_session.insert(cluster_name, cluster.clone());
                match mapping.entry(cluster) {
                    std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                        occupied_entry.get_mut().append(&mut sessions_ids);
                    }
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(sessions_ids);
                    }
                }
            }
        }

        if !missing_ids.is_empty() {
            let filter = missing_ids
                .iter()
                .map(|session_id| {
                    [armonik::sessions::filter::Field {
                        field: armonik::sessions::Field::Raw(
                            armonik::sessions::RawField::SessionId,
                        ),
                        condition: armonik::sessions::filter::Condition::String(
                            armonik::FilterString {
                                value: session_id.clone(),
                                operator: armonik::FilterStringOperator::Equal,
                            },
                        ),
                    }]
                })
                .collect::<Vec<_>>();

            let mut list_all = self
                .clusters
                .values()
                .map(|cluster| async {
                    let mut client = match cluster.client().await {
                        Ok(client) => client,
                        Err(err) => return (cluster.clone(), Err(IntoStatus::into_status(err))),
                    };
                    let span = client.span();
                    let response = match client
                        .sessions()
                        .list(
                            filter.clone(),
                            Default::default(),
                            true,
                            0,
                            filter.len() as i32,
                        )
                        .instrument(span)
                        .await
                    {
                        Ok(response) => response,
                        Err(err) => return (cluster.clone(), Err(IntoStatus::into_status(err))),
                    };
                    (cluster.clone(), Ok(response.sessions))
                })
                .collect::<futures::stream::FuturesUnordered<_>>();

            let mut errors = Vec::new();
            while let Some((cluster, list)) = list_all.next().await {
                match list {
                    Ok(sessions) => {
                        if !sessions.is_empty() {
                            let cluster_mapping = mapping.entry(cluster.clone()).or_default();
                            for session in &sessions {
                                missing_ids.remove(session.session_id.as_str());
                                cluster_mapping.push(session.session_id.clone());
                            }

                            self.add_sessions(sessions, cluster.clone()).await?;
                        }
                    }
                    Err(err) => {
                        errors.push((cluster, err));
                    }
                }
            }

            if !missing_ids.is_empty() {
                let mut message = String::new();
                let mut sep = "";
                for (cluster, error) in errors {
                    let cluster_name = &cluster.name;
                    message.push_str(&format!(
                        "{sep}Error while fetching sessions from cluster {cluster_name}: {error}"
                    ));
                    sep = "\n";
                }
                return Err(Status::unavailable(message));
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

    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_results(
        &self,
        result_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        let mut missing_ids = HashSet::new();
        let mut mapping = HashMap::<Arc<Cluster>, Vec<String>>::new();

        for &result_id in result_ids {
            if let Some(cluster) = self.mapping_result.get(result_id) {
                match mapping.entry(cluster.clone()) {
                    std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                        occupied_entry.get_mut().push(String::from(result_id));
                    }
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(vec![String::from(result_id)]);
                    }
                }
            } else {
                missing_ids.insert(result_id);
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

            let mut list_all = self
                .clusters
                .values()
                .map(|cluster| async {
                    let mut client = match cluster.client().await {
                        Ok(client) => client,
                        Err(err) => return (cluster.clone(), Err(IntoStatus::into_status(err))),
                    };
                    let span = client.span();
                    let response = match client
                        .results()
                        .list(filter.clone(), Default::default(), 0, filter.len() as i32)
                        .instrument(span)
                        .await
                    {
                        Ok(response) => response,
                        Err(err) => return (cluster.clone(), Err(IntoStatus::into_status(err))),
                    };
                    (cluster.clone(), Ok(response.results))
                })
                .collect::<futures::stream::FuturesUnordered<_>>();

            let mut errors = Vec::new();
            while let Some((cluster, list)) = list_all.next().await {
                match list {
                    Ok(results) => {
                        if !results.is_empty() {
                            let cluster_mapping = mapping.entry(cluster.clone()).or_default();
                            for result in &results {
                                missing_ids.remove(result.result_id.as_str());
                                cluster_mapping.push(result.result_id.clone());
                                self.mapping_result
                                    .insert(result.result_id.clone(), cluster.clone());
                            }
                        }
                    }
                    Err(err) => {
                        errors.push((cluster, err));
                    }
                }
            }

            if !missing_ids.is_empty() {
                let mut message = String::new();
                let mut sep = "";
                for (cluster, error) in errors {
                    let cluster_name = &cluster.name;
                    message.push_str(&format!(
                        "{sep}Error while fetching results from cluster {cluster_name}: {error}"
                    ));
                    sep = "\n";
                }
                return Err(Status::unavailable(message));
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

    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_tasks(
        &self,
        task_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        let mut missing_ids = HashSet::new();
        let mut mapping = HashMap::<Arc<Cluster>, Vec<String>>::new();

        for &task_id in task_ids {
            if let Some(cluster) = self.mapping_task.get(task_id) {
                match mapping.entry(cluster.clone()) {
                    std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                        occupied_entry.get_mut().push(String::from(task_id));
                    }
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(vec![String::from(task_id)]);
                    }
                }
            } else {
                missing_ids.insert(task_id);
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

            let mut list_all = self
                .clusters
                .values()
                .map(|cluster| async {
                    let mut client = match cluster.client().await {
                        Ok(client) => client,
                        Err(err) => return (cluster.clone(), Err(IntoStatus::into_status(err))),
                    };
                    let span = client.span();
                    let response = match client
                        .tasks()
                        .list(
                            filter.clone(),
                            Default::default(),
                            false,
                            0,
                            filter.len() as i32,
                        )
                        .instrument(span)
                        .await
                    {
                        Ok(response) => response,
                        Err(err) => return (cluster.clone(), Err(IntoStatus::into_status(err))),
                    };
                    (cluster.clone(), Ok(response.tasks))
                })
                .collect::<futures::stream::FuturesUnordered<_>>();

            let mut errors = Vec::new();
            while let Some((cluster, list)) = list_all.next().await {
                match list {
                    Ok(tasks) => {
                        if !tasks.is_empty() {
                            let cluster_mapping = mapping.entry(cluster.clone()).or_default();
                            for task in &tasks {
                                missing_ids.remove(task.task_id.as_str());
                                cluster_mapping.push(task.task_id.clone());
                                self.mapping_task
                                    .insert(task.task_id.clone(), cluster.clone());
                            }
                        }
                    }
                    Err(err) => {
                        errors.push((cluster, err));
                    }
                }
            }

            if !missing_ids.is_empty() {
                let mut message = String::new();
                let mut sep = "";
                for (cluster, error) in errors {
                    let cluster_name = &cluster.name;
                    message.push_str(&format!(
                        "{sep}Error while fetching tasks from cluster {cluster_name}: {error}"
                    ));
                    sep = "\n";
                }
                return Err(Status::unavailable(message));
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

    #[armonik::reexports::tracing::instrument(skip_all)]
    pub async fn update_sessions(&self) -> Result<(), Status> {
        let streams = self.clusters.values().map(|cluster| {
            Box::pin(async_stream::stream! {
                let mut client = match cluster.client().await.map_err(IntoStatus::into_status) {
                    Ok(client) => client,
                    Err(err) => {
                        yield (cluster.clone(), Err(err));
                        return;
                    }
                };
                let span = client.span();
                let stream = match client
                    .get_all_sessions(Default::default(), Default::default())
                    .instrument(span)
                    .await
                {
                    Ok(stream) => stream,
                    Err(err) => {
                        yield (cluster.clone(), Err(err));
                        return;
                    }
                };
                let mut stream = std::pin::pin!(stream);

                while let Some(response) = stream.next().await {
                    match response {
                        Ok(response) => yield (cluster.clone(), Result::<_, Status>::Ok(response)),
                        Err(err) => {
                            yield (cluster.clone(), Err(err));
                            return;
                        }
                    }
                }
            })
        });

        let mut streams = std::pin::pin!(merge_streams(streams));

        while let Some((cluster, response)) = streams.next().await {
            match response {
                Ok(chunk) => {
                    if let Err(err) = self.add_sessions(chunk, cluster.clone()).await {
                        tracing::error!(
                            "Could not record sessions from cluster {}: {}",
                            cluster.name,
                            err
                        )
                    }
                }
                Err(err) => tracing::error!(
                    "Could not fetch sessions from cluster {}: {}",
                    cluster.name,
                    err
                ),
            }
        }

        Ok(())
    }
}
