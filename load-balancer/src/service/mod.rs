#![allow(clippy::mutable_key_type)]

use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::{
        atomic::{AtomicI32, AtomicUsize},
        Arc,
    },
};

use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};
use sessions::Session;

use armonik::reexports::{tokio_stream::StreamExt, tonic::Status, tracing_futures::Instrument};
use thread_local::ThreadLocal;

use crate::{
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceOptions {
    sqlite_path: Option<String>,
    session_cache_size: usize,
    result_cache_size: usize,
    task_cache_size: usize,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            sqlite_path: None,
            session_cache_size: 10000,
            result_cache_size: 1000000,
            task_cache_size: 1000000,
        }
    }
}

pub struct Service {
    clusters: HashMap<String, Arc<Cluster>>,
    fallbacks: HashSet<Arc<Cluster>>,
    db: DB,
    mapping_session: Cache<String, Arc<Cluster>>,
    mapping_result: Cache<String, Arc<Cluster>>,
    mapping_task: Cache<String, Arc<Cluster>>,
    counter: AtomicUsize,
    result_preferred_size: AtomicI32,
    submitter_preferred_size: AtomicI32,
}

#[derive(Clone)]
pub struct DB {
    connection: Arc<ThreadLocal<rusqlite::Connection>>,
    path: String,
}

impl DB {
    fn new(path: Option<&str>) -> Self {
        let connection_string = match path {
            None => "file::memory:?cache=shared&psow=1",
            Some("") => "file:./lb.sqlite?cache=shared",
            Some(x) => x,
        };

        Self {
            connection: Default::default(),
            path: String::from(connection_string),
        }
    }

    fn connection(&self) -> &rusqlite::Connection {
        self.connection
            .get_or(|| rusqlite::Connection::open(&self.path).unwrap())
    }

    pub async fn execute_batch(
        &self,
        sql: &str,
        span: tracing::Span,
    ) -> Result<(), rusqlite::Error> {
        let sql = sql.to_owned();
        self.call(span, move |db| db.connection().execute_batch(&sql))
            .await
    }
    pub async fn execute(
        &self,
        sql: &str,
        params: impl rusqlite::Params + Send + Sync + 'static,
        span: tracing::Span,
    ) -> Result<usize, rusqlite::Error> {
        let sql = sql.to_owned();
        self.call(span, move |db| db.connection().execute(&sql, params))
            .await
    }

    pub async fn call<Out, F>(&self, span: tracing::Span, f: F) -> Out
    where
        Out: Send + 'static,
        F: FnOnce(&DB) -> Out,
        F: Send + Sync + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let this: DB = self.clone();

        rayon::spawn(move || {
            let _entered = span.entered();
            _ = tx.send(f(&this));
        });

        rx.await.unwrap()
    }
}

impl Deref for DB {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        self.connection()
    }
}

impl Service {
    pub async fn new(
        clusters: impl IntoIterator<Item = (String, Cluster)>,
        fallbacks: impl IntoIterator<Item = String>,
        options: ServiceOptions,
    ) -> Self {
        let sqlite_path = options.sqlite_path;
        let db = DB::new(sqlite_path.as_deref());
        db.execute_batch(
            "BEGIN;
            CREATE TABLE IF NOT EXISTS session(
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
            CREATE INDEX IF NOT EXISTS session_status ON session(status);
            CREATE INDEX IF NOT EXISTS session_client_submission ON session(client_submission);
            CREATE INDEX IF NOT EXISTS session_worker_submission ON session(worker_submission);
            CREATE INDEX IF NOT EXISTS session_created_at ON session(created_at);
            CREATE INDEX IF NOT EXISTS session_cancelled_at ON session(cancelled_at);
            CREATE INDEX IF NOT EXISTS session_closed_at ON session(closed_at);
            CREATE INDEX IF NOT EXISTS session_purged_at ON session(purged_at);
            CREATE INDEX IF NOT EXISTS session_deleted_at ON session(deleted_at);
            CREATE INDEX IF NOT EXISTS session_duration ON session(duration);
            COMMIT;",
            tracing::trace_span!("create_table"),
        )
        .await
        .unwrap();
        let clusters = clusters
            .into_iter()
            .map(|(name, cluster)| (name, Arc::new(cluster)))
            .collect::<HashMap<_, _>>();
        let fallbacks = fallbacks
            .into_iter()
            .map(|cluster_name| clusters[&cluster_name].clone())
            .collect();
        Self {
            clusters,
            fallbacks,
            db,
            mapping_session: Cache::new(options.session_cache_size),
            mapping_result: Cache::new(options.result_cache_size),
            mapping_task: Cache::new(options.task_cache_size),
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
        if self.clusters.len() == 1 && self.fallbacks.len() == 1 {
            let cluster = self.fallbacks.iter().next().unwrap().clone();

            return Ok([(
                cluster,
                session_ids.iter().copied().map(String::from).collect(),
            )]
            .into_iter()
            .collect());
        }

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
                if self.fallbacks.is_empty() {
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

                let cluster = self
                    .fallbacks
                    .iter()
                    .nth(
                        self.counter.load(std::sync::atomic::Ordering::Relaxed)
                            % self.fallbacks.len(),
                    )
                    .unwrap()
                    .clone();
                let entry = mapping.entry(cluster.clone()).or_default();
                for session_id in missing_ids {
                    entry.push(session_id);
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

    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_results(
        &self,
        result_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        if self.clusters.len() == 1 && self.fallbacks.len() == 1 {
            let cluster = self.fallbacks.iter().next().unwrap().clone();

            return Ok([(
                cluster,
                result_ids.iter().copied().map(String::from).collect(),
            )]
            .into_iter()
            .collect());
        }

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
                if self.fallbacks.is_empty() {
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

                let cluster = self
                    .fallbacks
                    .iter()
                    .nth(
                        self.counter.load(std::sync::atomic::Ordering::Relaxed)
                            % self.fallbacks.len(),
                    )
                    .unwrap()
                    .clone();
                let entry = mapping.entry(cluster.clone()).or_default();
                for result_id in missing_ids {
                    entry.push(String::from(result_id));
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

    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_tasks(
        &self,
        task_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        if self.clusters.len() == 1 && self.fallbacks.len() == 1 {
            let cluster = self.fallbacks.iter().next().unwrap().clone();

            return Ok([(
                cluster,
                task_ids.iter().copied().map(String::from).collect(),
            )]
            .into_iter()
            .collect());
        }

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
                if self.fallbacks.is_empty() {
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

                let cluster = self
                    .fallbacks
                    .iter()
                    .nth(
                        self.counter.load(std::sync::atomic::Ordering::Relaxed)
                            % self.fallbacks.len(),
                    )
                    .unwrap()
                    .clone();
                let entry = mapping.entry(cluster.clone()).or_default();
                for task_id in missing_ids {
                    entry.push(String::from(task_id));
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
