// `Cluster` has interior mutability (availability flag, connection pool), but its
// Eq/Hash only depend on the immutable connection config, so it is a valid map key.
#![allow(clippy::mutable_key_type)]

use std::{
    collections::{HashMap, HashSet},
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

/// SQLite `journal_mode`. Only `wal` lets the background session sync and concurrent
/// readers proceed without blocking each other; every other mode makes a writer and a
/// reader mutually exclusive. WAL needs a real file, so on an in-memory database SQLite
/// silently keeps `memory` and this setting has no effect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JournalMode {
    #[serde(alias = "DELETE")]
    Delete,
    #[serde(alias = "TRUNCATE")]
    Truncate,
    #[serde(alias = "PERSIST")]
    Persist,
    #[serde(alias = "MEMORY")]
    Memory,
    #[default]
    #[serde(alias = "WAL")]
    Wal,
    #[serde(alias = "OFF")]
    Off,
}

impl JournalMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
            Self::Persist => "PERSIST",
            Self::Memory => "MEMORY",
            Self::Wal => "WAL",
            Self::Off => "OFF",
        }
    }
}

/// SQLite `synchronous`. Only meaningful when `sqlite_path` names a file: it decides how
/// often SQLite waits for the storage to flush.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Synchronous {
    #[serde(alias = "OFF")]
    Off,
    #[default]
    #[serde(alias = "NORMAL")]
    Normal,
    #[serde(alias = "FULL")]
    Full,
    #[serde(alias = "EXTRA")]
    Extra,
}

impl Synchronous {
    fn as_str(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
            Self::Extra => "EXTRA",
        }
    }
}

/// Routing options, flattened into the top level of [`crate::LbConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceOptions {
    /// None: in-memory database; "": ./lb.sqlite; anything else: used as SQLite path/URI
    sqlite_path: Option<String>,
    /// Capacities of the id -> cluster caches
    session_cache_size: usize,
    result_cache_size: usize,
    task_cache_size: usize,
    /// Journal mode. `wal` is what keeps listings fast while the session sync writes, but
    /// it only takes effect when `sqlite_path` names a file.
    sqlite_journal_mode: JournalMode,
    /// Durability. The session table is a mirror rebuilt from the clusters on every
    /// refresh, so `off` is a reasonable choice for a disposable file.
    sqlite_synchronous: Synchronous,
    /// How long a statement waits for a lock before giving up, in milliseconds.
    sqlite_busy_timeout: u64,
    /// Page cache per connection: negative is KiB, positive is pages. There is one
    /// connection per rayon worker, so the total is roughly this times the core count.
    sqlite_cache_size: i64,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            sqlite_path: None,
            session_cache_size: 10000,
            result_cache_size: 1000000,
            task_cache_size: 1000000,
            sqlite_journal_mode: JournalMode::Wal,
            sqlite_synchronous: Synchronous::Normal,
            sqlite_busy_timeout: 5000,
            sqlite_cache_size: -2000,
        }
    }
}

/// Shared state behind every gRPC service implementation (a single `Arc<Service>` is
/// registered for all of them in `main`).
pub struct Service {
    clusters: HashMap<String, Arc<Cluster>>,
    /// Clusters receiving requests whose ids cannot be resolved anywhere
    fallbacks: HashSet<Arc<Cluster>>,
    /// Local mirror of every cluster's sessions, kept fresh by [`Service::update_sessions`]
    db: DB,
    /// id -> owning-cluster caches, first step of the resolution ladder
    mapping_session: Cache<String, Arc<Cluster>>,
    mapping_result: Cache<String, Arc<Cluster>>,
    mapping_task: Cache<String, Arc<Cluster>>,
    /// Round-robin position for session creation; only read (not incremented) when
    /// picking a fallback, so the fallback choice is stable between creations
    counter: AtomicUsize,
    /// Cached minimum data_chunk_max_size across clusters (0 = not fetched yet)
    result_preferred_size: AtomicI32,
    submitter_preferred_size: AtomicI32,
}

/// SQLite access with one lazily opened connection per thread.
#[derive(Clone)]
pub struct DB {
    connection: Arc<ThreadLocal<rusqlite::Connection>>,
    path: String,
    /// Applied to every connection as it is opened, see [`DB::new`]
    pragmas: Arc<str>,
}

impl DB {
    fn new(options: &ServiceOptions) -> Self {
        // Every connection needs to see the same database. A file does that on its own;
        // for the in-memory case the `memdb` VFS provides a named store shared by every
        // connection to the same URI. Note that shared-cache mode (`cache=shared`) would
        // also share it, but it reports contention as SQLITE_LOCKED, which no busy
        // handler ever retries, and serializes all connections behind a single mutex.
        let connection_string = match options.sqlite_path.as_deref() {
            None => "file:/armonik_load_balancer?vfs=memdb",
            Some("") => "file:./lb.sqlite",
            Some(x) => x,
        };

        // busy_timeout goes first so the statements below can wait rather than fail if
        // another connection is already holding the database.
        let pragmas = format!(
            "PRAGMA busy_timeout = {};
             PRAGMA journal_mode = {};
             PRAGMA synchronous = {};
             PRAGMA cache_size = {};",
            options.sqlite_busy_timeout,
            options.sqlite_journal_mode.as_str(),
            options.sqlite_synchronous.as_str(),
            options.sqlite_cache_size,
        );

        Self {
            connection: Default::default(),
            path: String::from(connection_string),
            pragmas: Arc::from(pragmas.as_str()),
        }
    }

    /// Not cached on failure: `get_or_try` retries the open on the next call.
    fn connection(&self) -> Result<&rusqlite::Connection, rusqlite::Error> {
        self.connection.get_or_try(|| {
            // rusqlite's message already names the path.
            let connection = rusqlite::Connection::open(&self.path)?;
            connection.execute_batch(&self.pragmas)?;
            Ok(connection)
        })
    }

    /// Truncate the write-ahead log, returning whether it actually ran to completion.
    ///
    /// A checkpoint can only reset the log while no reader is holding a snapshot, so
    /// under continuous listing traffic the file grows without bound unless it is
    /// truncated periodically. Outside WAL mode there is no log and SQLite answers
    /// `(busy: 0, log: -1, checkpointed: -1)`, so this is a successful no-op.
    pub async fn checkpoint(&self, span: tracing::Span) -> Result<bool, rusqlite::Error> {
        self.call(span, move |db| {
            // The first column is the busy flag: 1 means a reader held the log and
            // nothing was reclaimed, which still reports as a successful statement.
            db.connection()?
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok(row.get::<_, i64>(0)? == 0)
                })
        })
        .await
    }

    pub async fn execute_batch(
        &self,
        sql: &str,
        span: tracing::Span,
    ) -> Result<(), rusqlite::Error> {
        let sql = sql.to_owned();
        self.call(span, move |db| db.connection()?.execute_batch(&sql))
            .await
    }
    pub async fn execute(
        &self,
        sql: &str,
        params: impl rusqlite::Params + Send + Sync + 'static,
        span: tracing::Span,
    ) -> Result<usize, rusqlite::Error> {
        let sql = sql.to_owned();
        self.call(span, move |db| db.connection()?.execute(&sql, params))
            .await
    }

    /// Run blocking SQL on the rayon pool so it never stalls the tokio runtime. rayon
    /// also caps its threads at the core count, which bounds the number of thread-local
    /// SQLite connections (unlike `spawn_blocking` and its hundreds of threads).
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

impl Service {
    pub async fn new(
        clusters: impl IntoIterator<Item = (String, Cluster)>,
        fallbacks: impl IntoIterator<Item = String>,
        options: ServiceOptions,
    ) -> Result<Self, rusqlite::Error> {
        let db = DB::new(&options);
        // Timestamps and durations are stored as REAL seconds, lists and task options as
        // JSON text; every filterable column is indexed.
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
        .await?;
        let clusters = clusters
            .into_iter()
            .map(|(name, cluster)| (name, Arc::new(cluster)))
            .collect::<HashMap<_, _>>();
        let fallbacks = fallbacks
            .into_iter()
            .map(|cluster_name| clusters[&cluster_name].clone())
            .collect();
        Ok(Self {
            clusters,
            fallbacks,
            db,
            mapping_session: Cache::new(options.session_cache_size),
            mapping_result: Cache::new(options.result_cache_size),
            mapping_task: Cache::new(options.task_cache_size),
            counter: AtomicUsize::new(0),
            result_preferred_size: AtomicI32::new(0),
            submitter_preferred_size: AtomicI32::new(0),
        })
    }

    /// Bulk-upsert sessions into the local mirror: the whole batch is passed as a single
    /// JSON array parameter and exploded server-side with `json_each`.
    pub async fn add_sessions(
        &self,
        sessions: Vec<armonik::sessions::Raw>,
        cluster: Arc<Cluster>,
    ) -> Result<(), Status> {
        let span = tracing::trace_span!("add_sessions");

        self.db
            .call(span.clone(), move |conn| {
                let prepare_span = tracing::trace_span!(parent: &span, "prepare").entered();
                let mut stmt = conn.connection()?.prepare_cached(
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

    /// Resolve the owning cluster of each session id, trying in order: the in-memory
    /// cache, the SQLite mirror, a live fan-out `list` on every cluster, then a fallback
    /// cluster for ids still unknown. Fails only if some ids remain unresolved and no
    /// fallback is configured.
    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_sessions(
        &self,
        session_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        // Fast path: a single cluster that is also the fallback gets everything.
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

        // Cache misses: look the ids up in the SQLite mirror.
        if !missing_ids.is_empty() {
            let name_mapping;
            (name_mapping, missing_ids) = self.db.call(tracing::Span::current(), move |conn| {
                let mut name_mapping = HashMap::<String, Vec<String>>::new();

                let prepare_span = tracing::trace_span!("prepare");
                let mut stmt = conn.connection()?.prepare_cached("SELECT session_id, cluster FROM session WHERE session_id IN (SELECT e.value FROM json_each(?) e)")?;
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

        // Still unknown: fan out an exact-match list to every cluster and record the hits.
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
                    let mut client = match cluster.client(&Default::default()).await {
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

            // Ids found nowhere: route them to a fallback, or fail if none is configured.
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

                // Deliberate load (not fetch_add): the fallback pick only changes when a
                // session creation advances the counter, keeping it stable in between.
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

    /// Same resolution ladder as [`Service::get_cluster_from_sessions`], but results
    /// have no SQLite mirror: cache, then fan-out `list` (caching hits), then fallback.
    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_results(
        &self,
        result_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        // Fast path: a single cluster that is also the fallback gets everything.
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

        // Still unknown: fan out an exact-match list to every cluster and record the hits.
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
                    let mut client = match cluster.client(&Default::default()).await {
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

            // Ids found nowhere: route them to a fallback, or fail if none is configured.
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

                // Deliberate load (not fetch_add): the fallback pick only changes when a
                // session creation advances the counter, keeping it stable in between.
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

    /// Same resolution ladder as [`Service::get_cluster_from_sessions`], but tasks have
    /// no SQLite mirror: cache, then fan-out `list` (caching hits), then fallback.
    #[armonik::reexports::tracing::instrument(level = armonik::reexports::tracing::Level::TRACE, skip_all)]
    pub async fn get_cluster_from_tasks(
        &self,
        task_ids: &[&str],
    ) -> Result<HashMap<Arc<Cluster>, Vec<String>>, Status> {
        // Fast path: a single cluster that is also the fallback gets everything.
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

        // Still unknown: fan out an exact-match list to every cluster and record the hits.
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
                    let mut client = match cluster.client(&Default::default()).await {
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

            // Ids found nowhere: route them to a fallback, or fail if none is configured.
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

                // Deliberate load (not fetch_add): the fallback pick only changes when a
                // session creation advances the counter, keeping it stable in between.
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

    /// Background tick: stream every cluster's full session list into the SQLite mirror.
    /// Sole writer of the availability flags: any failure marks the cluster unavailable,
    /// and only a complete, error-free pass marks it available again.
    #[armonik::reexports::tracing::instrument(skip_all)]
    pub async fn update_sessions(&self) -> Result<(), Status> {
        let streams = self.clusters.values().map(|cluster| {
            Box::pin(async_stream::stream! {
                let mut client = match cluster.client(&Default::default()).await.map_err(IntoStatus::into_status) {
                    Ok(client) => client,
                    Err(err) => {
                        cluster.set_available(false);
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
                        cluster.set_available(false);
                        yield (cluster.clone(), Err(err));
                        return;
                    }
                };
                let mut stream = std::pin::pin!(stream);

                while let Some(response) = stream.next().await {
                    match response {
                        Ok(response) => yield (cluster.clone(), Result::<_, Status>::Ok(response)),
                        Err(err) => {
                            cluster.set_available(false);
                            yield (cluster.clone(), Err(err));
                            return;
                        }
                    }
                }
                // Recovery requires the full stream to complete without error.
                // A partial or flaky stream leaves the cluster marked unavailable
                // until the next successful update_sessions run.
                cluster.set_available(true);
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

        // The sync is the only bulk writer, so this is the natural place to reclaim the
        // WAL it just produced.
        match self.db.checkpoint(tracing::trace_span!("checkpoint")).await {
            Ok(true) => {}
            Ok(false) => tracing::debug!(
                "Session database checkpoint was blocked by an open read, \
                 the write-ahead log will be reclaimed on a later refresh"
            ),
            Err(err) => tracing::warn!("Could not checkpoint the session database: {}", err),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options_from_yaml(yaml: &str) -> ServiceOptions {
        // Mirrors how `main` builds the configuration, so this covers the `serde(flatten)`
        // of ServiceOptions into LbConfig as well as the field names themselves.
        let conf: crate::LbConfig = config::Config::builder()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        conf.service_options
    }

    #[test]
    fn sqlite_defaults() {
        let options = ServiceOptions::default();
        assert_eq!(options.sqlite_journal_mode, JournalMode::Wal);
        assert_eq!(options.sqlite_synchronous, Synchronous::Normal);
        assert_eq!(options.sqlite_busy_timeout, 5000);
        assert_eq!(options.sqlite_cache_size, -2000);
        assert_eq!(options_from_yaml("clusters: {}"), options);
    }

    #[test]
    fn sqlite_options_are_configurable() {
        let options = options_from_yaml(concat!(
            "clusters: {}\n",
            "sqlite_path: /dev/shm/lb.sqlite\n",
            "sqlite_journal_mode: wal\n",
            "sqlite_synchronous: \"off\"\n",
            "sqlite_busy_timeout: 15000\n",
            "sqlite_cache_size: -8000\n",
        ));
        assert_eq!(options.sqlite_path.as_deref(), Some("/dev/shm/lb.sqlite"));
        assert_eq!(options.sqlite_journal_mode, JournalMode::Wal);
        assert_eq!(options.sqlite_synchronous, Synchronous::Off);
        assert_eq!(options.sqlite_busy_timeout, 15000);
        assert_eq!(options.sqlite_cache_size, -8000);
    }

    #[test]
    fn pragma_names_are_accepted_in_either_case() {
        let lower = options_from_yaml(
            "clusters: {}\nsqlite_journal_mode: delete\nsqlite_synchronous: full\n",
        );
        let upper = options_from_yaml(
            "clusters: {}\nsqlite_journal_mode: DELETE\nsqlite_synchronous: FULL\n",
        );
        assert_eq!(lower.sqlite_journal_mode, JournalMode::Delete);
        assert_eq!(lower.sqlite_synchronous, Synchronous::Full);
        assert_eq!(lower, upper);
    }

    /// The pragmas must actually reach the connection, and must be harmless on the
    /// in-memory default where WAL is not available.
    #[test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    fn pragmas_are_applied_to_every_connection() {
        let dir = std::env::temp_dir().join(format!("lb_pragma_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("lb.sqlite");

        for (path, expected_journal_mode) in [
            (None, "memory"),
            (Some(format!("file:{}", file.display())), "wal"),
        ] {
            let options = ServiceOptions {
                sqlite_path: path,
                sqlite_journal_mode: JournalMode::Wal,
                sqlite_synchronous: Synchronous::Off,
                sqlite_busy_timeout: 15000,
                sqlite_cache_size: -8000,
                ..Default::default()
            };
            let db = DB::new(&options);
            let connection = db.connection().unwrap();

            let journal_mode: String = connection
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap();
            let busy_timeout: i64 = connection
                .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
                .unwrap();
            let cache_size: i64 = connection
                .query_row("PRAGMA cache_size", [], |row| row.get(0))
                .unwrap();
            let synchronous: i64 = connection
                .query_row("PRAGMA synchronous", [], |row| row.get(0))
                .unwrap();

            assert_eq!(journal_mode, expected_journal_mode);
            assert_eq!(busy_timeout, 15000);
            assert_eq!(cache_size, -8000);
            assert_eq!(synchronous, 0);
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Two connections to the default in-memory database must see the same rows: the
    /// `memdb` VFS shares one store between them, a plain `:memory:` would not.
    #[test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    fn in_memory_database_is_shared_between_connections() {
        let db = DB::new(&ServiceOptions::default());
        db.connection()
            .unwrap()
            .execute_batch("CREATE TABLE IF NOT EXISTS shared(a); INSERT INTO shared VALUES (1);")
            .unwrap();

        let same = DB::new(&ServiceOptions::default());
        let rows: i64 = same
            .connection()
            .unwrap()
            .query_row("SELECT count(*) FROM shared", [], |row| row.get(0))
            .unwrap();
        assert!(rows >= 1);
    }

    /// Every backend the configuration can select, so a change to the connection string
    /// or the pragmas is exercised on all of them rather than on the default alone.
    /// Rollback-journal modes are deliberately absent: a writer there waits for readers
    /// to drain, which under sustained contention can exhaust `busy_timeout` and fail,
    /// so asserting zero failures for them would be a flaky test rather than a true one.
    fn concurrency_configurations(dir: &std::path::Path) -> Vec<(&'static str, ServiceOptions)> {
        vec![
            ("in-memory default", ServiceOptions::default()),
            (
                "file, WAL, synchronous=off",
                ServiceOptions {
                    sqlite_path: Some(format!("file:{}", dir.join("off.sqlite").display())),
                    sqlite_synchronous: Synchronous::Off,
                    ..Default::default()
                },
            ),
            (
                "file, WAL, synchronous=normal",
                ServiceOptions {
                    sqlite_path: Some(format!("file:{}", dir.join("normal.sqlite").display())),
                    sqlite_synchronous: Synchronous::Normal,
                    ..Default::default()
                },
            ),
            (
                "file, WAL, small cache and timeout",
                ServiceOptions {
                    sqlite_path: Some(format!("file:{}", dir.join("small.sqlite").display())),
                    sqlite_cache_size: -64,
                    sqlite_busy_timeout: 1000,
                    ..Default::default()
                },
            ),
        ]
    }

    fn scratch_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lb_{name}_{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Runs writers and `list_sessions`-shaped readers against one configuration and
    /// returns whatever failed.
    fn hammer(options: &ServiceOptions, duration: std::time::Duration) -> Vec<String> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Mutex;

        let db = DB::new(options);
        db.connection().unwrap()
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS session(
                    session_id TEXT PRIMARY KEY NOT NULL, cluster TEXT NOT NULL, status TINYINT NOT NULL);
                 CREATE INDEX IF NOT EXISTS conc_status ON session(status);",
            )
            .unwrap();

        let stop = AtomicBool::new(false);
        let failures = Mutex::new(Vec::<String>::new());

        std::thread::scope(|scope| {
            for writer in 0..2 {
                let (db, stop, failures) = (db.clone(), &stop, &failures);
                scope.spawn(move || {
                    let mut id = writer * 1_000_000;
                    while !stop.load(Ordering::Relaxed) {
                        let result = db.connection().unwrap().execute(
                            "INSERT OR REPLACE INTO session VALUES (?, 'cluster', 1)",
                            [format!("session-{id}")],
                        );
                        if let Err(err) = result {
                            failures.lock().unwrap().push(format!("write: {err}"));
                        }
                        id += 1;
                    }
                });
            }
            for _ in 0..4 {
                let (db, stop, failures) = (db.clone(), &stop, &failures);
                scope.spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        // The list_sessions shape: a count and a page in one transaction.
                        let result = (|| -> Result<(), rusqlite::Error> {
                            let connection = db.connection().unwrap();
                            let transaction = connection.unchecked_transaction()?;
                            let _: i64 = transaction.query_row(
                                "SELECT count(*) FROM session WHERE status = 1",
                                [],
                                |row| row.get(0),
                            )?;
                            transaction
                                .prepare_cached(
                                    "SELECT session_id FROM session WHERE status = 1 LIMIT 20",
                                )?
                                .query_map([], |row| row.get::<_, String>(0))?
                                .collect::<Result<Vec<_>, _>>()?;
                            transaction.commit()
                        })();
                        if let Err(err) = result {
                            failures.lock().unwrap().push(format!("read: {err}"));
                        }
                    }
                });
            }

            std::thread::sleep(duration);
            stop.store(true, Ordering::Relaxed);
        });

        failures.into_inner().unwrap()
    }

    /// The reported bug: the session refresh and concurrent readers used to fail each
    /// other outright, because shared-cache mode reports contention as SQLITE_LOCKED and
    /// no busy handler ever retries it. Drives the real statements through the real
    /// per-thread connections and requires that nothing fails, on every backend.
    #[test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    fn concurrent_readers_and_writers_do_not_fail() {
        let dir = scratch_dir("concurrency");

        for (label, options) in concurrency_configurations(&dir) {
            let failures = hammer(&options, std::time::Duration::from_millis(700));
            assert!(
                failures.is_empty(),
                "{label}: {} operations failed, first few: {:?}",
                failures.len(),
                &failures[..failures.len().min(5)]
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The `unlock_notify` feature is only useful if it actually reached the bundled
    /// SQLite, and nothing else in the build would notice if it were dropped.
    #[test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    fn unlock_notify_is_compiled_in() {
        let db = DB::new(&ServiceOptions::default());
        let options: Vec<String> = db
            .connection()
            .unwrap()
            .prepare("PRAGMA compile_options")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            options.iter().any(|o| o == "ENABLE_UNLOCK_NOTIFY"),
            "SQLite was built without unlock_notify: {options:?}"
        );
    }

    /// `update_sessions` checkpoints on every pass, so this has to succeed on backends
    /// that have no write-ahead log at all, otherwise every refresh would log a warning.
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    async fn checkpoint_succeeds_in_every_journal_mode() {
        let dir = scratch_dir("checkpoint");

        for mode in [
            JournalMode::Wal,
            JournalMode::Delete,
            JournalMode::Truncate,
            JournalMode::Persist,
            JournalMode::Memory,
            JournalMode::Off,
        ] {
            for (backend, path) in [
                ("in-memory", None),
                (
                    "file",
                    Some(format!(
                        "file:{}",
                        dir.join(format!("{}.sqlite", mode.as_str())).display()
                    )),
                ),
            ] {
                let db = DB::new(&ServiceOptions {
                    sqlite_path: path,
                    sqlite_journal_mode: mode,
                    ..Default::default()
                });
                db.connection()
                    .unwrap()
                    .execute_batch("CREATE TABLE IF NOT EXISTS t(a); INSERT INTO t VALUES (1);")
                    .unwrap();

                let checkpointed = db.checkpoint(tracing::Span::none()).await;
                assert!(
                    matches!(checkpointed, Ok(true)),
                    "{backend}, journal_mode={}: {checkpointed:?}",
                    mode.as_str()
                );
            }
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A failed open must surface as an error, not a panic on a rayon worker.
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    async fn a_database_that_cannot_be_opened_reports_instead_of_panicking() {
        let dir = std::env::temp_dir().join(format!("lb_openfail_{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        let options = ServiceOptions {
            sqlite_path: Some(format!("file:{}", dir.join("lb.sqlite").display())),
            ..Default::default()
        };

        // startup
        let err = Service::new([], [], options.clone())
            .await
            .map(|_| ()) // Service: !Debug
            .expect_err("a database under a missing directory should not open");
        assert_eq!(
            err.sqlite_error_code(),
            Some(rusqlite::ErrorCode::CannotOpen)
        );

        // request path
        let db = DB::new(&options);
        let err = db
            .execute_batch("SELECT 1;", tracing::Span::none())
            .await
            .expect_err("the failure should surface through the query helpers");
        assert_eq!(
            err.sqlite_error_code(),
            Some(rusqlite::ErrorCode::CannotOpen)
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
