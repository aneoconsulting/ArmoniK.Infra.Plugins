// `Cluster` has interior mutability (availability flag, connection pool), but its
// Eq/Hash only depend on the immutable connection config, so it is a valid map key.
#![allow(clippy::mutable_key_type)]

use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    rc::Rc,
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

    fn connection(&self) -> &rusqlite::Connection {
        self.connection.get_or(|| {
            let connection = rusqlite::Connection::open(&self.path).unwrap_or_else(|err| {
                panic!("Could not open SQLite database {}: {err}", self.path)
            });
            connection
                .execute_batch(&self.pragmas)
                .unwrap_or_else(|err| panic!("Could not configure SQLite database: {err}"));
            // `rarray` is per-connection: it lets a whole id list be bound as one
            // parameter, see [`Service::get_cluster_from_sessions`].
            rusqlite::vtab::array::load_module(&connection)
                .unwrap_or_else(|err| panic!("Could not register the rarray module: {err}"));
            connection
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
            db.connection()
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

impl Deref for DB {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        self.connection()
    }
}

/// The local session mirror. Timestamps and durations are stored as REAL seconds, lists
/// and task options as JSON text; every filterable column is indexed.
const CREATE_SESSION_TABLE: &str = "BEGIN;
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
    CREATE INDEX IF NOT EXISTS session_created_at ON session(created_at);
    CREATE INDEX IF NOT EXISTS session_cancelled_at ON session(cancelled_at);
    CREATE INDEX IF NOT EXISTS session_closed_at ON session(closed_at);
    CREATE INDEX IF NOT EXISTS session_purged_at ON session(purged_at);
    CREATE INDEX IF NOT EXISTS session_deleted_at ON session(deleted_at);
    CREATE INDEX IF NOT EXISTS session_duration ON session(duration);
    CREATE INDEX IF NOT EXISTS session_status_created_at ON session(status, created_at);
    COMMIT;";

/// A `session` column, paired with how to read it out of a [`Session`]. Both the upsert
/// statement and the arrays bound to it are derived from this list, so the column order
/// and the parameter order cannot drift apart.
type SessionColumn = (&'static str, fn(&Session) -> rusqlite::types::Value);

const SESSION_COLUMNS: [SessionColumn; 13] = {
    use rusqlite::types::Value;

    fn text(value: &str) -> Value {
        Value::Text(String::from(value))
    }
    // Nested values keep their JSON encoding: SQLite has no composite type, and
    // `service::sessions` reads these columns back with `json()`.
    fn json<T: Serialize>(value: &T) -> Value {
        Value::Text(serde_json::to_string(value).unwrap())
    }
    // Timestamps and durations are REAL seconds, and every one of them is optional.
    fn real(value: Option<f64>) -> Value {
        value.map_or(Value::Null, Value::Real)
    }

    [
        ("session_id", |s| text(&s.session_id)),
        ("cluster", |s| text(&s.cluster)),
        ("status", |s| Value::Integer(s.status as i64)),
        ("client_submission", |s| {
            Value::Integer(s.client_submission as i64)
        }),
        ("worker_submission", |s| {
            Value::Integer(s.worker_submission as i64)
        }),
        ("partition_ids", |s| json(&s.partition_ids)),
        ("default_task_options", |s| json(&s.default_task_options)),
        ("created_at", |s| real(s.created_at)),
        ("cancelled_at", |s| real(s.cancelled_at)),
        ("closed_at", |s| real(s.closed_at)),
        ("purged_at", |s| real(s.purged_at)),
        ("deleted_at", |s| real(s.deleted_at)),
        ("duration", |s| real(s.duration)),
    ]
};

/// Bulk upsert built from [`SESSION_COLUMNS`]: one `rarray` parameter per column, all of
/// them the same length, zipped back into rows on their shared array position.
///
/// `MATERIALIZED` is load-bearing. `rarray` only answers `pointer =` constraints in its
/// `best_index`, so a `rowid` join predicate cannot be pushed into it and joining the
/// arrays directly degenerates into a 13-way cross product. Spilling each one into an
/// ephemeral table first lets SQLite build an automatic index over the positions.
static UPSERT_SESSIONS: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let names = SESSION_COLUMNS.map(|(name, _)| name);
    let driver = names[0];

    let sources = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            format!(
                "{name}_src(r, v) AS MATERIALIZED (SELECT rowid, value FROM rarray(?{}))",
                i + 1
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let values = names
        .iter()
        .map(|name| format!("{name}_src.v"))
        .collect::<Vec<_>>()
        .join(", ");
    let joins = names[1..]
        .iter()
        .map(|name| format!(" JOIN {name}_src ON {name}_src.r = {driver}_src.r"))
        .collect::<String>();

    format!(
        "WITH {sources} INSERT OR REPLACE INTO session({}) SELECT {values} FROM {driver}_src{joins}",
        names.join(", "),
    )
});

impl Service {
    pub async fn new(
        clusters: impl IntoIterator<Item = (String, Cluster)>,
        fallbacks: impl IntoIterator<Item = String>,
        options: ServiceOptions,
    ) -> Self {
        let db = DB::new(&options);
        db.execute_batch(CREATE_SESSION_TABLE, tracing::trace_span!("create_table"))
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

    /// Bulk-upsert sessions into the local mirror: the batch is transposed into one
    /// `rarray` per column and reassembled server-side, see [`UPSERT_SESSIONS`].
    pub async fn add_sessions(
        &self,
        sessions: Vec<armonik::sessions::Raw>,
        cluster: Arc<Cluster>,
    ) -> Result<(), Status> {
        let span = tracing::trace_span!("add_sessions");

        self.db
            .call(span.clone(), move |conn| {
                let prepare_span = tracing::trace_span!(parent: &span, "prepare").entered();
                let mut stmt = conn.prepare_cached(&UPSERT_SESSIONS)?;
                std::mem::drop(prepare_span);

                let _execute_span = tracing::trace_span!(parent: &span, "execute").entered();
                let sessions = sessions
                    .into_iter()
                    .map(|session| Session::from_grpc(session, cluster.name.clone()))
                    .collect::<Vec<_>>();
                // `Rc` is !Send, so the arrays are built here rather than captured.
                let columns = SESSION_COLUMNS.map(|(_, get)| -> rusqlite::vtab::array::Array {
                    Rc::new(sessions.iter().map(get).collect())
                });
                stmt.execute(rusqlite::params_from_iter(columns.iter()))?;

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
            (name_mapping, missing_ids) = self
                .db
                .call(tracing::Span::current(), move |conn| {
                    let mut name_mapping = HashMap::<String, Vec<String>>::new();

                    let prepare_span = tracing::trace_span!("prepare");
                    let mut stmt = conn.prepare_cached(
                        "SELECT session_id, cluster FROM session WHERE session_id IN rarray(?)",
                    )?;
                    std::mem::drop(prepare_span);

                    let _execute_span = tracing::trace_span!("execute");
                    // The whole id list travels as a single carray pointer: one cached
                    // statement whatever the length, and no JSON round-trip. `Rc` is !Send,
                    // so it is built here rather than captured by the closure.
                    let ids: rusqlite::vtab::array::Array = Rc::new(
                        missing_ids
                            .iter()
                            .map(|id| rusqlite::types::Value::Text(id.clone()))
                            .collect(),
                    );
                    let mut rows = stmt.query([ids])?;

                    while let Some(row) = rows.next()? {
                        let session_id: String = row.get(0)?;
                        let cluster: String = row.get(1)?;

                        missing_ids.remove(session_id.as_str());
                        match name_mapping.entry(cluster) {
                            std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                                occupied_entry.get_mut().push(session_id)
                            }
                            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(vec![session_id]);
                            }
                        }
                    }

                    Result::<_, rusqlite::Error>::Ok((name_mapping, missing_ids))
                })
                .await
                .map_err(IntoStatus::into_status)?;

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
            let connection = db.connection();

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
            .execute_batch("CREATE TABLE IF NOT EXISTS shared(a); INSERT INTO shared VALUES (1);")
            .unwrap();

        let same = DB::new(&ServiceOptions::default());
        let rows: i64 = same
            .connection()
            .query_row("SELECT count(*) FROM shared", [], |row| row.get(0))
            .unwrap();
        assert!(rows >= 1);
    }

    /// A private in-memory database. Tests that shape the `session` table differently
    /// would otherwise collide over the single `memdb` store the default configuration
    /// shares between every connection.
    fn private_options(name: &str) -> ServiceOptions {
        ServiceOptions {
            sqlite_path: Some(format!(
                "file:/test_{name}_{}?vfs=memdb",
                std::process::id()
            )),
            ..Default::default()
        }
    }

    /// A service owning a single cluster, returned alongside it so tests can feed rows
    /// through `add_sessions`. `name` names the private in-memory database.
    async fn service_with_cluster(name: &str) -> (Arc<Service>, Arc<Cluster>) {
        let cluster_name = String::from("c");
        let service = Arc::new(
            Service::new(
                [(
                    cluster_name.clone(),
                    Cluster::new(cluster_name.clone(), Default::default()),
                )],
                [],
                private_options(name),
            )
            .await,
        );
        let cluster = service.clusters[&cluster_name].clone();
        (service, cluster)
    }

    /// Every backend the configuration can select, so a change to the connection string
    /// or the pragmas is exercised on all of them rather than on the default alone.
    /// Rollback-journal modes are deliberately absent: a writer there waits for readers
    /// to drain, which under sustained contention can exhaust `busy_timeout` and fail,
    /// so asserting zero failures for them would be a flaky test rather than a true one.
    fn concurrency_configurations(dir: &std::path::Path) -> Vec<(&'static str, ServiceOptions)> {
        vec![
            ("in-memory default", private_options("concurrency")),
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
        db.connection()
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
                        let result = db.connection().execute(
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
                            let connection = db.connection();
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

    /// The upsert zips 13 independent arrays back into rows, so a column landing in the
    /// wrong slot, or one array being read at the wrong position, would corrupt the
    /// mirror silently. Every column here carries a value that could not come from any
    /// other one.
    #[test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    fn upsert_puts_every_column_in_its_own_slot() {
        let db = DB::new(&ServiceOptions {
            sqlite_path: Some(String::from("file:/armonik_load_balancer_upsert?vfs=memdb")),
            ..Default::default()
        });
        db.connection().execute_batch(CREATE_SESSION_TABLE).unwrap();

        let options = |key: &str| sessions::TaskOptions {
            options: [(String::from(key), String::from("v\"quoted"))]
                .into_iter()
                .collect(),
            max_duration: 0.25,
            max_retries: -3,
            priority: 7,
            partition_id: String::from("part"),
            application_name: String::from("app"),
            application_version: String::from("1.0"),
            application_namespace: String::from("ns"),
            application_service: String::from("svc"),
            engine_type: String::from("Unified"),
        };

        // Row 1 fills every optional, row 2 leaves them all NULL and empties the list.
        let rows = vec![
            Session {
                session_id: String::from("id-1"),
                cluster: String::from("cluster-1"),
                status: 3,
                client_submission: true,
                worker_submission: false,
                partition_ids: vec![String::from("p \"one\""), String::from("p/two")],
                default_task_options: options("k\u{e9}"),
                created_at: Some(1.0),
                cancelled_at: Some(2.5),
                closed_at: Some(3.5),
                purged_at: Some(4.5),
                deleted_at: Some(5.5),
                duration: Some(6.5),
            },
            Session {
                session_id: String::from("id-2"),
                cluster: String::from("cluster-2"),
                status: 0,
                client_submission: false,
                worker_submission: true,
                partition_ids: Vec::new(),
                default_task_options: options("other"),
                created_at: None,
                cancelled_at: None,
                closed_at: None,
                purged_at: None,
                deleted_at: None,
                duration: None,
            },
        ];

        let columns = SESSION_COLUMNS.map(|(_, get)| -> rusqlite::vtab::array::Array {
            Rc::new(rows.iter().map(get).collect())
        });
        db.connection()
            .prepare_cached(&UPSERT_SESSIONS)
            .unwrap()
            .execute(rusqlite::params_from_iter(columns.iter()))
            .unwrap();

        // Read back through the same JSON projection `sessions::list` uses.
        let read = |id: &str| -> Session {
            let json: String = db
                .connection()
                .query_row(
                    "SELECT json_object(
                        'session_id', session_id,
                        'cluster', cluster,
                        'status', status,
                        'client_submission', json(iif(client_submission, 'true', 'false')),
                        'worker_submission', json(iif(worker_submission, 'true', 'false')),
                        'partition_ids', json(partition_ids),
                        'default_task_options', json(default_task_options),
                        'created_at', created_at,
                        'cancelled_at', cancelled_at,
                        'closed_at', closed_at,
                        'purged_at', purged_at,
                        'deleted_at', deleted_at,
                        'duration', duration
                    ) FROM session WHERE session_id = ?",
                    [id],
                    |row| row.get(0),
                )
                .unwrap();
            serde_json::from_str(&json).unwrap()
        };

        for expected in &rows {
            let got = read(&expected.session_id);
            let (expected, got) = (
                serde_json::to_value(expected).unwrap(),
                serde_json::to_value(&got).unwrap(),
            );
            assert_eq!(expected, got, "round-trip changed the row");
        }

        // Storage classes have to survive too: a REAL arriving as TEXT would still
        // compare equal through JSON but would break the range filters.
        let types: Vec<String> = db
            .connection()
            .prepare("SELECT typeof(status), typeof(created_at), typeof(duration) FROM session WHERE session_id = 'id-1'")
            .unwrap()
            .query_row([], |row| Ok(vec![row.get(0)?, row.get(1)?, row.get(2)?]))
            .unwrap();
        assert_eq!(types, ["integer", "real", "real"]);

        let nulls: i64 = db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM session WHERE session_id = 'id-2'
                 AND created_at IS NULL AND duration IS NULL AND partition_ids = '[]'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(nulls, 1);
    }

    /// `rarray` is registered per connection, so a connection opened without it would
    /// only fail at runtime, on the session routing path.
    #[test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    fn rarray_is_registered_on_every_connection() {
        // Its own memdb: the default URI is process-global and `hammer` puts a table of
        // the same name in it.
        let db = DB::new(&ServiceOptions {
            sqlite_path: Some(String::from("file:/armonik_load_balancer_rarray?vfs=memdb")),
            ..Default::default()
        });
        db.connection()
            .execute_batch(
                "CREATE TABLE session(session_id TEXT PRIMARY KEY NOT NULL, cluster TEXT NOT NULL);
                 INSERT INTO session VALUES ('a', 'c1'), ('b', 'c2'), ('c', 'c1');",
            )
            .unwrap();

        let lookup = |ids: &[&str]| {
            let ids: rusqlite::vtab::array::Array = Rc::new(
                ids.iter()
                    .map(|id| rusqlite::types::Value::Text(String::from(*id)))
                    .collect(),
            );
            let mut found: Vec<(String, String)> = db
                .connection()
                .prepare_cached(
                    "SELECT session_id, cluster FROM session WHERE session_id IN rarray(?)",
                )
                .unwrap()
                .query_map([ids], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            found.sort();
            found
        };

        // The same cached statement has to serve every list length, empty included.
        assert_eq!(lookup(&[]), []);
        assert_eq!(lookup(&["b"]), [(String::from("b"), String::from("c2"))]);
        assert_eq!(
            lookup(&["a", "c", "missing"]),
            [
                (String::from("a"), String::from("c1")),
                (String::from("c"), String::from("c1")),
            ]
        );

        // A second thread gets its own connection, which must register the module too.
        let other = db.clone();
        std::thread::spawn(move || {
            other
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM session WHERE session_id IN rarray(?)",
                    [
                        Rc::new(vec![rusqlite::types::Value::Text(String::from("a"))])
                            as rusqlite::vtab::array::Array,
                    ],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
        })
        .join()
        .map(|count| assert_eq!(count, 1))
        .unwrap();
    }

    /// The `unlock_notify` feature is only useful if it actually reached the bundled
    /// SQLite, and nothing else in the build would notice if it were dropped.
    #[test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    fn unlock_notify_is_compiled_in() {
        let db = DB::new(&ServiceOptions::default());
        let options: Vec<String> = db
            .connection()
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

    /// A listing filters on one column and sorts on another, which a single-column index
    /// cannot serve at once: without a composite index SQLite materialises the whole
    /// filtered set in a temp B-tree before paginating it.
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    async fn listing_avoids_a_sort_for_the_common_filter_and_order() {
        let (service, _cluster) = service_with_cluster("plan").await;

        let plan: Vec<String> = service
            .db
            .call(tracing::Span::none(), |db| {
                db.connection()
                    .prepare(
                        "EXPLAIN QUERY PLAN SELECT session_id FROM session
                         WHERE (status = 1) ORDER BY created_at ASC LIMIT 20 OFFSET 100",
                    )
                    .unwrap()
                    .query_map([], |row| row.get(3))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap()
            })
            .await;

        let plan = plan.join(" | ");
        assert!(
            plan.contains("session_status_created_at"),
            "the composite index should serve filter and order together: {plan}"
        );
        assert!(
            !plan.contains("TEMP B-TREE"),
            "the listing should not need to sort: {plan}"
        );
    }

    /// `COUNT(*)` is unaffected by ordering, and carrying the page's ORDER BY into it
    /// costs a covering-index scan. The two queries also bind different parameter counts,
    /// so this exercises that split end to end through the real RPC.
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // SQLite is a C library, MIRI cannot call into it
    async fn listing_counts_and_pages_agree() {
        use armonik::server::SessionsService;

        let (service, cluster) = service_with_cluster("counts").await;

        service
            .add_sessions(
                (0..50)
                    .map(|i| armonik::sessions::Raw {
                        session_id: format!("s{i:04}"),
                        // half the rows carry the status the listing filters on
                        status: if i % 2 == 0 {
                            armonik::SessionStatus::Cancelled
                        } else {
                            armonik::SessionStatus::Running
                        },
                        created_at: Some(armonik::reexports::prost_types::Timestamp {
                            seconds: 1_700_000_000 + i,
                            nanos: 0,
                        }),
                        ..Default::default()
                    })
                    .collect(),
                cluster,
            )
            .await
            .expect("sessions should be stored");

        let response = service
            .clone()
            .list(
                armonik::sessions::list::Request {
                    filters: armonik::sessions::filter::Or {
                        or: vec![armonik::sessions::filter::And {
                            and: vec![armonik::sessions::filter::Field {
                                field: armonik::sessions::Field::Raw(
                                    armonik::sessions::RawField::Status,
                                ),
                                condition: armonik::sessions::filter::Condition::Status(
                                    armonik::sessions::filter::Status {
                                        value: armonik::SessionStatus::Running,
                                        operator: armonik::FilterStatusOperator::Equal,
                                    },
                                ),
                            }],
                        }],
                    },
                    sort: armonik::sessions::Sort {
                        field: armonik::sessions::Field::Raw(
                            armonik::sessions::RawField::CreatedAt,
                        ),
                        direction: armonik::SortDirection::Asc,
                    },
                    with_task_options: false,
                    page: 0,
                    page_size: 10,
                },
                Default::default(),
            )
            .await
            .expect("listing should succeed");

        // 25 of the 50 rows have status Running(1); the page is capped at 10.
        assert_eq!(response.total, 25);
        assert_eq!(response.sessions.len(), 10);
    }
}
