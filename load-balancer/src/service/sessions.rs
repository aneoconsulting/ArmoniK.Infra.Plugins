use std::{collections::HashMap, sync::Arc};

use armonik::{
    reexports::{
        tonic::{self, Status},
        tracing_futures::Instrument,
    },
    server::{RequestContext, SessionsService},
    sessions,
};
use rusqlite::params_from_iter;
use serde::{Deserialize, Serialize};

use crate::utils::{impl_unary, IntoStatus};

use super::Service;

impl SessionsService for Service {
    #[allow(clippy::blocks_in_conditions)]
    async fn list(
        self: Arc<Self>,
        request: sessions::list::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::list::Response, tonic::Status> {
        let Ok(page) = usize::try_from(request.page) else {
            return Err(tonic::Status::invalid_argument("Page should be positive"));
        };
        let Ok(page_size) = usize::try_from(request.page_size) else {
            return Err(tonic::Status::invalid_argument(
                "Page size should be positive",
            ));
        };

        let build_span = tracing::trace_span!("build");
        let mut params = Vec::<Box<dyn rusqlite::ToSql + Send + Sync + 'static>>::new();
        let mut query_suffix = String::new();
        let mut sep = " WHERE (";
        let mut term = "";
        for filter in &request.filters {
            query_suffix.push_str(sep);
            sep = ") OR (";
            term = ")";
            let mut filter = filter.iter();
            if let Some(cond) = filter.next() {
                let mut cond = cond;
                let mut sep = "";
                loop {
                    query_suffix.push_str(sep);
                    sep = " AND ";
                    let column = field_to_column_name(cond.field.clone(), true)?;
                    if let sessions::Field::TaskOptionGeneric(key) = &cond.field {
                        params.push(Box::new(key.clone()));
                    }
                    match (column.value_type, &cond.condition) {
                        (ValueType::String, sessions::filter::Condition::String(_)) => (),
                        (ValueType::Number, sessions::filter::Condition::Number(_)) => (),
                        (ValueType::Boolean, sessions::filter::Condition::Boolean(_)) => (),
                        (ValueType::Status, sessions::filter::Condition::Status(_)) => (),
                        (ValueType::Date, sessions::filter::Condition::Date(_)) => (),
                        (ValueType::Duration, sessions::filter::Condition::Duration(_)) => (),
                        (ValueType::Array, sessions::filter::Condition::Array(_)) => (),
                        _ => {
                            return Err(Status::invalid_argument(format!(
                                "Condition {:?} is not valid for the field {}",
                                &cond.condition, column.grpc
                            )));
                        }
                    }

                    let thunk = match &cond.condition {
                        sessions::filter::Condition::String(cond) => {
                            params.push(Box::new(cond.value.clone()));
                            match cond.operator {
                                armonik::FilterStringOperator::Equal => format!("{column} = ?"),
                                armonik::FilterStringOperator::NotEqual => format!("{column} != ?"),
                                armonik::FilterStringOperator::Contains => {
                                    format!("instr({column}, ?) > 0")
                                }
                                armonik::FilterStringOperator::NotContains => {
                                    format!("instr({column}, ?) == 0")
                                }
                                armonik::FilterStringOperator::StartsWith => {
                                    format!("instr({column}, ?) == 1")
                                }
                                armonik::FilterStringOperator::EndsWith => {
                                    params.push(Box::new(cond.value.clone()));
                                    format!(
                                        "instr({column}, ?) + length(?) == length({column}) + 1"
                                    )
                                }
                            }
                        }
                        sessions::filter::Condition::Number(cond) => {
                            params.push(Box::new(cond.value));
                            match &cond.operator {
                                armonik::FilterNumberOperator::Equal => format!("{column} = ?"),
                                armonik::FilterNumberOperator::NotEqual => format!("{column} != ?"),
                                armonik::FilterNumberOperator::LessThan => format!("{column} < ?"),
                                armonik::FilterNumberOperator::LessThanOrEqual => {
                                    format!("{column} <= ?")
                                }
                                armonik::FilterNumberOperator::GreaterThanOrEqual => {
                                    format!("{column} >= ?")
                                }
                                armonik::FilterNumberOperator::GreaterThan => {
                                    format!("{column} > ?")
                                }
                            }
                        }
                        sessions::filter::Condition::Boolean(cond) => {
                            if cond.value {
                                column.to_string()
                            } else {
                                format!("NOT {column}")
                            }
                        }
                        sessions::filter::Condition::Status(cond) => {
                            params.push(Box::new(cond.value.clone() as i32));
                            match &cond.operator {
                                armonik::FilterStatusOperator::Equal => format!("{column} = ?"),
                                armonik::FilterStatusOperator::NotEqual => format!("{column} != ?"),
                            }
                        }
                        sessions::filter::Condition::Date(cond) => {
                            params.push(Box::new(
                                cond.value.seconds as f64 + cond.value.nanos as f64 * 1e-9f64,
                            ));
                            match &cond.operator {
                                armonik::FilterDateOperator::Equal => format!("{column} = ?"),
                                armonik::FilterDateOperator::NotEqual => format!("{column} != ?"),
                                armonik::FilterDateOperator::Before => format!("{column} < ?"),
                                armonik::FilterDateOperator::BeforeOrEqual => {
                                    format!("{column} <= ?")
                                }
                                armonik::FilterDateOperator::AfterOrEqual => {
                                    format!("{column} >= ?")
                                }
                                armonik::FilterDateOperator::After => format!("{column} > ?"),
                            }
                        }
                        sessions::filter::Condition::Duration(cond) => {
                            params.push(Box::new(
                                cond.value.seconds as f64 + cond.value.nanos as f64 * 1e-9f64,
                            ));
                            match &cond.operator {
                                armonik::FilterDurationOperator::Equal => format!("{column} = ?"),
                                armonik::FilterDurationOperator::NotEqual => {
                                    format!("{column} != ?")
                                }
                                armonik::FilterDurationOperator::ShorterThan => {
                                    format!("{column} < ?")
                                }
                                armonik::FilterDurationOperator::ShorterThanOrEqual => {
                                    format!("{column} <= ?")
                                }
                                armonik::FilterDurationOperator::LongerThanOrEqual => {
                                    format!("{column} >= ?")
                                }
                                armonik::FilterDurationOperator::LongerThan => {
                                    format!("{column} > ?")
                                }
                            }
                        }
                        sessions::filter::Condition::Array(cond) => {
                            params.push(Box::new(cond.value.clone()));
                            match &cond.operator {
                                armonik::FilterArrayOperator::Contains => format!("EXISTS (SELECT 1 FROM json_each({column}) WHERE value = ?)"),
                                armonik::FilterArrayOperator::NotContains => format!("NOT EXISTS (SELECT 1 FROM json_each({column}) WHERE value = ?)"),
                            }
                        }
                    };

                    query_suffix.push_str(&thunk);
                    let Some(c) = filter.next() else {
                        break;
                    };
                    cond = c;
                }
            } else {
                query_suffix.push_str("TRUE");
            }
        }

        query_suffix.push_str(term);

        match &request.sort {
            sessions::Sort {
                field: sessions::Field::Raw(sessions::RawField::Unspecified),
                ..
            } => (),
            sessions::Sort {
                field: sessions::Field::TaskOption(armonik::TaskOptionField::Unspecified),
                ..
            } => (),
            sessions::Sort {
                direction: armonik::SortDirection::Unspecified,
                ..
            } => (),
            _ => {
                let column = field_to_column_name(request.sort.field, false)?;
                let direction = if matches!(request.sort.direction, armonik::SortDirection::Desc) {
                    "DESC"
                } else {
                    "ASC"
                };
                query_suffix.push_str(&format!(" ORDER BY {column} {direction}"));
            }
        }

        let query = format!(
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
            ) FROM session{} LIMIT {} OFFSET {}",
            query_suffix,
            page_size,
            page * page_size
        );
        let query_count = format!("SELECT COUNT(*) FROM session{query_suffix}");
        std::mem::drop(build_span);

        let (sessions, total) = self
            .db
            .call(tracing::trace_span!("transaction"), move |conn| {
                let mut sessions = Vec::<armonik::sessions::Raw>::new();
                let transaction = conn.transaction()?;

                let count_span = tracing::trace_span!("count");
                let total =
                    transaction
                        .query_row(&query_count, params_from_iter(&params), |row| row.get(0))?;
                std::mem::drop(count_span);

                let prepare_span = tracing::trace_span!("prepare");
                let mut stmt = transaction.prepare(&query)?;
                std::mem::drop(prepare_span);

                let execute_span = tracing::trace_span!("execute");
                let mut rows = stmt.query(params_from_iter(&params))?;

                while let Some(row) = rows.next()? {
                    let json: String = row.get(0)?;
                    match serde_json::from_str(&json) {
                        Ok(session) => sessions.push(Session::into(session)),
                        Err(err) => {
                            return Err(rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            ))
                        }
                    };
                }
                std::mem::drop(execute_span);
                std::mem::drop(rows);
                std::mem::drop(stmt);

                transaction.commit()?;
                Result::<_, rusqlite::Error>::Ok((sessions, total))
            })
            .await
            .map_err(IntoStatus::into_status)?;

        Ok(armonik::sessions::list::Response {
            sessions,
            page: request.page,
            page_size: request.page_size,
            total,
        })
    }

    async fn get(
        self: Arc<Self>,
        request: sessions::get::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::get::Response, tonic::Status> {
        impl_unary!(self.sessions, request, session)
    }

    async fn cancel(
        self: Arc<Self>,
        request: sessions::cancel::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::cancel::Response, tonic::Status> {
        impl_unary!(self.sessions, request, session)
    }

    async fn create(
        self: Arc<Self>,
        request: sessions::create::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::create::Response, tonic::Status> {
        let n = self.clusters.len();
        let i = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut err = None;

        for cluster in self.clusters.values().cycle().skip(i % n).take(n) {
            match cluster.client().await {
                Ok(mut client) => {
                    let span = client.span();
                    let response = client
                        .sessions()
                        .call(request.clone())
                        .instrument(span)
                        .await;

                    match response {
                        Ok(response) => {
                            self.add_sessions(
                                vec![Session {
                                    session_id: response.session_id.clone(),
                                    cluster: cluster.name.clone(),
                                    status: armonik::SessionStatus::Running as i32 as u8,
                                    client_submission: true,
                                    worker_submission: true,
                                    partition_ids: request.partition_ids,
                                    default_task_options: request.default_task_options.into(),
                                    created_at: None,
                                    cancelled_at: None,
                                    closed_at: None,
                                    purged_at: None,
                                    deleted_at: None,
                                    duration: None,
                                }
                                .into()],
                                cluster.clone(),
                            )
                            .await?;
                            return Ok(response);
                        }
                        Err(error) => err = Some(error.into_status()),
                    }
                }
                Err(error) => err = Some(error.into_status()),
            }
        }

        match err {
            Some(err) => Err(err),
            None => Err(tonic::Status::internal("No cluster")),
        }
    }

    async fn pause(
        self: Arc<Self>,
        request: sessions::pause::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::pause::Response, tonic::Status> {
        impl_unary!(self.sessions, request, session)
    }

    async fn resume(
        self: Arc<Self>,
        request: sessions::resume::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::resume::Response, tonic::Status> {
        impl_unary!(self.sessions, request, session)
    }

    async fn close(
        self: Arc<Self>,
        request: sessions::close::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::close::Response, tonic::Status> {
        impl_unary!(self.sessions, request, session)
    }

    async fn purge(
        self: Arc<Self>,
        request: sessions::purge::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::purge::Response, tonic::Status> {
        impl_unary!(self.sessions, request, session)
    }

    async fn delete(
        self: Arc<Self>,
        request: sessions::delete::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::delete::Response, tonic::Status> {
        let service = self.clone();
        let session_id = request.session_id.clone();
        let response = impl_unary!(service.sessions, request, session)?;

        // If delete is successful, remove the session from the list
        self.db
            .execute(
                "DELETE FROM session WHERE session_id = ?",
                [session_id],
                tracing::trace_span!("delete"),
            )
            .await
            .map_err(IntoStatus::into_status)?;

        Ok(response)
    }

    async fn stop_submission(
        self: Arc<Self>,
        request: sessions::stop_submission::Request,
        _context: RequestContext,
    ) -> std::result::Result<sessions::stop_submission::Response, tonic::Status> {
        impl_unary!(self.sessions, request, session)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct TaskOptions {
    pub options: HashMap<String, String>,
    pub max_duration: f64,
    pub max_retries: i32,
    pub priority: i32,
    pub partition_id: String,
    pub application_name: String,
    pub application_version: String,
    pub application_namespace: String,
    pub application_service: String,
    pub engine_type: String,
}

fn f64_to_timestamp(t: f64) -> armonik::reexports::prost_types::Timestamp {
    armonik::reexports::prost_types::Timestamp {
        seconds: t.trunc() as i64,
        nanos: (t.fract() * 1e9) as i32,
    }
}

fn f64_to_duration(t: f64) -> armonik::reexports::prost_types::Duration {
    armonik::reexports::prost_types::Duration {
        seconds: t.trunc() as i64,
        nanos: (t.fract() * 1e9) as i32,
    }
}

fn timestamp_to_f64(t: armonik::reexports::prost_types::Timestamp) -> f64 {
    t.seconds as f64 + t.nanos as f64 * 1e-9f64
}
fn duration_to_f64(t: armonik::reexports::prost_types::Duration) -> f64 {
    t.seconds as f64 + t.nanos as f64 * 1e-9f64
}

impl From<TaskOptions> for armonik::TaskOptions {
    fn from(value: TaskOptions) -> Self {
        Self {
            options: value.options,
            max_duration: f64_to_duration(value.max_duration),
            max_retries: value.max_retries,
            priority: value.priority,
            partition_id: value.partition_id,
            application_name: value.application_name,
            application_version: value.application_version,
            application_namespace: value.application_namespace,
            application_service: value.application_service,
            engine_type: value.engine_type,
        }
    }
}

impl From<armonik::TaskOptions> for TaskOptions {
    fn from(value: armonik::TaskOptions) -> Self {
        Self {
            options: value.options,
            max_duration: duration_to_f64(value.max_duration),
            max_retries: value.max_retries,
            priority: value.priority,
            partition_id: value.partition_id,
            application_name: value.application_name,
            application_version: value.application_version,
            application_namespace: value.application_namespace,
            application_service: value.application_service,
            engine_type: value.engine_type,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Session {
    /// The session ID.
    pub session_id: String,
    /// The cluster which host the session.
    pub cluster: String,
    /// The session status.
    pub status: u8,
    /// Whether clients can submit tasks in the session.
    pub client_submission: bool,
    /// Whether workers can submit tasks in the session.
    pub worker_submission: bool,
    /// The partition IDs.
    pub partition_ids: Vec<String>,
    /// The task options. In fact, these are used as default value in child tasks.
    pub default_task_options: TaskOptions,
    /// The creation date.
    pub created_at: Option<f64>,
    /// The cancellation date. Only set when status is 'cancelled'.
    pub cancelled_at: Option<f64>,
    /// The closure date. Only set when status is 'closed'.
    pub closed_at: Option<f64>,
    /// The purge date. Only set when status is 'purged'.
    pub purged_at: Option<f64>,
    /// The deletion date. Only set when status is 'deleted'.
    pub deleted_at: Option<f64>,
    /// The duration. Only set when status is 'cancelled'.
    pub duration: Option<f64>,
}

impl From<Session> for armonik::sessions::Raw {
    fn from(value: Session) -> Self {
        Self {
            session_id: value.session_id,
            status: armonik::SessionStatus::from(value.status as i32),
            client_submission: value.client_submission,
            worker_submission: value.worker_submission,
            partition_ids: value.partition_ids,
            default_task_options: value.default_task_options.into(),
            created_at: value.created_at.map(f64_to_timestamp),
            cancelled_at: value.cancelled_at.map(f64_to_timestamp),
            closed_at: value.closed_at.map(f64_to_timestamp),
            purged_at: value.purged_at.map(f64_to_timestamp),
            deleted_at: value.deleted_at.map(f64_to_timestamp),
            duration: value.duration.map(f64_to_duration),
        }
    }
}

impl Session {
    pub fn from_grpc(raw: armonik::sessions::Raw, cluster: String) -> Self {
        Self {
            session_id: raw.session_id,
            cluster,
            status: raw.status as i32 as u8,
            client_submission: raw.client_submission,
            worker_submission: raw.worker_submission,
            partition_ids: raw.partition_ids,
            default_task_options: raw.default_task_options.into(),
            created_at: raw.created_at.map(timestamp_to_f64),
            cancelled_at: raw.cancelled_at.map(timestamp_to_f64),
            closed_at: raw.closed_at.map(timestamp_to_f64),
            purged_at: raw.purged_at.map(timestamp_to_f64),
            deleted_at: raw.deleted_at.map(timestamp_to_f64),
            duration: raw.duration.map(duration_to_f64),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ValueType {
    String,
    Number,
    Boolean,
    Status,
    Date,
    Duration,
    Array,
}

#[derive(Debug, Clone, Copy)]
struct Column {
    name: &'static str,
    grpc: &'static str,
    value_type: ValueType,
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

#[allow(clippy::result_large_err)]
fn field_to_column_name(
    field: armonik::sessions::Field,
    filter: bool,
) -> Result<Column, tonic::Status> {
    match field {
        sessions::Field::Raw(sessions::RawField::Unspecified) => {
            Err(Status::invalid_argument(if filter {
                "Filter field is not set"
            } else {
                "Sort field is not set"
            }))
        }
        sessions::Field::Raw(sessions::RawField::SessionId) => Ok(Column {
            name: "session_id",
            grpc: "SessionId",
            value_type: ValueType::String,
        }),
        sessions::Field::Raw(sessions::RawField::Status) => Ok(Column {
            name: "status",
            grpc: "Status",
            value_type: ValueType::Status,
        }),
        sessions::Field::Raw(sessions::RawField::ClientSubmission) => Ok(Column {
            name: "client_submission",
            grpc: "ClientSubmission",
            value_type: ValueType::Boolean,
        }),
        sessions::Field::Raw(sessions::RawField::WorkerSubmission) => Ok(Column {
            name: "worker_submission",
            grpc: "WorkerSubmission",
            value_type: ValueType::Boolean,
        }),
        sessions::Field::Raw(sessions::RawField::PartitionIds) => Ok(Column {
            name: "partition_ids",
            grpc: "PartitionIds",
            value_type: ValueType::Array,
        }),
        sessions::Field::Raw(sessions::RawField::Options) => {
            Err(Status::invalid_argument(if filter {
                "Filter field Options is not valid for a RawField filter"
            } else {
                "Sort field Options is not valid for a RawField sort"
            }))
        }
        sessions::Field::Raw(sessions::RawField::CreatedAt) => Ok(Column {
            name: "created_at",
            grpc: "CreatedAt",
            value_type: ValueType::Date,
        }),
        sessions::Field::Raw(sessions::RawField::CancelledAt) => Ok(Column {
            name: "cancelled_at",
            grpc: "CancelledAt",
            value_type: ValueType::Date,
        }),
        sessions::Field::Raw(sessions::RawField::ClosedAt) => Ok(Column {
            name: "closed_at",
            grpc: "ClosedAt",
            value_type: ValueType::Date,
        }),
        sessions::Field::Raw(sessions::RawField::PurgedAt) => Ok(Column {
            name: "purged_at",
            grpc: "PurgedAt",
            value_type: ValueType::Date,
        }),
        sessions::Field::Raw(sessions::RawField::DeletedAt) => Ok(Column {
            name: "deleted_at",
            grpc: "DeletedAt",
            value_type: ValueType::Date,
        }),
        sessions::Field::Raw(sessions::RawField::Duration) => Ok(Column {
            name: "duration",
            grpc: "Duration",
            value_type: ValueType::Duration,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::Unspecified) => {
            Err(Status::invalid_argument(if filter {
                "Filter field is not set"
            } else {
                "Sort field is not set"
            }))
        }
        sessions::Field::TaskOption(armonik::TaskOptionField::MaxDuration) => Ok(Column {
            name: "default_task_options ->> 'max_duration'",
            grpc: "DefaultTaskOptions.MaxDuration",
            value_type: ValueType::Duration,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::MaxRetries) => Ok(Column {
            name: "default_task_options ->> 'max_retries'",
            grpc: "DefaultTaskOptions.MaxRetries",
            value_type: ValueType::Number,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::Priority) => Ok(Column {
            name: "default_task_options ->> 'priority'",
            grpc: "DefaultTaskOptions.Priority",
            value_type: ValueType::Number,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::PartitionId) => Ok(Column {
            name: "default_task_options ->> 'partition_id'",
            grpc: "DefaultTaskOptions.PartitionId",
            value_type: ValueType::String,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::ApplicationName) => Ok(Column {
            name: "default_task_options ->> 'application_name'",
            grpc: "DefaultTaskOptions.ApplicationName",
            value_type: ValueType::String,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::ApplicationVersion) => Ok(Column {
            name: "default_task_options ->> 'application_version'",
            grpc: "DefaultTaskOptions.ApplicationVersion",
            value_type: ValueType::String,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::ApplicationNamespace) => Ok(Column {
            name: "default_task_options ->> 'application_namespace'",
            grpc: "DefaultTaskOptions.ApplicationNamespace",
            value_type: ValueType::String,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::ApplicationService) => Ok(Column {
            name: "default_task_options ->> 'application_service'",
            grpc: "DefaultTaskOptions.ApplicationService",
            value_type: ValueType::String,
        }),
        sessions::Field::TaskOption(armonik::TaskOptionField::ApplicationEngine) => Ok(Column {
            name: "default_task_options ->> 'engine_type'",
            grpc: "DefaultTaskOptions.ApplicationEngine",
            value_type: ValueType::String,
        }),
        sessions::Field::TaskOptionGeneric(_) => Ok(Column {
            name: "default_task_options -> 'options' ->> ?",
            grpc: "DefaultTaskOptions.Options",
            value_type: ValueType::String,
        }),
    }
}
