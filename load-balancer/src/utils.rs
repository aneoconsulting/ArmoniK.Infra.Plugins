use std::cmp::Ordering;

use armonik::reexports::{prost_types, tonic::Status};

macro_rules! run_with_cancellation {
    (use $ct:expr; $($body:tt)*) => {
        crate::utils::run_with_cancellation!($ct, async move { $($body)* })
    };
    ($ct:expr, $fut:expr) => {
        match $ct.run_until_cancelled($fut).await {
            Some(res) => res,
            None => {
                Err(tonic::Status::aborted("Cancellation token has been triggered"))?
            }
        }
    }
}
pub(crate) use run_with_cancellation;

macro_rules! impl_unary {
    ($self:ident.$service:ident, $request:ident, $ct:ident, session) => {
        crate::utils::impl_unary!($self.$service, $request, $ct, {get_cluster_from_session, session_id, "Session {} was not found"})
    };
    ($self:ident.$service:ident, $request:ident, $ct:ident, result) => {
        crate::utils::impl_unary!($self.$service, $request, $ct, {get_cluster_from_result, result_id, "Result {} was not found"})
    };
    ($self:ident.$service:ident, $request:ident, $ct:ident, task) => {
        crate::utils::impl_unary!($self.$service, $request, $ct, {get_cluster_from_task, task_id, "Task {} was not found"})
    };

    ($self:ident.$service:ident, $request:ident, $ct:ident, {$get_cluster:ident, $id:ident, $msg:literal}) => {
        crate::utils::run_with_cancellation! {
            use $ct;

            let Some(cluster) = $self.$get_cluster(&$request.$id).await? else {
                return Err(tonic::Status::not_found(format!(
                    $msg,
                    $request.$id
                )));
            };

            cluster
                .client()
                .await
                .map_err(crate::utils::IntoStatus::into_status)?
                .$service()
                .call($request)
                .await
                .map_err(crate::utils::IntoStatus::into_status)
        }
    };
}
pub(crate) use impl_unary;

pub trait IntoStatus {
    fn into_status(self) -> Status;
}

impl IntoStatus for armonik::client::RequestError {
    fn into_status(self) -> Status {
        match self {
            armonik::client::RequestError::Grpc { source, .. } => *source,
            err => Status::internal(err.to_string()),
        }
    }
}

impl IntoStatus for armonik::client::ConnectionError {
    fn into_status(self) -> Status {
        Status::unavailable(self.to_string())
    }
}

impl IntoStatus for armonik::client::ConfigError {
    fn into_status(self) -> Status {
        Status::internal(self.to_string())
    }
}

impl IntoStatus for armonik::client::ReadEnvError {
    fn into_status(self) -> Status {
        Status::internal(self.to_string())
    }
}

pub(crate) fn filter_match_string(value: &str, condition: &armonik::FilterString) -> bool {
    match condition.operator {
        armonik::FilterStringOperator::Equal => condition.value == value,
        armonik::FilterStringOperator::NotEqual => condition.value != value,
        armonik::FilterStringOperator::Contains => condition.value.contains(value),
        armonik::FilterStringOperator::NotContains => !condition.value.contains(value),
        armonik::FilterStringOperator::StartsWith => condition.value.starts_with(value),
        armonik::FilterStringOperator::EndsWith => condition.value.ends_with(value),
    }
}

pub(crate) fn filter_match_number(value: i64, condition: &armonik::FilterNumber) -> bool {
    match condition.operator {
        armonik::FilterNumberOperator::Equal => value == condition.value,
        armonik::FilterNumberOperator::NotEqual => value != condition.value,
        armonik::FilterNumberOperator::LessThan => value < condition.value,
        armonik::FilterNumberOperator::LessThanOrEqual => value <= condition.value,
        armonik::FilterNumberOperator::GreaterThanOrEqual => value >= condition.value,
        armonik::FilterNumberOperator::GreaterThan => value > condition.value,
    }
}

pub(crate) fn filter_match_bool(value: bool, condition: &armonik::FilterBoolean) -> bool {
    match condition.operator {
        armonik::FilterBooleanOperator::Is => value == condition.value,
    }
}

pub(crate) fn filter_match_array(
    value: impl IntoIterator<Item = impl AsRef<str>>,
    condition: &armonik::FilterArray,
) -> bool {
    let contains = value.into_iter().any(|s| s.as_ref() == condition.value);
    match condition.operator {
        armonik::FilterArrayOperator::Contains => contains,
        armonik::FilterArrayOperator::NotContains => !contains,
    }
}

pub(crate) fn filter_match_status<T: PartialEq>(
    value: &T,
    condition: &armonik::FilterStatus<T>,
) -> bool {
    match condition.operator {
        armonik::FilterStatusOperator::Equal => *value == condition.value,
        armonik::FilterStatusOperator::NotEqual => *value != condition.value,
    }
}

pub(crate) fn filter_match_duration(
    value: Option<prost_types::Duration>,
    condition: &armonik::FilterDuration,
) -> bool {
    let Some(value) = value else {
        return matches!(
            condition.operator,
            armonik::FilterDurationOperator::NotEqual
        );
    };
    let prost_types::Duration { seconds, nanos } = value;
    let lhs = (seconds, nanos);

    let prost_types::Duration { seconds, nanos } = condition.value;
    let rhs = (seconds, nanos);

    match condition.operator {
        armonik::FilterDurationOperator::Equal => lhs == rhs,
        armonik::FilterDurationOperator::NotEqual => lhs != rhs,
        armonik::FilterDurationOperator::ShorterThan => lhs < rhs,
        armonik::FilterDurationOperator::ShorterThanOrEqual => lhs <= rhs,
        armonik::FilterDurationOperator::LongerThanOrEqual => lhs >= rhs,
        armonik::FilterDurationOperator::LongerThan => lhs > rhs,
    }
}

pub(crate) fn filter_match_date(
    value: Option<prost_types::Timestamp>,
    condition: &armonik::FilterDate,
) -> bool {
    let Some(value) = value else {
        return matches!(condition.operator, armonik::FilterDateOperator::NotEqual);
    };

    let prost_types::Timestamp { seconds, nanos } = value;
    let lhs = (seconds, nanos);

    let prost_types::Timestamp { seconds, nanos } = condition.value;
    let rhs = (seconds, nanos);

    match condition.operator {
        armonik::FilterDateOperator::Equal => lhs == rhs,
        armonik::FilterDateOperator::NotEqual => lhs != rhs,
        armonik::FilterDateOperator::Before => lhs < rhs,
        armonik::FilterDateOperator::BeforeOrEqual => lhs <= rhs,
        armonik::FilterDateOperator::AfterOrEqual => lhs >= rhs,
        armonik::FilterDateOperator::After => lhs > rhs,
    }
}

pub(crate) fn filter_match_session(
    value: &armonik::sessions::Raw,
    condition: &armonik::sessions::filter::Field,
) -> Result<bool, Status> {
    match &condition.field {
        armonik::sessions::Field::Raw(raw_field) => match raw_field {
            armonik::sessions::RawField::Unspecified => {
                Err(Status::invalid_argument("Filter field is not set"))
            }
            armonik::sessions::RawField::SessionId => match &condition.condition {
                armonik::sessions::filter::Condition::String(filter_string) => {
                    Ok(filter_match_string(&value.session_id, filter_string))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field SessionId"
                ))),
            },
            armonik::sessions::RawField::Status => match &condition.condition {
                armonik::sessions::filter::Condition::Status(filter_status) => {
                    Ok(filter_match_status(&value.status, filter_status))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field Status"
                ))),
            },
            armonik::sessions::RawField::ClientSubmission => match &condition.condition {
                armonik::sessions::filter::Condition::Boolean(filter_bool) => {
                    Ok(filter_match_bool(value.client_submission, filter_bool))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field ClientSubmission"
                ))),
            },
            armonik::sessions::RawField::WorkerSubmission => match &condition.condition {
                armonik::sessions::filter::Condition::Boolean(filter_bool) => {
                    Ok(filter_match_bool(value.worker_submission, filter_bool))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field WorkerSubmission"
                ))),
            },
            armonik::sessions::RawField::PartitionIds => match &condition.condition {
                armonik::sessions::filter::Condition::Array(filter_array) => {
                    Ok(filter_match_array(&value.partition_ids, filter_array))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field PartitionIds"
                ))),
            },
            armonik::sessions::RawField::Options => Err(Status::invalid_argument(
                "Filter field Options is not valid for a RawField filter",
            )),
            armonik::sessions::RawField::CreatedAt => match &condition.condition {
                armonik::sessions::filter::Condition::Date(filter_date) => {
                    Ok(filter_match_date(value.created_at, filter_date))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field CreatedAt"
                ))),
            },
            armonik::sessions::RawField::CancelledAt => match &condition.condition {
                armonik::sessions::filter::Condition::Date(filter_date) => {
                    Ok(filter_match_date(value.cancelled_at, filter_date))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field CancelledAt"
                ))),
            },
            armonik::sessions::RawField::ClosedAt => match &condition.condition {
                armonik::sessions::filter::Condition::Date(filter_date) => {
                    Ok(filter_match_date(value.closed_at, filter_date))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field ClosedAt"
                ))),
            },
            armonik::sessions::RawField::PurgedAt => match &condition.condition {
                armonik::sessions::filter::Condition::Date(filter_date) => {
                    Ok(filter_match_date(value.purged_at, filter_date))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field PurgedAt"
                ))),
            },
            armonik::sessions::RawField::DeletedAt => match &condition.condition {
                armonik::sessions::filter::Condition::Date(filter_date) => {
                    Ok(filter_match_date(value.deleted_at, filter_date))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DeletedAt"
                ))),
            },
            armonik::sessions::RawField::Duration => match &condition.condition {
                armonik::sessions::filter::Condition::Duration(filter_duration) => {
                    Ok(filter_match_duration(value.duration, filter_duration))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field Duration"
                ))),
            },
        },
        armonik::sessions::Field::TaskOption(task_option_field) => match task_option_field {
            armonik::TaskOptionField::Unspecified => {
                Err(Status::invalid_argument("Filter field is not set"))
            }
            armonik::TaskOptionField::MaxDuration => match &condition.condition {
                armonik::sessions::filter::Condition::Duration(filter_duration) => {
                    Ok(filter_match_duration(Some(value.default_task_options.max_duration), filter_duration))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.MaxDuration"
                ))),
            },
            armonik::TaskOptionField::MaxRetries => match &condition.condition {
                armonik::sessions::filter::Condition::Number(filter_number) => {
                    Ok(filter_match_number(value.default_task_options.max_retries as i64, filter_number))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.MaxRetries"
                ))),
            },
            armonik::TaskOptionField::Priority => match &condition.condition {
                armonik::sessions::filter::Condition::Number(filter_number) => {
                    Ok(filter_match_number(value.default_task_options.priority as i64, filter_number))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.Priority"
                ))),
            },
            armonik::TaskOptionField::PartitionId => match &condition.condition {
                armonik::sessions::filter::Condition::String(filter_string) => {
                    Ok(filter_match_string(&value.default_task_options.partition_id, filter_string))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.PartitionId"
                ))),
            },
            armonik::TaskOptionField::ApplicationName => match &condition.condition {
                armonik::sessions::filter::Condition::String(filter_string) => {
                    Ok(filter_match_string(&value.default_task_options.application_name, filter_string))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.ApplicationName"
                ))),
            },
            armonik::TaskOptionField::ApplicationVersion => match &condition.condition {
                armonik::sessions::filter::Condition::String(filter_string) => {
                    Ok(filter_match_string(&value.default_task_options.application_version, filter_string))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.ApplicationVersion"
                ))),
            },
            armonik::TaskOptionField::ApplicationNamespace => match &condition.condition {
                armonik::sessions::filter::Condition::String(filter_string) => {
                    Ok(filter_match_string(&value.default_task_options.application_namespace, filter_string))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.ApplicationNamespace"
                ))),
            },
            armonik::TaskOptionField::ApplicationService => match &condition.condition {
                armonik::sessions::filter::Condition::String(filter_string) => {
                    Ok(filter_match_string(&value.default_task_options.application_service, filter_string))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.ApplicationService"
                ))),
            },
            armonik::TaskOptionField::ApplicationEngine => match &condition.condition {
                armonik::sessions::filter::Condition::String(filter_string) => {
                    Ok(filter_match_string(&value.default_task_options.engine_type, filter_string))
                }
                condition => Err(Status::invalid_argument(format!(
                    "Condition {condition:?} is not valid for the field DefaultTaskOptions.ApplicationEngine"
                ))),
            },
        },
        armonik::sessions::Field::TaskOptionGeneric(field) => match &condition.condition {
            armonik::sessions::filter::Condition::String(filter_string) => {
                if let Some(value) = value.default_task_options.options.get(field) {
                    Ok(filter_match_string(value, filter_string))
                } else {
                    Ok(false)
                }
            }
            condition => Err(Status::invalid_argument(format!(
                "Condition {condition:?} is not valid for the field DefaultTaskOptions.ApplicationEngine"
            ))),
        },
    }
}

pub(crate) fn cmp_duration(
    lhs: Option<prost_types::Duration>,
    rhs: Option<prost_types::Duration>,
) -> Ordering {
    match (lhs, rhs) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(lhs), Some(rhs)) => {
            let cmp = lhs.seconds.cmp(&rhs.seconds);
            if cmp.is_eq() {
                return cmp;
            }
            lhs.nanos.cmp(&rhs.nanos)
        }
    }
}

pub(crate) fn cmp_timestamp(
    lhs: Option<prost_types::Timestamp>,
    rhs: Option<prost_types::Timestamp>,
) -> Ordering {
    match (lhs, rhs) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(lhs), Some(rhs)) => {
            let cmp = lhs.seconds.cmp(&rhs.seconds);
            if cmp.is_eq() {
                return cmp;
            }
            lhs.nanos.cmp(&rhs.nanos)
        }
    }
}
