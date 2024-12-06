use std::sync::Arc;

use armonik::{
    reexports::{tokio_util, tonic},
    server::SessionsService,
    sessions,
};

use crate::utils::{impl_unary, run_with_cancellation, IntoStatus};

use super::Service;

impl SessionsService for Service {
    async fn list(
        self: Arc<Self>,
        request: sessions::list::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::list::Response, tonic::Status> {
        let Ok(page) = usize::try_from(request.page) else {
            return Err(tonic::Status::invalid_argument("Page should be positive"));
        };
        let Ok(page_size) = usize::try_from(request.page_size) else {
            return Err(tonic::Status::invalid_argument(
                "Page size should be positive",
            ));
        };
        let guard = self.mapping_session.read().await;

        let mut sessions = Vec::new();

        for (_, (_, session)) in guard.iter() {
            if cancellation_token.is_cancelled() {
                return Err(tonic::Status::aborted("Request aborted"));
            }
            for filter in &request.filters {
                let mut ok = true;

                for filter in filter {
                    if !crate::utils::filter_match_session(session, filter)? {
                        ok = false;
                        break;
                    }
                }

                if ok {
                    sessions.push(session.clone());
                    break;
                }
            }
        }

        std::mem::drop(guard);

        match request.sort.field {
            sessions::Field::Raw(raw_field) => {
                match raw_field {
                    sessions::RawField::Unspecified => (),
                    sessions::RawField::SessionId => {
                        sessions.sort_by(|a, b| a.session_id.cmp(&b.session_id))
                    }
                    sessions::RawField::Status => sessions.sort_by(|a, b| a.status.cmp(&b.status)),
                    sessions::RawField::ClientSubmission => {
                        sessions.sort_by(|a, b| a.client_submission.cmp(&b.client_submission))
                    }
                    sessions::RawField::WorkerSubmission => {
                        sessions.sort_by(|a, b| a.worker_submission.cmp(&b.worker_submission))
                    }
                    sessions::RawField::PartitionIds => {
                        sessions.sort_by(|a, b| a.partition_ids.cmp(&b.partition_ids))
                    }
                    sessions::RawField::Options => {
                        return Err(tonic::Status::invalid_argument(
                            "Field Options is not sortable",
                        ));
                    }
                    sessions::RawField::CreatedAt => sessions
                        .sort_by(|a, b| crate::utils::cmp_timestamp(a.created_at, b.created_at)),
                    sessions::RawField::CancelledAt => sessions.sort_by(|a, b| {
                        crate::utils::cmp_timestamp(a.cancelled_at, b.cancelled_at)
                    }),
                    sessions::RawField::ClosedAt => sessions
                        .sort_by(|a, b| crate::utils::cmp_timestamp(a.closed_at, b.closed_at)),
                    sessions::RawField::PurgedAt => sessions
                        .sort_by(|a, b| crate::utils::cmp_timestamp(a.purged_at, b.purged_at)),
                    sessions::RawField::DeletedAt => sessions
                        .sort_by(|a, b| crate::utils::cmp_timestamp(a.deleted_at, b.deleted_at)),
                    sessions::RawField::Duration => {
                        sessions.sort_by(|a, b| crate::utils::cmp_duration(a.duration, b.duration))
                    }
                }
            }
            sessions::Field::TaskOption(task_option_field) => match task_option_field {
                armonik::TaskOptionField::Unspecified => (),
                armonik::TaskOptionField::MaxDuration => sessions.sort_by(|a, b| {
                    crate::utils::cmp_duration(
                        Some(a.default_task_options.max_duration),
                        Some(b.default_task_options.max_duration),
                    )
                }),
                armonik::TaskOptionField::MaxRetries => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .max_retries
                        .cmp(&b.default_task_options.max_retries)
                }),
                armonik::TaskOptionField::Priority => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .priority
                        .cmp(&b.default_task_options.priority)
                }),
                armonik::TaskOptionField::PartitionId => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .partition_id
                        .cmp(&b.default_task_options.partition_id)
                }),
                armonik::TaskOptionField::ApplicationName => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .application_name
                        .cmp(&b.default_task_options.application_name)
                }),
                armonik::TaskOptionField::ApplicationVersion => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .application_version
                        .cmp(&b.default_task_options.application_version)
                }),
                armonik::TaskOptionField::ApplicationNamespace => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .application_namespace
                        .cmp(&b.default_task_options.application_namespace)
                }),
                armonik::TaskOptionField::ApplicationService => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .application_service
                        .cmp(&b.default_task_options.application_service)
                }),
                armonik::TaskOptionField::ApplicationEngine => sessions.sort_by(|a, b| {
                    a.default_task_options
                        .engine_type
                        .cmp(&b.default_task_options.engine_type)
                }),
            },
            sessions::Field::TaskOptionGeneric(key) => {
                sessions.sort_by(|a, b| {
                    a.default_task_options
                        .options
                        .get(&key)
                        .cmp(&b.default_task_options.options.get(&key))
                });
            }
        }

        if matches!(request.sort.direction, armonik::SortDirection::Desc) {
            sessions.reverse();
        }

        let total = sessions.len() as i32;

        Ok(armonik::sessions::list::Response {
            sessions: sessions
                .into_iter()
                .skip(page * page_size)
                .take(page_size)
                .collect(),
            page: request.page,
            page_size: request.page_size,
            total,
        })
    }

    async fn get(
        self: Arc<Self>,
        request: sessions::get::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::get::Response, tonic::Status> {
        impl_unary!(self.sessions, request, cancellation_token, session)
    }

    async fn cancel(
        self: Arc<Self>,
        request: sessions::cancel::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::cancel::Response, tonic::Status> {
        impl_unary!(self.sessions, request, cancellation_token, session)
    }

    async fn create(
        self: Arc<Self>,
        request: sessions::create::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::create::Response, tonic::Status> {
        let n = self.clusters.len();
        let i = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut err = None;

        for (_, cluster) in self.clusters.iter().cycle().skip(i % n).take(n) {
            match run_with_cancellation!(cancellation_token, cluster.client()) {
                Ok(client) => {
                    let response = run_with_cancellation!(
                        cancellation_token,
                        client.sessions().call(request.clone())
                    );

                    match response {
                        Ok(response) => return Ok(response),
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
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::pause::Response, tonic::Status> {
        impl_unary!(self.sessions, request, cancellation_token, session)
    }

    async fn resume(
        self: Arc<Self>,
        request: sessions::resume::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::resume::Response, tonic::Status> {
        impl_unary!(self.sessions, request, cancellation_token, session)
    }

    async fn close(
        self: Arc<Self>,
        request: sessions::close::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::close::Response, tonic::Status> {
        impl_unary!(self.sessions, request, cancellation_token, session)
    }

    async fn purge(
        self: Arc<Self>,
        request: sessions::purge::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::purge::Response, tonic::Status> {
        impl_unary!(self.sessions, request, cancellation_token, session)
    }

    async fn delete(
        self: Arc<Self>,
        request: sessions::delete::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::delete::Response, tonic::Status> {
        let service = self.clone();
        let response = impl_unary!(service.sessions, request, cancellation_token, session)?;

        // If delete is successful, remove the session from the list
        let mut guard =
            crate::utils::run_with_cancellation!(cancellation_token, self.mapping_session.write());

        guard.remove(&response.session.session_id);

        Ok(response)
    }

    async fn stop_submission(
        self: Arc<Self>,
        request: sessions::stop_submission::Request,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<sessions::stop_submission::Response, tonic::Status> {
        impl_unary!(self.sessions, request, cancellation_token, session)
    }
}
