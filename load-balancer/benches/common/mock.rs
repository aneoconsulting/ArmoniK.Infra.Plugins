//! Mock upstream cluster for the load-balancer bench harness.
//!
//! Implements every `armonik::server::SessionsService` trait method so the mock
//! can be mounted on a tonic server via `SessionsServer::from_arc(mock)`.
//!
//! Per-method canned responses are configured via `UpstreamHandle::on_<method>`
//! setters in `common::mod`. Methods without an explicit setter return
//! `Default::default()`.
//!
//! Adding a new RPC family:
//! 1. Implement the corresponding `armonik::server::*Service` trait below.
//! 2. Add a slot field (e.g. `tasks_get: ArcSwap<Arc<TasksGetFn>>`) and the
//!    matching setter on `UpstreamHandle`.
//! 3. Add `.add_service(*Server::from_arc(mock.clone()))` to the spawn
//!    function in `common/mod.rs`.

use std::sync::Arc;

use arc_swap::ArcSwap;
use armonik::{
    reexports::tonic::Status,
    server::{RequestContext, SessionsService},
    sessions,
};

pub type SessionsGetFn = dyn Fn(&sessions::get::Request) -> sessions::get::Response + Send + Sync;

pub struct MockUpstream {
    pub(crate) sessions_get: ArcSwap<Arc<SessionsGetFn>>,
}

impl Default for MockUpstream {
    fn default() -> Self {
        let default_get: Arc<SessionsGetFn> = Arc::new(|_| sessions::get::Response::default());
        Self {
            sessions_get: ArcSwap::new(Arc::new(default_get)),
        }
    }
}

impl SessionsService for MockUpstream {
    async fn list(
        self: Arc<Self>,
        _request: sessions::list::Request,
        _context: RequestContext,
    ) -> Result<sessions::list::Response, Status> {
        Ok(sessions::list::Response::default())
    }

    async fn get(
        self: Arc<Self>,
        request: sessions::get::Request,
        _context: RequestContext,
    ) -> Result<sessions::get::Response, Status> {
        let f = self.sessions_get.load();
        Ok((f)(&request))
    }

    async fn cancel(
        self: Arc<Self>,
        _request: sessions::cancel::Request,
        _context: RequestContext,
    ) -> Result<sessions::cancel::Response, Status> {
        Ok(sessions::cancel::Response::default())
    }

    async fn create(
        self: Arc<Self>,
        _request: sessions::create::Request,
        _context: RequestContext,
    ) -> Result<sessions::create::Response, Status> {
        Ok(sessions::create::Response::default())
    }

    async fn pause(
        self: Arc<Self>,
        _request: sessions::pause::Request,
        _context: RequestContext,
    ) -> Result<sessions::pause::Response, Status> {
        Ok(sessions::pause::Response::default())
    }

    async fn resume(
        self: Arc<Self>,
        _request: sessions::resume::Request,
        _context: RequestContext,
    ) -> Result<sessions::resume::Response, Status> {
        Ok(sessions::resume::Response::default())
    }

    async fn close(
        self: Arc<Self>,
        _request: sessions::close::Request,
        _context: RequestContext,
    ) -> Result<sessions::close::Response, Status> {
        Ok(sessions::close::Response::default())
    }

    async fn purge(
        self: Arc<Self>,
        _request: sessions::purge::Request,
        _context: RequestContext,
    ) -> Result<sessions::purge::Response, Status> {
        Ok(sessions::purge::Response::default())
    }

    async fn delete(
        self: Arc<Self>,
        _request: sessions::delete::Request,
        _context: RequestContext,
    ) -> Result<sessions::delete::Response, Status> {
        Ok(sessions::delete::Response::default())
    }

    async fn stop_submission(
        self: Arc<Self>,
        _request: sessions::stop_submission::Request,
        _context: RequestContext,
    ) -> Result<sessions::stop_submission::Response, Status> {
        Ok(sessions::stop_submission::Response::default())
    }
}
