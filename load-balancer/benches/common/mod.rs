//! Shared harness for load-balancer benchmarks.
//!
//! The harness wires up two parallel topologies over localhost TCP:
//! * [`Topology::Direct`] — `mock_client → mock_server`.
//! * [`Topology::LoadBalancer`] — `mock_client → in-process LB → mock_server`.
//!
//! Diffing the two isolates the LB's own CPU/memory/latency/throughput
//! overhead.
//!
//! ## Conventions
//!
//! * Mocks are real tonic servers (no duplex, no bypass). The production
//!   `Cluster::new` path is exercised end-to-end — see
//!   [`load_balancer::cluster::Cluster`].
//! * For the `LoadBalancer` topology, the default configuration is **1
//!   upstream and 0 fallbacks** so the
//!   `Service::get_cluster_from_*` shortcut at
//!   [`load_balancer/src/service/mod.rs`] line 278 never fires; routing flows
//!   through the cache, and on misses through SQLite and cluster fan-out.
//!   Sessions configured via [`HarnessBuilder::seed_session`] are written to
//!   SQLite (via `Service::add_sessions`) and pre-warmed by a single warmup
//!   `get` so subsequent measured iterations are pure cache hits.
//! * Multi-upstream LB benches are supported (`upstreams(2..)`); the same
//!   shortcut is avoided by `clusters.len() > 1`. For Direct topology only
//!   one upstream is meaningful — requesting more panics.
//!
//! ## Adding a new RPC family
//!
//! 1. Add the slot + setter in [`mock`] (e.g. `tasks_get`, `on_tasks_get`).
//! 2. Implement the corresponding `armonik::server::*Service` trait on
//!    `MockUpstream`.
//! 3. Register `<*Server>::from_arc(mock.clone())` in
//!    [`spawn_mock_upstream`] (mock side) and in the LB-spawning branch of
//!    [`HarnessBuilder::build`].
//! 4. Add a new `benches/<family>_unary.rs` modeled on `sessions_unary.rs`.
//!
//! ## Tracing & flamegraphs
//!
//! See [`tracing::init_tracing`]. Default (no env var) is zero-cost. Run
//! `LB_BENCH_TRACE=flame cargo bench …` to render a flamegraph SVG on bench
//! exit.
//!
//! ## Allocator
//!
//! Each bench file must declare `#[global_allocator] static GLOBAL:
//! mimalloc::MiMalloc = mimalloc::MiMalloc;` at the top so allocations match
//! the production binary.

// The harness exposes several builder knobs (multi-upstream, fallbacks,
// service_options, ...) that the first bench (`sessions_unary`) does not yet
// exercise. Per-bench-binary compilation otherwise reports them as dead code.
#![allow(dead_code)]

pub mod mock;
pub mod report;
pub mod tracing;

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use armonik::{
    api::v3::sessions::sessions_server::SessionsServer, client::ClientConfigArgs,
    reexports::tonic, server::RequestContext, sessions, ClientConfig,
};
use load_balancer::{
    cluster::{Cluster, ClusterConfig},
    service::{Service, ServiceOptions},
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;

pub use mock::MockUpstream;
pub use tracing::init_tracing;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Topology {
    Direct,
    LoadBalancer,
}

/// Handle on a single mock upstream cluster: lets the bench install canned
/// responses and inspect where the mock is bound.
pub struct UpstreamHandle {
    pub mock: Arc<MockUpstream>,
    pub addr: SocketAddr,
    pub cluster_name: String,
}

impl UpstreamHandle {
    pub fn on_sessions_get<F>(&self, f: F)
    where
        F: Fn(&sessions::get::Request) -> sessions::get::Response + Send + Sync + 'static,
    {
        let func: Arc<mock::SessionsGetFn> = Arc::new(f);
        self.mock.sessions_get.store(Arc::new(func));
    }
}

/// Fully-wired bench environment. Drop aborts all background server tasks.
pub struct Harness {
    pub client: armonik::Client,
    pub mocks: Vec<UpstreamHandle>,
    pub topology: Topology,
    pub service: Option<Arc<Service>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Harness {
    pub fn builder() -> HarnessBuilder {
        HarnessBuilder::default()
    }
}

pub struct HarnessBuilder {
    topology: Topology,
    upstreams: usize,
    fallback_names: Vec<String>,
    service_options: ServiceOptions,
    cluster_pool_size: Option<usize>,
    cluster_multiplex: bool,
    seed_sessions: Vec<(String, usize)>,
}

impl Default for HarnessBuilder {
    fn default() -> Self {
        Self {
            topology: Topology::Direct,
            upstreams: 1,
            fallback_names: Vec::new(),
            service_options: ServiceOptions::default(),
            cluster_pool_size: None,
            cluster_multiplex: false,
            seed_sessions: Vec::new(),
        }
    }
}

impl HarnessBuilder {
    pub fn topology(mut self, t: Topology) -> Self {
        self.topology = t;
        self
    }
    pub fn upstreams(mut self, n: usize) -> Self {
        assert!(n >= 1, "upstreams must be >= 1");
        self.upstreams = n;
        self
    }
    pub fn fallback(mut self, name: &str) -> Self {
        self.fallback_names.push(name.to_owned());
        self
    }
    pub fn service_options(mut self, opts: ServiceOptions) -> Self {
        self.service_options = opts;
        self
    }
    pub fn cluster_pool_size(mut self, n: usize) -> Self {
        self.cluster_pool_size = Some(n);
        self
    }
    pub fn cluster_multiplex(mut self, on: bool) -> Self {
        self.cluster_multiplex = on;
        self
    }
    /// Pre-seed the session→cluster mapping in the LB. The session is written
    /// to SQLite via `Service::add_sessions` and the cache is warmed via a
    /// single `get` RPC issued during `build()`. No effect in the `Direct`
    /// topology.
    pub fn seed_session(mut self, session_id: &str, upstream_index: usize) -> Self {
        self.seed_sessions
            .push((session_id.to_owned(), upstream_index));
        self
    }

    pub async fn build(self) -> Harness {
        if matches!(self.topology, Topology::Direct) && self.upstreams != 1 {
            panic!(
                "Direct topology requires exactly 1 upstream (got {})",
                self.upstreams
            );
        }

        // Spawn N mock upstream servers.
        let mut mocks = Vec::with_capacity(self.upstreams);
        let mut tasks = Vec::new();
        for i in 0..self.upstreams {
            let (mock, addr, handle) = spawn_mock_upstream().await;
            tasks.push(handle);
            mocks.push(UpstreamHandle {
                mock,
                addr,
                cluster_name: format!("c{i}"),
            });
        }

        let (client_endpoint, service) = match self.topology {
            Topology::Direct => (format!("http://{}", mocks[0].addr), None),
            Topology::LoadBalancer => {
                // Build per-upstream Cluster configs.
                let fallback_set: HashSet<&str> =
                    self.fallback_names.iter().map(String::as_str).collect();
                let mut cluster_map = HashMap::with_capacity(self.upstreams);
                for handle in &mocks {
                    let cfg = ClusterConfig::<ClientConfig> {
                        client: client_config(&format!("http://{}", handle.addr)),
                        pool_size: self.cluster_pool_size,
                        requests_per_connection: None,
                        multiplex: self.cluster_multiplex,
                        fallback: fallback_set.contains(handle.cluster_name.as_str()),
                        forward_headers: None,
                        extra_headers: None,
                    };
                    cluster_map.insert(
                        handle.cluster_name.clone(),
                        Cluster::new(handle.cluster_name.clone(), cfg),
                    );
                }
                let service = Arc::new(
                    Service::new(
                        cluster_map,
                        self.fallback_names.clone(),
                        self.service_options,
                    )
                    .await,
                );

                // Seed sessions: write SQLite row + warm cache via one get.
                for (session_id, idx) in &self.seed_sessions {
                    let name = format!("c{idx}");
                    let cluster = service
                        .cluster_handle(&name)
                        .unwrap_or_else(|| panic!("seed_session: unknown cluster {name}"));
                    let raw = sessions::Raw {
                        session_id: session_id.clone(),
                        ..Default::default()
                    };
                    service
                        .add_sessions(vec![raw], cluster)
                        .await
                        .expect("add_sessions failed");
                    // Warm cache: one in-process get through the SessionsService trait.
                    use armonik::server::SessionsService;
                    let _ = service
                        .clone()
                        .get(
                            sessions::get::Request {
                                session_id: session_id.clone(),
                            },
                            RequestContext::default(),
                        )
                        .await;
                }

                // Spawn the LB itself.
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let svc_for_server = service.clone();
                let handle = tokio::spawn(async move {
                    let _ = tonic::transport::Server::builder()
                        .accept_http1(true)
                        .add_service(SessionsServer::from_arc(svc_for_server))
                        // future: add other *Server::from_arc(svc) here as RPCs are benched.
                        .serve_with_incoming(TcpListenerStream::new(listener))
                        .await;
                });
                tasks.push(handle);
                (format!("http://{addr}"), Some(service))
            }
        };

        let client = armonik::Client::with_config(client_config(&client_endpoint))
            .await
            .expect("armonik::Client::with_config failed for mock_client");

        Harness {
            client,
            mocks,
            topology: self.topology,
            service,
            tasks,
        }
    }
}

fn client_config(endpoint: &str) -> ClientConfig {
    let mut args = ClientConfigArgs::default();
    args.endpoint = endpoint.to_owned();
    args.allow_unsafe_connection = true;
    // Disable Nagle so unary RPCs don't trip the ~40 ms delayed-ACK timer.
    // Defaults to false in armonik's ClientConfigArgs; we want it on for both
    // the mock_client side and the LB-outbound side of the harness.
    args.tcp_nodelay = true;
    ClientConfig::from_config_args(args).expect("ClientConfig::from_config_args")
}

async fn spawn_mock_upstream() -> (Arc<MockUpstream>, SocketAddr, JoinHandle<()>) {
    let mock = Arc::new(MockUpstream::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = mock.clone();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .accept_http1(true)
            .add_service(SessionsServer::from_arc(svc))
            // future: add other *Server::from_arc(svc.clone()) here.
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (mock, addr, handle)
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn smoke_direct() {
        let h = Harness::builder()
            .topology(Topology::Direct)
            .upstreams(1)
            .build()
            .await;
        h.mocks[0].on_sessions_get(|req| sessions::get::Response {
            session: sessions::Raw {
                session_id: req.session_id.clone(),
                ..Default::default()
            },
        });
        let mut client = h.client.clone();
        let resp = client
            .sessions()
            .get("s-direct".to_owned())
            .await
            .expect("direct get failed");
        assert_eq!(resp.session_id, "s-direct");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn smoke_lb() {
        let h = Harness::builder()
            .topology(Topology::LoadBalancer)
            .upstreams(1)
            .seed_session("s-lb", 0)
            .build()
            .await;
        h.mocks[0].on_sessions_get(|req| sessions::get::Response {
            session: sessions::Raw {
                session_id: req.session_id.clone(),
                ..Default::default()
            },
        });
        let mut client = h.client.clone();
        let resp = client
            .sessions()
            .get("s-lb".to_owned())
            .await
            .expect("lb get failed");
        assert_eq!(resp.session_id, "s-lb");
    }
}
