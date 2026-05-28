//! Latency and throughput benchmarks for `sessions::get`, comparing the
//! `Direct` and `LoadBalancer` topologies over localhost TCP.
//!
//! Run with `cargo bench --bench sessions_unary`.
//!
//! `LB_BENCH_TRACE=flame cargo bench --bench sessions_unary` renders a
//! flamegraph SVG on exit (see `benches/common/tracing.rs`).
//!
//! ## Topology
//!
//! * `Direct`: each concurrent RPC has its own `armonik::Client` →
//!   `mock_server`. N independent TCP connections.
//! * `LoadBalancer`: each concurrent RPC has its own `armonik::Client` → LB →
//!   single upstream. The LB is configured with `cluster_pool_size = N` and
//!   `cluster_multiplex = false` so concurrent LB requests each pop a distinct
//!   upstream `Client` from the pool — N independent Channels on the
//!   LB→upstream link too. This isolates the LB's own per-RPC overhead from
//!   the single-Channel `tower::buffer` serialization that would otherwise
//!   dominate.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod common;

use std::time::SystemTime;

use armonik::sessions;
use common::{Harness, Topology};
use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

const CONCURRENCIES: &[usize] = &[1, 8, 64, 256, 1024, 4096];

fn install_canned_get(handle: &common::UpstreamHandle) {
    handle.on_sessions_get(|req| sessions::get::Response {
        session: sessions::Raw {
            session_id: req.session_id.clone(),
            ..Default::default()
        },
    });
}

fn bench_sessions_get(c: &mut Criterion) {
    let _trace = common::init_tracing();
    let runtimes = common::runtime::Runtimes::from_env();
    let session_id = "s-1";

    // ---- latency: one in-flight RPC at a time on a single client ----
    let mut lat = c.benchmark_group("sessions_get_latency");
    for topology in [Topology::Direct, Topology::LoadBalancer] {
        let h = runtimes.mocks().block_on(async {
            let h = Harness::builder()
                .topology(topology)
                .upstreams(1)
                // No fallback: with 1 cluster + 0 fallbacks the LB's
                // single-cluster-single-fallback shortcut never fires; real
                // routing runs (cache hit thanks to seed_session below).
                .seed_session(session_id, 0)
                .cluster_pool_size(1)
                .cluster_multiplex(false)
                .build(&runtimes)
                .await;
            install_canned_get(&h.mocks[0]);
            h
        });
        let client = h.client.clone();
        lat.bench_function(format!("{topology:?}"), |b| {
            b.to_async(runtimes.mocks()).iter(|| {
                let mut client = client.clone();
                async move { client.sessions().get(session_id).await.unwrap() }
            });
        });
        drop(h);
    }
    lat.finish();

    // ---- throughput: N concurrent RPCs per iter, each on its own client ----
    let mut tp = c.benchmark_group("sessions_get_throughput");
    for topology in [Topology::Direct, Topology::LoadBalancer] {
        for &concurrency in CONCURRENCIES {
            let (h, clients) = runtimes.mocks().block_on(async {
                let h = Harness::builder()
                    .topology(topology)
                    .upstreams(1)
                    .seed_session(session_id, 0)
                    .cluster_pool_size(concurrency)
                    .cluster_multiplex(false)
                    .build(&runtimes)
                    .await;
                install_canned_get(&h.mocks[0]);
                let clients = h.make_clients(concurrency).await;
                (h, clients)
            });
            tp.throughput(Throughput::Elements(concurrency as u64));
            tp.bench_with_input(
                BenchmarkId::new(format!("{topology:?}"), concurrency),
                &concurrency,
                |b, &_n| {
                    let clients = clients.clone();
                    b.to_async(runtimes.mocks()).iter(move || {
                        let clients = clients.clone();
                        async move {
                            let futs = clients.into_iter().map(|mut c| async move {
                                c.sessions().get(session_id).await.unwrap()
                            });
                            let _ = futures::future::join_all(futs).await;
                        }
                    });
                },
            );
            drop(clients);
            drop(h);
        }
    }
    tp.finish();
}

criterion_group!(g, bench_sessions_get);

fn main() {
    let started = SystemTime::now();
    g();
    Criterion::default().configure_from_args().final_summary();
    let crit_home = common::report::criterion_home();
    let csv = crit_home.join("sessions_unary-summary.csv");
    if let Err(err) = common::report::summarize_to_csv(&crit_home, &csv, started) {
        eprintln!("CSV synthesis failed: {err}");
    }
}
