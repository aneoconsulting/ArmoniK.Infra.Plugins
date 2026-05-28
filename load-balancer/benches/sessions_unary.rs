//! Latency and throughput benchmarks for `sessions::get`, comparing the
//! `Direct` and `LoadBalancer` topologies over localhost TCP.
//!
//! Run with `cargo bench --bench sessions_unary`.
//!
//! `LB_BENCH_TRACE=flame cargo bench --bench sessions_unary` renders a
//! flamegraph SVG on exit (see `benches/common/tracing.rs`).

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod common;

use std::time::SystemTime;

use armonik::sessions;
use common::{Harness, Topology};
use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let session_id = "s-1";

    // ---- latency: one in-flight RPC at a time ----
    let mut lat = c.benchmark_group("sessions_get_latency");
    for topology in [Topology::Direct, Topology::LoadBalancer] {
        let h = rt.block_on(async {
            let h = Harness::builder()
                .topology(topology)
                .upstreams(1)
                // No fallback: with 1 cluster + 0 fallbacks the LB's
                // single-cluster-single-fallback shortcut never fires; real
                // routing runs (cache hit thanks to seed_session below).
                .seed_session(session_id, 0)
                .build()
                .await;
            install_canned_get(&h.mocks[0]);
            h
        });
        let client = h.client.clone();
        lat.bench_function(format!("{topology:?}"), |b| {
            b.to_async(&rt).iter(|| {
                let mut client = client.clone();
                async move { client.sessions().get(session_id).await.unwrap() }
            });
        });
        // Drop harness (aborts background tasks) before moving to the next one
        // so the next harness can re-bind 127.0.0.1:0 freely.
        drop(h);
    }
    lat.finish();

    // ---- throughput: N concurrent RPCs per iter ----
    let mut tp = c.benchmark_group("sessions_get_throughput");
    for topology in [Topology::Direct, Topology::LoadBalancer] {
        let h = rt.block_on(async {
            let h = Harness::builder()
                .topology(topology)
                .upstreams(1)
                .seed_session(session_id, 0)
                .cluster_pool_size(64)
                .cluster_multiplex(true)
                .build()
                .await;
            install_canned_get(&h.mocks[0]);
            h
        });
        let client = h.client.clone();
        for &concurrency in &[1usize, 8, 64, 256, 1024] {
            tp.throughput(Throughput::Elements(concurrency as u64));
            tp.bench_with_input(
                BenchmarkId::new(format!("{topology:?}"), concurrency),
                &concurrency,
                |b, &n| {
                    b.to_async(&rt).iter(|| {
                        let client = client.clone();
                        async move {
                            let mut set = tokio::task::JoinSet::new();
                            for _ in 0..n {
                                let mut c = client.clone();
                                set.spawn(
                                    async move { c.sessions().get(session_id).await.unwrap() },
                                );
                            }
                            while let Some(res) = set.join_next().await {
                                let _ = res.unwrap();
                            }
                        }
                    });
                },
            );
        }
        drop(h);
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
