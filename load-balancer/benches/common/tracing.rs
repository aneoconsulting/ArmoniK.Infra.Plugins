//! Tracing initialization for benches, with day-1 flamegraph rendering.
//!
//! Controlled by the `LB_BENCH_TRACE` env var:
//!
//! * unset — no subscriber is installed; `tracing::*` macros are zero-cost.
//!   Use this for timing measurements.
//! * `fmt` — pretty stdout subscriber respecting `RUST_LOG`. Useful to debug
//!   the harness; inflates measurements.
//! * `flame` — writes `target/lb-bench-flame.folded` and renders
//!   `target/lb-bench-flame.svg` on guard drop via [`inferno`]. Adds per-span
//!   overhead and inflates measurements — run separately from the timing run.
//! * `flame:<stem>` — same as `flame` but using `<stem>.folded` and
//!   `<stem>.svg` instead of the default location.
//!
//! The returned [`TraceGuard`] must be kept alive for the duration of the bench
//! process (typically by binding it in `bench_*` with `let _trace = …;`).

use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::PathBuf,
};

use tracing_flame::{FlameLayer, FlushGuard};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub struct TraceGuard {
    flame: Option<FlushGuard<BufWriter<File>>>,
    folded_path: Option<PathBuf>,
    svg_path: Option<PathBuf>,
}

pub fn init_tracing() -> TraceGuard {
    match std::env::var("LB_BENCH_TRACE").ok().as_deref() {
        None => TraceGuard {
            flame: None,
            folded_path: None,
            svg_path: None,
        },
        Some("fmt") => {
            tracing_subscriber::registry()
                .with(EnvFilter::from_default_env())
                .with(tracing_subscriber::fmt::layer())
                .init();
            TraceGuard {
                flame: None,
                folded_path: None,
                svg_path: None,
            }
        }
        Some(spec) if spec == "flame" || spec.starts_with("flame:") => {
            let stem = spec
                .strip_prefix("flame:")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("target/lb-bench-flame"));
            if let Some(parent) = stem.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).unwrap();
                }
            }
            let folded = stem.with_extension("folded");
            let svg = stem.with_extension("svg");
            let (layer, guard) = FlameLayer::with_file(&folded).unwrap();
            // `h2`/`tower` spans dominate the flamegraph if left on; the LB's
            // own spans are what we want to see.
            let env = std::env::var("RUST_LOG").unwrap_or_default();
            let filter = if env.is_empty() {
                EnvFilter::new("info,load_balancer=trace,armonik=debug,h2=off,tower=off,hyper=off")
            } else {
                EnvFilter::new(env)
            };
            tracing_subscriber::registry()
                .with(filter)
                .with(layer)
                .init();
            TraceGuard {
                flame: Some(guard),
                folded_path: Some(folded),
                svg_path: Some(svg),
            }
        }
        Some(other) => {
            panic!("LB_BENCH_TRACE={other:?} unrecognized; use 'fmt' or 'flame[:<path-stem>]'")
        }
    }
}

impl Drop for TraceGuard {
    fn drop(&mut self) {
        // Flush the flame folded file before reading it back.
        drop(self.flame.take());
        let (Some(folded), Some(svg)) = (self.folded_path.take(), self.svg_path.take()) else {
            return;
        };
        let reader = match File::open(&folded) {
            Ok(f) => BufReader::new(f),
            Err(err) => {
                eprintln!("Could not open {}: {err}", folded.display());
                return;
            }
        };
        let writer = match File::create(&svg) {
            Ok(f) => BufWriter::new(f),
            Err(err) => {
                eprintln!("Could not create {}: {err}", svg.display());
                return;
            }
        };
        let mut opts = inferno::flamegraph::Options::default();
        opts.title = "load-balancer bench".to_owned();
        if let Err(err) = inferno::flamegraph::from_reader(&mut opts, reader, writer) {
            eprintln!("inferno flamegraph rendering failed: {err}");
            return;
        }
        eprintln!("LB bench flamegraph rendered to {}", svg.display());
    }
}
