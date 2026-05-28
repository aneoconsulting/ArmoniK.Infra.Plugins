//! Synthesize a CSV summary from criterion's per-bench JSON output.
//!
//! Criterion writes:
//!
//! ```text
//! target/criterion/<group>/<function>/[<param>/]new/benchmark.json
//! target/criterion/<group>/<function>/[<param>/]new/estimates.json
//! ```
//!
//! [`summarize_to_csv`] walks the given root, picks up every pair of those
//! two files whose mtime is `>= since`, and emits one CSV row per bench. The
//! `since` filter prevents stale runs from leaking in if the harness has run
//! before. The row is also echoed to stdout so it's visible at the end of
//! `cargo bench`.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::Deserialize;

/// Resolve criterion's output directory using the same lookup criterion uses
/// internally (see `cargo_target_directory` in criterion 0.5):
///
/// 1. `$CRITERION_HOME` if set.
/// 2. Else `$CARGO_TARGET_DIR/criterion` if set.
/// 3. Else `cargo metadata`'s `target_directory` + `/criterion` (this is what
///    criterion does so the path agrees with cargo's actual workspace target).
/// 4. Else `target/criterion` relative to CWD as a last resort.
///
/// This matters in a cargo workspace: `cargo bench` does *not* set
/// `CARGO_TARGET_DIR`, and the bench process CWD is the package dir, so a
/// naive relative `target/criterion` would resolve to `<package>/target/...`
/// while criterion is actually writing to `<workspace>/target/...`.
pub fn criterion_home() -> PathBuf {
    if let Some(home) = std::env::var_os("CRITERION_HOME") {
        return PathBuf::from(home);
    }
    if let Some(target) = std::env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(target).join("criterion");
    }
    if let Some(target) = cargo_metadata_target_directory() {
        return target.join("criterion");
    }
    PathBuf::from("target").join("criterion")
}

fn cargo_metadata_target_directory() -> Option<PathBuf> {
    #[derive(Deserialize)]
    struct Metadata {
        target_directory: PathBuf,
    }
    let cargo = std::env::var_os("CARGO")?;
    let output = std::process::Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let metadata: Metadata = serde_json::from_slice(&output.stdout).ok()?;
    Some(metadata.target_directory)
}

#[derive(Deserialize)]
struct PointEst {
    point_estimate: f64,
    confidence_interval: ConfidenceInterval,
    #[allow(dead_code)]
    standard_error: f64,
}

#[derive(Deserialize)]
struct ConfidenceInterval {
    #[allow(dead_code)]
    confidence_level: f64,
    lower_bound: f64,
    upper_bound: f64,
}

#[derive(Deserialize)]
struct Estimates {
    mean: PointEst,
    median: PointEst,
    std_dev: PointEst,
}

#[derive(Deserialize)]
enum Throughput {
    Bytes(u64),
    BytesDecimal(u64),
    Elements(u64),
}

#[derive(Deserialize)]
struct Benchmark {
    group_id: Option<String>,
    function_id: Option<String>,
    value_str: Option<String>,
    throughput: Option<Throughput>,
    #[allow(dead_code)]
    full_id: String,
}

/// Walk `root`, find every `<dir>/new/{benchmark,estimates}.json` pair newer
/// than `since`, and emit a CSV file at `out_csv`. The same CSV is echoed to
/// stdout for terminal viewers.
pub fn summarize_to_csv(
    root: impl AsRef<Path>,
    out_csv: impl AsRef<Path>,
    since: SystemTime,
) -> std::io::Result<()> {
    let mut rows = Vec::<(Benchmark, Estimates)>::new();
    walk(root.as_ref(), since, &mut rows);
    if rows.is_empty() {
        eprintln!("No new bench results found, CSV summary skipped.");
        return Ok(());
    }
    rows.sort_by(|a, b| {
        let key = |b: &Benchmark| {
            (
                b.group_id.clone().unwrap_or_default(),
                b.function_id.clone().unwrap_or_default(),
                parse_param(b.value_str.as_deref()),
                b.value_str.clone().unwrap_or_default(),
            )
        };
        key(&a.0).cmp(&key(&b.0))
    });

    let out_csv = out_csv.as_ref();
    if let Some(parent) = out_csv.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = BufWriter::new(File::create(out_csv)?);
    let header = "group,function,parameter,mean_ms,median_ms,mean_ci_lo_ms,mean_ci_hi_ms,std_dev_ms,throughput_kind,throughput_count,elements_per_sec\n";
    writer.write_all(header.as_bytes())?;
    print!("\n=== Bench summary ===\n{header}");
    for (bench, est) in &rows {
        let median_ns = est.median.point_estimate;
        let (kind, count, per_sec) = match bench.throughput.as_ref() {
            Some(Throughput::Bytes(n)) => (
                "Bytes",
                Some(*n as f64),
                Some(1e9 * (*n as f64) / median_ns),
            ),
            Some(Throughput::BytesDecimal(n)) => (
                "BytesDecimal",
                Some(*n as f64),
                Some(1e9 * (*n as f64) / median_ns),
            ),
            Some(Throughput::Elements(n)) => (
                "Elements",
                Some(*n as f64),
                Some(1e9 * (*n as f64) / median_ns),
            ),
            None => ("", None, None),
        };
        let group = bench.group_id.as_deref().unwrap_or("");
        let func = bench.function_id.as_deref().unwrap_or("");
        let param = bench.value_str.as_deref().unwrap_or("");
        let line = format!(
            "{group},{func},{param},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{}\n",
            ns_to_ms(est.mean.point_estimate),
            ns_to_ms(median_ns),
            ns_to_ms(est.mean.confidence_interval.lower_bound),
            ns_to_ms(est.mean.confidence_interval.upper_bound),
            ns_to_ms(est.std_dev.point_estimate),
            kind,
            opt(count),
            opt(per_sec),
        );
        writer.write_all(line.as_bytes())?;
        print!("{line}");
    }
    writer.flush()?;
    eprintln!("CSV summary: {}", out_csv.display());
    Ok(())
}

fn opt(v: Option<f64>) -> String {
    match v {
        Some(x) => format!("{x:.1}"),
        None => String::new(),
    }
}

fn ns_to_ms(ns: f64) -> f64 {
    ns / 1_000_000.0
}

fn parse_param(s: Option<&str>) -> Option<u64> {
    s.and_then(|s| s.parse().ok())
}

fn walk(dir: &Path, since: SystemTime, out: &mut Vec<(Benchmark, Estimates)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let bench_path = path.join("new").join("benchmark.json");
        let est_path = path.join("new").join("estimates.json");
        if bench_path.is_file() && est_path.is_file() {
            // Only include results whose estimates were written by this run.
            let fresh = std::fs::metadata(&est_path)
                .and_then(|m| m.modified())
                .map(|m| m >= since)
                .unwrap_or(false);
            if !fresh {
                continue;
            }
            let bench = match load_json::<Benchmark>(&bench_path) {
                Ok(b) => b,
                Err(err) => {
                    eprintln!("skip {}: {err}", bench_path.display());
                    continue;
                }
            };
            let est = match load_json::<Estimates>(&est_path) {
                Ok(e) => e,
                Err(err) => {
                    eprintln!("skip {}: {err}", est_path.display());
                    continue;
                }
            };
            out.push((bench, est));
        } else {
            walk(&path, since, out);
        }
    }
}

fn load_json<T: for<'a> Deserialize<'a>>(path: &Path) -> std::io::Result<T> {
    let file = File::open(path)?;
    serde_json::from_reader(BufReader::new(file))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
