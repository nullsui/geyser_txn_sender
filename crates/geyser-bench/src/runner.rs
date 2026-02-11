use crate::report::{self, BenchReport, BenchResult, Comparison};
use crate::workloads::{LatencySample, RaceServerDetail, Workload};
use std::time::Duration;
use tracing::{info, warn};

/// Extract response-race winner (first to return to client) and execution-race winner
/// (earliest on-chain execution relative to client T=0) from race details.
///
/// The execution winner accounts for network transit, not just server processing:
///   execution_at ≈ one_way_to_proxy + server_total
///                = (elapsed - server_total) / 2 + server_total
///                = (elapsed + server_total) / 2
///
/// Returns (response_winner, exec_winner).
fn race_winners(details: &[RaceServerDetail]) -> (Option<String>, Option<String>) {
    let response = details
        .iter()
        .find(|s| s.is_winner)
        .map(|s| s.label.clone());

    let exec = details
        .iter()
        .filter_map(|s| {
            let server_total = s.server_submit_ms? + s.server_effects_ms?;
            let elapsed_ms = s.elapsed.as_millis() as u64;
            // (elapsed + server_total) / 2 approximates when execution completed
            // relative to client T=0. We skip the /2 since it's constant for comparison.
            Some((s, elapsed_ms + server_total))
        })
        .min_by_key(|(_, score)| *score)
        .map(|(s, _)| s.label.clone());

    (response, exec)
}

/// Benchmark runner configuration.
pub struct RunConfig {
    pub iterations: usize,
    pub warmup: usize,
    pub delay: Duration,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            iterations: 50,
            warmup: 5,
            delay: Duration::from_millis(500),
        }
    }
}

/// Run a single workload benchmark.
pub async fn run_workload(workload: &dyn Workload, config: &RunConfig) -> BenchResult {
    info!(
        workload = workload.name(),
        warmup = config.warmup,
        iterations = config.iterations,
        "Starting benchmark"
    );

    // Warmup phase
    for i in 0..config.warmup {
        match workload.execute().await {
            Ok(sample) => {
                if let Some(server_ms) = sample.server_total_ms {
                    let total = sample.total.as_millis() as u64;
                    let network_ms = total.saturating_sub(server_ms);
                    let submit = sample.server_submit_ms.unwrap_or(0);
                    let effects = sample.server_effects_ms.unwrap_or(0);
                    let (resp_w, exec_w) = sample
                        .race_details
                        .as_deref()
                        .map(race_winners)
                        .unwrap_or((None, None));
                    info!(
                        workload = workload.name(),
                        warmup = i + 1,
                        total_ms = total,
                        network_ms,
                        server_ms,
                        server_submit_ms = submit,
                        server_effects_ms = effects,
                        responded = resp_w.as_deref().unwrap_or("-"),
                        executed = exec_w.as_deref().unwrap_or("-"),
                        "Warmup iteration"
                    );
                } else {
                    info!(
                        workload = workload.name(),
                        warmup = i + 1,
                        submit_ms = sample.submit.as_millis(),
                        decode_ms = sample.decode.as_millis(),
                        total_ms = sample.total.as_millis(),
                        "Warmup iteration"
                    );
                }
            }
            Err(e) => {
                warn!(
                    workload = workload.name(),
                    warmup = i + 1,
                    error = %e,
                    "Warmup iteration failed"
                );
            }
        }
        tokio::time::sleep(config.delay).await;
    }

    // Measured phase
    let mut samples: Vec<LatencySample> = Vec::with_capacity(config.iterations);
    for i in 0..config.iterations {
        match workload.execute().await {
            Ok(sample) => {
                if let Some(server_ms) = sample.server_total_ms {
                    let total = sample.total.as_millis() as u64;
                    let network_ms = total.saturating_sub(server_ms);
                    let submit = sample.server_submit_ms.unwrap_or(0);
                    let effects = sample.server_effects_ms.unwrap_or(0);
                    let (resp_w, exec_w) = sample
                        .race_details
                        .as_deref()
                        .map(race_winners)
                        .unwrap_or((None, None));
                    info!(
                        workload = workload.name(),
                        iteration = i + 1,
                        total_ms = total,
                        network_ms,
                        server_ms,
                        server_submit_ms = submit,
                        server_effects_ms = effects,
                        responded = resp_w.as_deref().unwrap_or("-"),
                        executed = exec_w.as_deref().unwrap_or("-"),
                        "Measured iteration"
                    );
                } else {
                    info!(
                        workload = workload.name(),
                        iteration = i + 1,
                        submit_ms = sample.submit.as_millis(),
                        decode_ms = sample.decode.as_millis(),
                        total_ms = sample.total.as_millis(),
                        inline_executed = sample.was_inline_executed,
                        speculative = sample.was_speculative,
                        "Measured iteration"
                    );
                }
                samples.push(sample);
            }
            Err(e) => {
                warn!(
                    workload = workload.name(),
                    iteration = i + 1,
                    error = %e,
                    "Measured iteration failed"
                );
            }
        }
        tokio::time::sleep(config.delay).await;
    }

    report::build_result(workload.name(), workload.description(), &samples)
}

/// Run all workloads and generate a comparison report.
pub async fn run_all(workloads: Vec<(&str, Box<dyn Workload>)>, config: &RunConfig) -> BenchReport {
    let mut results = Vec::new();

    for (label, workload) in &workloads {
        info!(label, "Running workload");
        let result = run_workload(workload.as_ref(), config).await;
        results.push(result);
    }

    // Generate comparisons: direct-* vs cdn-fast-* vs grpc-* vs vanilla-*
    let comparisons = generate_comparisons(&results);

    BenchReport {
        results,
        comparisons,
    }
}

/// Generate comparisons across all submission paths.
/// Groups by workload suffix (transfer, counter, mint) and compares all paths.
fn generate_comparisons(results: &[BenchResult]) -> Vec<Comparison> {
    let mut comparisons = Vec::new();

    // Find unique workload suffixes from vanilla-* results (always present)
    let suffixes: Vec<String> = results
        .iter()
        .filter_map(|r| r.name.strip_prefix("vanilla-").map(|s| s.to_string()))
        .collect();

    for suffix in &suffixes {
        let direct = results
            .iter()
            .find(|r| r.name == format!("direct-{}", suffix));
        let cdn_fast = results
            .iter()
            .find(|r| r.name == format!("cdn-fast-{}", suffix));
        let grpc = results
            .iter()
            .find(|r| r.name == format!("grpc-{}", suffix));
        let vanilla = results
            .iter()
            .find(|r| r.name == format!("vanilla-{}", suffix));

        if let Some(vanilla_result) = vanilla {
            let vanilla_p50 = vanilla_result.p50_ms;

            let direct_p50 = direct.map(|r| r.p50_ms);
            let cdn_fast_p50 = cdn_fast.map(|r| r.p50_ms);
            let grpc_p50 = grpc.map(|r| r.p50_ms);

            let calc_improvement = |p50: Option<f64>| -> Option<f64> {
                p50.map(|v| {
                    if vanilla_p50 > 0.0 {
                        ((vanilla_p50 - v) / vanilla_p50) * 100.0
                    } else {
                        0.0
                    }
                })
            };

            comparisons.push(Comparison {
                workload: suffix.clone(),
                direct_p50_ms: direct_p50,
                cdn_fast_p50_ms: cdn_fast_p50,
                grpc_p50_ms: grpc_p50,
                vanilla_p50_ms: vanilla_p50,
                direct_vs_vanilla_pct: calc_improvement(direct_p50),
                cdn_fast_vs_vanilla_pct: calc_improvement(cdn_fast_p50),
                grpc_vs_vanilla_pct: calc_improvement(grpc_p50),
            });
        }
    }

    comparisons
}
