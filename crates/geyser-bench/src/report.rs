use crate::workloads::LatencySample;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::collections::HashMap;

/// A single benchmark result.
#[derive(Debug, Clone, Serialize)]
pub struct BenchResult {
    pub name: String,
    pub description: String,
    pub iterations: u64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub submit_mean_ms: f64,
    pub submit_p50_ms: f64,
    pub decode_mean_ms: f64,
    pub decode_p50_ms: f64,
    /// Number of iterations where SubmitTransaction returned Executed inline (1 RTT).
    pub inline_executed_count: u64,
    /// Number of iterations where the result came from speculative WaitForEffects.
    pub speculative_count: u64,
    /// CDN server-side total latency p50 (from `geyser.latencyMs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_total_p50_ms: Option<f64>,
    /// CDN server-side submit phase p50 (from `geyser.submitMs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_submit_p50_ms: Option<f64>,
    /// CDN server-side effects phase p50 (from `geyser.effectsMs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_effects_p50_ms: Option<f64>,
    /// Estimated network round-trip p50 (total - server_total).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_p50_ms: Option<f64>,
    /// Per-server race statistics (label → stats).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub race_server_stats: Vec<RaceServerStats>,
}

/// Aggregate stats for one server across all race iterations.
#[derive(Debug, Clone, Serialize)]
pub struct RaceServerStats {
    pub label: String,
    pub wins: u64,
    pub p50_ms: f64,
    pub mean_ms: f64,
    pub server_submit_p50_ms: Option<f64>,
    pub server_effects_p50_ms: Option<f64>,
}

/// Full benchmark report with comparison.
#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    pub results: Vec<BenchResult>,
    pub comparisons: Vec<Comparison>,
}

/// Comparison across submission paths.
#[derive(Debug, Clone, Serialize)]
pub struct Comparison {
    pub workload: String,
    pub direct_p50_ms: Option<f64>,
    pub cdn_fast_p50_ms: Option<f64>,
    pub grpc_p50_ms: Option<f64>,
    pub vanilla_p50_ms: f64,
    pub direct_vs_vanilla_pct: Option<f64>,
    pub cdn_fast_vs_vanilla_pct: Option<f64>,
    pub grpc_vs_vanilla_pct: Option<f64>,
}

/// Build a BenchResult from a set of latency measurements.
pub fn build_result(name: &str, description: &str, samples: &[LatencySample]) -> BenchResult {
    let mut total_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("Failed to create total histogram");
    let mut submit_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("Failed to create submit histogram");
    let mut decode_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("Failed to create decode histogram");

    let mut inline_executed_count = 0u64;
    let mut speculative_count = 0u64;

    // Server timing histograms (CDN fast mode only)
    let mut server_total_hist: Option<Histogram<u64>> = None;
    let mut server_submit_hist: Option<Histogram<u64>> = None;
    let mut server_effects_hist: Option<Histogram<u64>> = None;

    for sample in samples {
        let _ = total_hist.record(sample.total.as_micros().max(1) as u64);
        let _ = submit_hist.record(sample.submit.as_micros().max(1) as u64);
        let _ = decode_hist.record(sample.decode.as_micros().max(1) as u64);
        if sample.was_inline_executed {
            inline_executed_count += 1;
        }
        if sample.was_speculative {
            speculative_count += 1;
        }
        if let (Some(st), Some(ss), Some(se)) = (
            sample.server_total_ms,
            sample.server_submit_ms,
            sample.server_effects_ms,
        ) {
            let st_h = server_total_hist
                .get_or_insert_with(|| Histogram::new_with_bounds(1, 60_000_000, 3).unwrap());
            let ss_h = server_submit_hist
                .get_or_insert_with(|| Histogram::new_with_bounds(1, 60_000_000, 3).unwrap());
            let se_h = server_effects_hist
                .get_or_insert_with(|| Histogram::new_with_bounds(1, 60_000_000, 3).unwrap());
            // Values are already in ms; store as ms * 1000 (micros) for consistency
            let _ = st_h.record((st * 1000).max(1));
            let _ = ss_h.record((ss * 1000).max(1));
            let _ = se_h.record((se * 1000).max(1));
        }
    }

    let server_total_p50_ms = server_total_hist
        .as_ref()
        .map(|h| h.value_at_percentile(50.0) as f64 / 1000.0);
    let server_submit_p50_ms = server_submit_hist
        .as_ref()
        .map(|h| h.value_at_percentile(50.0) as f64 / 1000.0);
    let server_effects_p50_ms = server_effects_hist
        .as_ref()
        .map(|h| h.value_at_percentile(50.0) as f64 / 1000.0);
    let network_p50_ms = server_total_p50_ms.map(|st| {
        let total_p50 = total_hist.value_at_percentile(50.0) as f64 / 1000.0;
        (total_p50 - st).max(0.0)
    });

    // Aggregate per-server race stats
    let mut per_server: HashMap<
        String,
        (
            u64,
            Histogram<u64>,
            Option<Histogram<u64>>,
            Option<Histogram<u64>>,
        ),
    > = HashMap::new();
    // Track insertion order
    let mut server_order: Vec<String> = Vec::new();
    for sample in samples {
        if let Some(ref details) = sample.race_details {
            for d in details {
                let entry = per_server.entry(d.label.clone()).or_insert_with(|| {
                    server_order.push(d.label.clone());
                    (
                        0,
                        Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
                        None,
                        None,
                    )
                });
                if d.is_winner {
                    entry.0 += 1;
                }
                let _ = entry.1.record(d.elapsed.as_micros().max(1) as u64);
                if let Some(ss) = d.server_submit_ms {
                    let h = entry.2.get_or_insert_with(|| {
                        Histogram::new_with_bounds(1, 60_000_000, 3).unwrap()
                    });
                    let _ = h.record((ss * 1000).max(1));
                }
                if let Some(se) = d.server_effects_ms {
                    let h = entry.3.get_or_insert_with(|| {
                        Histogram::new_with_bounds(1, 60_000_000, 3).unwrap()
                    });
                    let _ = h.record((se * 1000).max(1));
                }
            }
        }
    }
    let race_server_stats: Vec<RaceServerStats> = server_order
        .iter()
        .filter_map(|label| {
            per_server
                .get(label)
                .map(|(wins, hist, sub_hist, eff_hist)| RaceServerStats {
                    label: label.clone(),
                    wins: *wins,
                    p50_ms: hist.value_at_percentile(50.0) as f64 / 1000.0,
                    mean_ms: hist.mean() / 1000.0,
                    server_submit_p50_ms: sub_hist
                        .as_ref()
                        .map(|h| h.value_at_percentile(50.0) as f64 / 1000.0),
                    server_effects_p50_ms: eff_hist
                        .as_ref()
                        .map(|h| h.value_at_percentile(50.0) as f64 / 1000.0),
                })
        })
        .collect();

    BenchResult {
        name: name.to_string(),
        description: description.to_string(),
        iterations: total_hist.len(),
        min_ms: total_hist.min() as f64 / 1000.0,
        max_ms: total_hist.max() as f64 / 1000.0,
        mean_ms: total_hist.mean() / 1000.0,
        p50_ms: total_hist.value_at_percentile(50.0) as f64 / 1000.0,
        p95_ms: total_hist.value_at_percentile(95.0) as f64 / 1000.0,
        p99_ms: total_hist.value_at_percentile(99.0) as f64 / 1000.0,
        submit_mean_ms: submit_hist.mean() / 1000.0,
        submit_p50_ms: submit_hist.value_at_percentile(50.0) as f64 / 1000.0,
        decode_mean_ms: decode_hist.mean() / 1000.0,
        decode_p50_ms: decode_hist.value_at_percentile(50.0) as f64 / 1000.0,
        inline_executed_count,
        speculative_count,
        server_total_p50_ms,
        server_submit_p50_ms,
        server_effects_p50_ms,
        network_p50_ms,
        race_server_stats,
    }
}

/// Format a report as a table.
pub fn format_table(report: &BenchReport) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "\n{:<25} {:>6} {:>9} {:>10} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}\n",
        "Test", "N", "SubP50", "DecP50", "Min", "P50", "P95", "P99", "Max", "Avg"
    ));
    out.push_str(&"-".repeat(118));
    out.push('\n');

    for r in &report.results {
        let mut line = format!(
            "{:<25} {:>6} {:>8.1}ms {:>9.3}ms {:>7.1}ms {:>7.1}ms {:>7.1}ms {:>7.1}ms {:>7.1}ms {:>7.1}ms",
            r.name,
            r.iterations,
            r.submit_p50_ms,
            r.decode_p50_ms,
            r.min_ms,
            r.p50_ms,
            r.p95_ms,
            r.p99_ms,
            r.max_ms,
            r.mean_ms,
        );
        // Show diagnostic counts for direct-* results
        if r.name.starts_with("direct-") && r.iterations > 0 {
            line.push_str(&format!(
                "  [inline:{}/spec:{}]",
                r.inline_executed_count, r.speculative_count
            ));
        }
        line.push('\n');
        out.push_str(&line);
        // Show CDN server timing breakdown (winner)
        if let (Some(net), Some(sub), Some(eff)) = (
            r.network_p50_ms,
            r.server_submit_p50_ms,
            r.server_effects_p50_ms,
        ) {
            out.push_str(&format!(
                "{:>25}  Network: {:.0}ms | Submit: {:.0}ms | Effects: {:.0}ms\n",
                "\u{2514}\u{2500}", net, sub, eff,
            ));
        }
        // Show per-server race breakdown
        if !r.race_server_stats.is_empty() {
            for s in &r.race_server_stats {
                let timing = match (s.server_submit_p50_ms, s.server_effects_p50_ms) {
                    (Some(sub), Some(eff)) => {
                        let net = (s.p50_ms - sub - eff).max(0.0);
                        format!(" (net:{:.0} sub:{:.0} eff:{:.0})", net, sub, eff)
                    }
                    _ => String::new(),
                };
                out.push_str(&format!(
                    "{:>25}  {:<5} wins:{:>2}/{:<2} p50:{:.0}ms avg:{:.0}ms{}\n",
                    "\u{251c}\u{2500}", s.label, s.wins, r.iterations, s.p50_ms, s.mean_ms, timing,
                ));
            }
        }
    }

    if !report.comparisons.is_empty() {
        let has_direct = report.comparisons.iter().any(|c| c.direct_p50_ms.is_some());
        let has_cdn_fast = report
            .comparisons
            .iter()
            .any(|c| c.cdn_fast_p50_ms.is_some());
        let has_grpc = report.comparisons.iter().any(|c| c.grpc_p50_ms.is_some());

        let fmt_p50 = |v: Option<f64>| -> String {
            v.map(|v| format!("{:.1}ms", v))
                .unwrap_or_else(|| "  --".to_string())
        };
        let fmt_pct = |v: Option<f64>| -> String {
            v.map(|v| {
                let sign = if v > 0.0 { "+" } else { "" };
                format!("{}{:.1}%", sign, v)
            })
            .unwrap_or_else(|| "--".to_string())
        };

        // Build header dynamically
        let mut header = format!("\n{:<18}", "Comparison");
        if has_direct {
            header.push_str(&format!(" {:>12}", "Direct P50"));
        }
        if has_cdn_fast {
            header.push_str(&format!(" {:>14}", "CDN-Fast P50"));
        }
        if has_grpc {
            header.push_str(&format!(" {:>12}", "gRPC P50"));
        }
        header.push_str(&format!(" {:>12}", "Vanilla P50"));
        if has_direct {
            header.push_str(&format!(" {:>16}", "Direct vs Van."));
        }
        if has_cdn_fast {
            header.push_str(&format!(" {:>18}", "CDN-Fast vs Van."));
        }
        if has_grpc {
            header.push_str(&format!(" {:>16}", "gRPC vs Van."));
        }
        header.push('\n');
        out.push_str(&header);

        // Separator
        let sep_len = 18
            + if has_direct { 13 } else { 0 }
            + if has_cdn_fast { 15 } else { 0 }
            + if has_grpc { 13 } else { 0 }
            + 13
            + if has_direct { 17 } else { 0 }
            + if has_cdn_fast { 19 } else { 0 }
            + if has_grpc { 17 } else { 0 };
        out.push_str(&"-".repeat(sep_len));
        out.push('\n');

        for c in &report.comparisons {
            let mut line = format!("{:<18}", c.workload);
            if has_direct {
                line.push_str(&format!(" {:>12}", fmt_p50(c.direct_p50_ms)));
            }
            if has_cdn_fast {
                line.push_str(&format!(" {:>14}", fmt_p50(c.cdn_fast_p50_ms)));
            }
            if has_grpc {
                line.push_str(&format!(" {:>12}", fmt_p50(c.grpc_p50_ms)));
            }
            line.push_str(&format!(" {:>11.1}ms", c.vanilla_p50_ms));
            if has_direct {
                line.push_str(&format!(" {:>16}", fmt_pct(c.direct_vs_vanilla_pct)));
            }
            if has_cdn_fast {
                line.push_str(&format!(" {:>18}", fmt_pct(c.cdn_fast_vs_vanilla_pct)));
            }
            if has_grpc {
                line.push_str(&format!(" {:>16}", fmt_pct(c.grpc_vs_vanilla_pct)));
            }
            line.push('\n');
            out.push_str(&line);
        }
    }

    out
}

/// Format a report as JSON.
pub fn format_json(report: &BenchReport) -> String {
    serde_json::to_string_pretty(report).unwrap_or_else(|_| "{}".to_string())
}
