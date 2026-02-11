pub mod counter;
pub mod token;
pub mod transfer;

use anyhow::{anyhow, Result};
use base64::Engine;
use geyser_core::sui_types::{decode_effects, decode_events};
use serde_json::Value;
use std::time::Duration;

/// How a benchmark workload submits transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitMode {
    /// Race across direct validator gRPC endpoints (port 8080).
    DirectValidator,
    /// gRPC V2 to a single fullnode.
    GrpcFullnode,
    /// JSON-RPC to fullnode (vanilla baseline).
    JsonRpc,
    /// JSON-RPC via Geyser CDN geo-router (fast mode, raw BCS).
    CdnFast,
    /// JSON-RPC via a single CDN server (fast mode) — for per-server comparison.
    CdnPerServer,
}

/// Per-iteration latency breakdown for benchmark reporting.
#[derive(Debug, Clone)]
pub struct LatencySample {
    /// End-to-end latency for what the caller experiences.
    pub total: Duration,
    /// Time spent until the submission response is received.
    pub submit: Duration,
    /// Time spent decoding/validating BCS payloads.
    pub decode: Duration,
    /// True if SubmitTransaction returned Executed directly (1 RTT, no WaitForEffects).
    pub was_inline_executed: bool,
    /// True if result came from speculative WaitForEffects fan-out.
    pub was_speculative: bool,
    /// Server-reported total latency (from CDN `geyser.latencyMs`).
    pub server_total_ms: Option<u64>,
    /// Server-reported submit phase latency (from CDN `geyser.submitMs`).
    pub server_submit_ms: Option<u64>,
    /// Server-reported effects phase latency (from CDN `geyser.effectsMs`).
    pub server_effects_ms: Option<u64>,
    /// Per-server timing from a detailed CDN race (url, elapsed, server_submit_ms, server_effects_ms, is_winner).
    pub race_details: Option<Vec<RaceServerDetail>>,
}

/// Per-server detail from a CDN race iteration.
#[derive(Debug, Clone)]
pub struct RaceServerDetail {
    pub url: String,
    pub label: String,
    pub elapsed: std::time::Duration,
    pub server_submit_ms: Option<u64>,
    pub server_effects_ms: Option<u64>,
    pub is_winner: bool,
}

impl LatencySample {
    pub fn new(total: Duration, submit: Duration, decode: Duration) -> Self {
        Self {
            total,
            submit,
            decode,
            was_inline_executed: false,
            was_speculative: false,
            server_total_ms: None,
            server_submit_ms: None,
            server_effects_ms: None,
            race_details: None,
        }
    }

    pub fn with_diagnostics(
        total: Duration,
        submit: Duration,
        decode: Duration,
        was_inline_executed: bool,
        was_speculative: bool,
    ) -> Self {
        Self {
            total,
            submit,
            decode,
            was_inline_executed,
            was_speculative,
            server_total_ms: None,
            server_submit_ms: None,
            server_effects_ms: None,
            race_details: None,
        }
    }

    pub fn with_server_timing(mut self, total: u64, submit: u64, effects: u64) -> Self {
        self.server_total_ms = Some(total);
        self.server_submit_ms = Some(submit);
        self.server_effects_ms = Some(effects);
        self
    }

    pub fn with_race_details(mut self, details: Vec<RaceServerDetail>) -> Self {
        self.race_details = Some(details);
        self
    }
}

/// Extract server-side timing from a CDN fast-mode response's `geyser` object.
/// Returns `(server_total_ms, server_submit_ms, server_effects_ms)`.
pub(crate) fn parse_cdn_server_timing(response: &Value) -> (Option<u64>, Option<u64>, Option<u64>) {
    let geyser = match response.get("geyser") {
        Some(g) => g,
        None => return (None, None, None),
    };
    let total = geyser.get("latencyMs").and_then(|v| v.as_u64());
    let submit = geyser.get("submitMs").and_then(|v| v.as_u64());
    let effects = geyser.get("effectsMs").and_then(|v| v.as_u64());
    (total, submit, effects)
}

/// Default headers for aggressive low-latency CDN fast mode.
/// These are request-scoped overrides consumed by geyser-proxy.
pub(crate) fn cdn_fast_headers() -> Vec<(String, String)> {
    vec![
        ("X-Geyser-Fast".into(), "true".into()),
        ("X-Geyser-Max-Amplification".into(), "124".into()),
        ("X-Geyser-Owned-Amplification".into(), "20".into()),
        ("X-Geyser-Effects-Fallback-Fanout".into(), "120".into()),
        ("X-Geyser-Effects-Fallback-Delay-Ms".into(), "20".into()),
        ("X-Geyser-Submit-Timeout-Ms".into(), "10000".into()),
        ("X-Geyser-Effects-Timeout-Ms".into(), "10000".into()),
        ("X-Geyser-Initial-Retry-Delay-Ms".into(), "10".into()),
        ("X-Geyser-Max-Retry-Delay-Ms".into(), "5000".into()),
        ("X-Geyser-Max-Retries".into(), "3".into()),
    ]
}

/// Decode BCS effects/events to ensure benchmarks include real decode cost.
pub(crate) fn decode_bcs_payload(effects_bcs: &[u8], events_bcs: Option<&[u8]>) -> Result<()> {
    decode_effects(effects_bcs).map_err(|e| anyhow!("Failed to decode effects BCS: {}", e))?;
    if let Some(events) = events_bcs {
        decode_events(events).map_err(|e| anyhow!("Failed to decode events BCS: {}", e))?;
    }
    Ok(())
}

/// Decode `X-Geyser-Fast: true` JSON-RPC response payloads.
pub(crate) fn decode_cdn_fast_response(result: &Value) -> Result<()> {
    let effects_b64 = result
        .get("effects")
        .and_then(|v| v.get("bcs"))
        .and_then(|v| v.as_str());
    if effects_b64.is_none() {
        // Some endpoints ignore X-Geyser-Fast and return already-decoded JSON.
        // Treat that as a successful no-op decode so benchmarking can proceed.
        if result.get("effects").is_some() {
            return Ok(());
        }
        return Err(anyhow!("CDN fast response missing effects payload"));
    }
    let effects_b64 = effects_b64.unwrap();
    let effects_bcs = base64::engine::general_purpose::STANDARD
        .decode(effects_b64)
        .map_err(|e| anyhow!("Invalid base64 in effects.bcs: {}", e))?;

    let events_bcs = result
        .get("events")
        .and_then(|v| v.get("bcs"))
        .and_then(|v| v.as_str())
        .map(|b64| {
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| anyhow!("Invalid base64 in events.bcs: {}", e))
        })
        .transpose()?;

    decode_bcs_payload(&effects_bcs, events_bcs.as_deref())
}

/// A benchmark workload that produces transactions.
pub trait Workload: Send + Sync {
    /// Name of the workload.
    fn name(&self) -> &str;

    /// Description for the report.
    fn description(&self) -> &str;

    /// Execute one iteration of the workload.
    /// Returns the latency of the transaction.
    fn execute(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<LatencySample>> + Send + '_>>;
}
