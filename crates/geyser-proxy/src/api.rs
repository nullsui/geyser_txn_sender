use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use geyser_core::fullnode_racer::FullnodeRacer;
use geyser_core::gas_pool::GasPool;
use geyser_core::metrics::GeyserMetrics;
use geyser_core::stats::StatsCollector;
use geyser_core::validator_monitor::ValidatorMonitor;
use std::sync::Arc;

/// Shared state for REST API handlers.
pub struct ApiState {
    pub validator_monitor: Arc<ValidatorMonitor>,
    pub fullnode_racer: Arc<FullnodeRacer>,
    pub gas_pool: Option<Arc<GasPool>>,
    pub stats: Arc<StatsCollector>,
    pub metrics: Arc<GeyserMetrics>,
}

/// GET /health — Validator health table.
pub async fn health(State(state): State<Arc<ApiState>>) -> Json<serde_json::Value> {
    let validator_health = state.validator_monitor.health_summary();
    let fullnode_health = state.fullnode_racer.health_summary();

    Json(serde_json::json!({
        "status": "ok",
        "validators": validator_health,
        "fullnodes": fullnode_health,
        "gas_pool": state.gas_pool.as_ref().map(|gp| serde_json::json!({
            "available": gp.available_count(),
            "in_use": gp.in_use_count(),
            "target": gp.target_size(),
        })),
    }))
}

/// GET /stats — Latency percentiles.
pub async fn stats(State(state): State<Arc<ApiState>>) -> Json<serde_json::Value> {
    let snapshot = state.stats.snapshot();
    Json(serde_json::json!({
        "total_transactions": snapshot.total_count,
        "owned_path": {
            "count": snapshot.owned.count,
            "p50_ms": snapshot.owned.p50_us as f64 / 1000.0,
            "p95_ms": snapshot.owned.p95_us as f64 / 1000.0,
            "p99_ms": snapshot.owned.p99_us as f64 / 1000.0,
            "min_ms": snapshot.owned.min_us as f64 / 1000.0,
            "max_ms": snapshot.owned.max_us as f64 / 1000.0,
            "avg_ms": snapshot.owned.mean_us as f64 / 1000.0,
        },
        "shared_path": {
            "count": snapshot.shared.count,
            "p50_ms": snapshot.shared.p50_us as f64 / 1000.0,
            "p95_ms": snapshot.shared.p95_us as f64 / 1000.0,
            "p99_ms": snapshot.shared.p99_us as f64 / 1000.0,
            "min_ms": snapshot.shared.min_us as f64 / 1000.0,
            "max_ms": snapshot.shared.max_us as f64 / 1000.0,
            "avg_ms": snapshot.shared.mean_us as f64 / 1000.0,
        },
        "all": {
            "count": snapshot.all.count,
            "p50_ms": snapshot.all.p50_us as f64 / 1000.0,
            "p95_ms": snapshot.all.p95_us as f64 / 1000.0,
            "p99_ms": snapshot.all.p99_us as f64 / 1000.0,
            "min_ms": snapshot.all.min_us as f64 / 1000.0,
            "max_ms": snapshot.all.max_us as f64 / 1000.0,
            "avg_ms": snapshot.all.mean_us as f64 / 1000.0,
        },
    }))
}

/// GET /metrics — Prometheus metrics.
pub async fn metrics(State(state): State<Arc<ApiState>>) -> (StatusCode, String) {
    // Update gas pool gauges
    if let Some(gp) = &state.gas_pool {
        state
            .metrics
            .update_gas_pool(gp.available_count(), gp.in_use_count());
    }

    (StatusCode::OK, state.metrics.encode())
}
