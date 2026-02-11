use crate::api::{self, ApiState};
use crate::auth::{validate_api_key, ApiKeyConfig};
use crate::grpc::GeyserGrpcService;
use crate::jsonrpc::{self, JsonRpcState};
use axum::middleware;
use axum::routing::{get, post};
use axum::Router;
use geyser_core::fullnode_racer::FullnodeRacer;
use geyser_core::gas_pool::GasPool;
use geyser_core::metrics::GeyserMetrics;
use geyser_core::quorum_driver::QuorumDriver;
use geyser_core::stats::StatsCollector;
use geyser_core::validator_monitor::ValidatorMonitor;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

/// Build and start the unified HTTP server (REST + JSON-RPC).
pub async fn start_server(
    port: u16,
    quorum_driver: Arc<QuorumDriver>,
    validator_monitor: Arc<ValidatorMonitor>,
    fullnode_racer: Arc<FullnodeRacer>,
    gas_pool: Option<Arc<GasPool>>,
    stats: Arc<StatsCollector>,
    metrics: Arc<GeyserMetrics>,
) -> anyhow::Result<()> {
    let grpc_service = Arc::new(GeyserGrpcService::new(
        quorum_driver,
        fullnode_racer.clone(),
    ));

    let jsonrpc_state = Arc::new(JsonRpcState {
        grpc_service,
        fullnode_racer: fullnode_racer.clone(),
    });

    let api_state = Arc::new(ApiState {
        validator_monitor,
        fullnode_racer,
        gas_pool,
        stats,
        metrics,
    });

    // Load API key config from env
    let api_keys: Vec<String> = std::env::var("GEYSER_API_KEYS")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.split(',').map(|k| k.trim().to_string()).collect())
        .unwrap_or_default();

    if api_keys.is_empty() {
        info!("API key auth disabled (GEYSER_API_KEYS not set)");
    } else {
        info!(count = api_keys.len(), "API key auth enabled");
    }

    let api_key_config = Arc::new(ApiKeyConfig { keys: api_keys });

    // REST routes with ApiState (no auth — health/stats/metrics stay open)
    let rest_routes = Router::new()
        .route("/health", get(api::health))
        .route("/stats", get(api::stats))
        .route("/metrics", get(api::metrics))
        .with_state(api_state);

    // JSON-RPC route with auth middleware
    let config = api_key_config.clone();
    let jsonrpc_routes = Router::new()
        .route("/", post(jsonrpc::handle_jsonrpc))
        .layer(middleware::from_fn(move |req, next| {
            validate_api_key(config.clone(), req, next)
        }))
        .with_state(jsonrpc_state);

    // Merge into a single router
    let app = rest_routes.merge(jsonrpc_routes);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!(%addr, "Geyser proxy server starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
