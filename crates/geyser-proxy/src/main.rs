mod api;
mod auth;
mod bcs_json;
mod grpc;
mod jsonrpc;
mod server;

use geyser_core::analyzer::TxAnalyzer;
use geyser_core::committee::CommitteeManager;
use geyser_core::config::GeyserConfig;
use geyser_core::effects_certifier::EffectsCertifier;
use geyser_core::epoch_monitor::EpochMonitor;
use geyser_core::fullnode_racer::FullnodeRacer;
use geyser_core::gas_pool::GasPool;
use geyser_core::metrics::GeyserMetrics;
use geyser_core::quorum_driver::QuorumDriver;
use geyser_core::stats::StatsCollector;
use geyser_core::tx_submitter::TxSubmitter;
use geyser_core::validator_monitor::ValidatorMonitor;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("geyser=info".parse().unwrap()),
        )
        .init();

    // Load config
    let config = GeyserConfig::load().unwrap_or_else(|e| {
        info!(error = %e, "Using default config");
        GeyserConfig::default()
    });

    // Override with env vars
    let port = std::env::var("GEYSER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(config.server.port);

    let fullnode_endpoints: Vec<String> = std::env::var("GEYSER_ENDPOINTS")
        .ok()
        .map(|s| s.split(',').map(String::from).collect())
        .unwrap_or_else(|| config.engine.fullnode_endpoints.clone());

    info!(
        port,
        mode = ?config.engine.mode,
        fullnodes = fullnode_endpoints.len(),
        "Starting Geyser"
    );

    // Initialize components
    let fullnode_url = fullnode_endpoints
        .first()
        .cloned()
        .unwrap_or_else(|| "https://fullnode.mainnet.sui.io:443".to_string());

    let committee_manager = Arc::new(CommitteeManager::new(fullnode_url.clone()));

    // Fetch initial committee
    info!("Fetching initial committee...");
    if let Err(e) = committee_manager.refresh().await {
        tracing::warn!(error = %e, "Failed to fetch initial committee, will retry");
    }

    let committee = Arc::new(RwLock::new(committee_manager.get()));

    // Validator monitor
    let validator_monitor = Arc::new(ValidatorMonitor::new(
        committee.clone(),
        config.quorum.latency_delta,
        config.health_ping_interval(),
    ));
    validator_monitor.init_from_committee();

    // Probe validators and seed latency data so the first TX picks the closest validators
    info!("Probing validators for latency...");
    validator_monitor.probe_and_seed().await;
    info!("Validator probing complete");

    // TX analyzer
    let analyzer = Arc::new(TxAnalyzer::new());

    // TX submitter
    let submitter = Arc::new(TxSubmitter::new(
        validator_monitor.clone(),
        committee.clone(),
        analyzer.clone(),
        config.submit_timeout(),
        config.initial_retry_delay(),
        config.max_retry_delay(),
    ));

    // Pre-warm gRPC channels to the expected concurrent submit set.
    let prewarm_count = config.quorum.max_amplification.max(3);
    info!(
        prewarm_count,
        "Pre-warming gRPC channels to top validators..."
    );
    submitter.prewarm_channels(prewarm_count).await;
    info!("gRPC channel pre-warm complete");

    // Start health pings
    let _ping_handle = validator_monitor.start_ping_task();

    // Epoch monitor
    let epoch_monitor = Arc::new(EpochMonitor::new(committee_manager.clone()));
    let _epoch_handle = epoch_monitor.start_monitor_task(Duration::from_secs(30));

    // Gas pool
    let gas_pool = Arc::new(GasPool::new(
        config.gas_pool.pool_size,
        config.gas_pool.min_balance_mist,
    ));

    // Stats and metrics
    let stats = Arc::new(StatsCollector::new());
    let metrics = Arc::new(GeyserMetrics::new());

    // Effects certifier
    let certifier = Arc::new(EffectsCertifier::new(
        validator_monitor.clone(),
        committee,
        config.effects_timeout(),
        config.effects_fallback_delay(),
        config.quorum.effects_fallback_fanout,
    ));

    // Quorum driver
    let quorum_driver = Arc::new(QuorumDriver::new(
        submitter,
        certifier,
        analyzer,
        validator_monitor.clone(),
        Some(gas_pool.clone()),
        epoch_monitor,
        stats.clone(),
        metrics.clone(),
        config.quorum.owned_amplification,
        config.quorum.max_amplification,
    ));

    // Fullnode racer (fallback)
    let fullnode_racer = Arc::new(FullnodeRacer::new(
        fullnode_endpoints,
        config.submit_timeout(),
    ));

    info!("All components initialized, starting server");

    // Start server
    server::start_server(
        port,
        quorum_driver,
        validator_monitor,
        fullnode_racer,
        Some(gas_pool),
        stats,
        metrics,
    )
    .await?;

    Ok(())
}
