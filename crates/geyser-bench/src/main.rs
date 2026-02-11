mod report;
mod runner;
mod workloads;

use clap::Parser;
use geyser_core::analyzer::TxAnalyzer;
use geyser_core::committee::CommitteeManager;
use geyser_core::effects_certifier::EffectsCertifier;
use geyser_core::epoch_monitor::EpochMonitor;
use geyser_core::fullnode_racer::FullnodeRacer;
use geyser_core::metrics::GeyserMetrics;
use geyser_core::ptb::PtbBuilder;
use geyser_core::quorum_driver::QuorumDriver;
use geyser_core::signer;
use geyser_core::stats::StatsCollector;
use geyser_core::tx_submitter::TxSubmitter;
use geyser_core::validator_monitor::ValidatorMonitor;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use workloads::counter::CounterWorkload;
use workloads::token::TokenWorkload;
use workloads::transfer::TransferWorkload;
use workloads::SubmitMode;
use workloads::Workload;

#[derive(Parser)]
#[command(name = "geyser-bench")]
#[command(
    about = "Benchmark direct validator vs Geyser gRPC vs vanilla JSON-RPC Sui transaction latency"
)]
struct Cli {
    /// gRPC V2 endpoints for Geyser path (comma-separated).
    #[arg(long, default_value = "http://localhost:9002")]
    grpc_endpoints: String,

    /// JSON-RPC endpoint for vanilla path + PTB building.
    #[arg(long, default_value = "https://fullnode.mainnet.sui.io:443")]
    rpc_url: String,

    /// Deployed geyser package ID.
    #[arg(long)]
    package_id: Option<String>,

    /// Shared counter object ID (for counter workload).
    #[arg(long)]
    counter_id: Option<String>,

    /// Sui keystore path.
    #[arg(long, default_value = "~/.sui/sui_config/sui.keystore")]
    keystore: String,

    /// Sender address (default: first ed25519 key in keystore).
    #[arg(long)]
    sender: Option<String>,

    /// Number of measured iterations per test.
    #[arg(long, default_value = "20")]
    iterations: usize,

    /// Number of warmup iterations.
    #[arg(long, default_value = "3")]
    warmup: usize,

    /// Delay between transactions in milliseconds.
    #[arg(long, default_value = "1000")]
    delay_ms: u64,

    /// Workload type: transfer | counter | mint | all.
    #[arg(long, default_value = "all")]
    workload: String,

    /// Maximum validators to race across for direct path.
    #[arg(long, default_value = "8")]
    max_validators: usize,

    /// Race all validators in the current committee for direct mode.
    #[arg(long, default_value_t = false)]
    submit_all: bool,

    /// Owned-object amplification for direct mode.
    #[arg(long, default_value = "5")]
    owned_amplification: usize,

    /// Submit timeout in seconds for direct mode.
    #[arg(long, default_value = "10")]
    submit_timeout_secs: u64,

    /// Effects timeout in seconds for direct mode.
    #[arg(long, default_value = "10")]
    effects_timeout_secs: u64,

    /// Initial retry delay in milliseconds for direct mode.
    #[arg(long, default_value = "10")]
    initial_retry_delay_ms: u64,

    /// Maximum retry delay in seconds for direct mode.
    #[arg(long, default_value = "5")]
    max_retry_delay_secs: u64,

    /// Effects fallback delay in milliseconds for direct mode.
    #[arg(long, default_value = "20")]
    effects_fallback_delay_ms: u64,

    /// Number of validators queried during effects fallback fan-out.
    #[arg(long, default_value = "8")]
    effects_fallback_fanout: usize,

    /// Latency delta for validator selection in direct mode.
    #[arg(long, default_value = "0.02")]
    latency_delta: f64,

    /// Submission mode filter: direct | grpc | vanilla | cdn-fast | cdn-per-server | all.
    #[arg(long, default_value = "all")]
    mode: String,

    /// CDN endpoint URLs for submission, comma-separated (races all in parallel).
    /// Supports optional labels: "sjc=http://1.2.3.4:8080,nrt=http://5.6.7.8:8080"
    /// Without labels, labels are inferred from hostnames when possible.
    #[arg(long)]
    cdn_url: Option<String>,

    /// Output format: table | json.
    #[arg(long, default_value = "table")]
    output: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("geyser_bench=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    // Expand ~ in keystore path
    let keystore_path = cli
        .keystore
        .replace('~', &std::env::var("HOME").unwrap_or_default());

    // Load keypair
    let keypairs = signer::load_keystore(&keystore_path)?;
    if keypairs.is_empty() {
        anyhow::bail!("No ed25519 keypairs found in keystore at {}", keystore_path);
    }

    let keypair = if let Some(addr) = &cli.sender {
        signer::load_keypair_for_address(&keystore_path, addr)?
    } else {
        keypairs[0].clone()
    };

    let sender = keypair.address();
    info!(sender = sender.as_str(), "Using sender address");

    // Parse gRPC endpoints
    let grpc_endpoints: Vec<String> = cli
        .grpc_endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Fetch validator committee for direct submission endpoints
    info!("Fetching validator committee for direct submission endpoints...");
    let committee_mgr = Arc::new(CommitteeManager::new(cli.rpc_url.clone()));
    let mut max_validators = cli.max_validators.max(1);
    let mut owned_amplification = cli.owned_amplification.max(1);

    let (has_validators, quorum_driver) = match committee_mgr.refresh().await {
        Ok(_) => {
            let committee = Arc::new(RwLock::new(committee_mgr.get()));
            let committee_size = committee.read().validators.len().max(1);
            if cli.submit_all {
                max_validators = committee_size;
            } else {
                max_validators = max_validators.min(committee_size);
            }
            owned_amplification = owned_amplification.min(max_validators).max(1);

            // Validator monitor: latency-aware selection with EMA tracking
            let validator_monitor = Arc::new(ValidatorMonitor::new(
                committee.clone(),
                cli.latency_delta,
                Duration::from_secs(10),
            ));
            validator_monitor.init_from_committee();

            info!("Probing validators and seeding latency data...");
            validator_monitor.probe_and_seed().await;

            // TX analyzer for owned/shared classification
            let analyzer = Arc::new(TxAnalyzer::new());

            // TX submitter with retry
            let submitter = Arc::new(TxSubmitter::new(
                validator_monitor.clone(),
                committee.clone(),
                analyzer.clone(),
                Duration::from_secs(cli.submit_timeout_secs),
                Duration::from_millis(cli.initial_retry_delay_ms),
                Duration::from_secs(cli.max_retry_delay_secs),
            ));

            info!(
                "Pre-warming gRPC channels to top {} validators...",
                max_validators
            );
            submitter.prewarm_channels(max_validators).await;

            // Epoch monitor
            let epoch_monitor = Arc::new(EpochMonitor::new(committee_mgr.clone()));
            let _epoch_handle = epoch_monitor.start_monitor_task(Duration::from_secs(30));

            // Stats and metrics
            let stats = Arc::new(StatsCollector::new());
            let metrics = Arc::new(GeyserMetrics::new());

            // Effects certifier
            let certifier = Arc::new(EffectsCertifier::new(
                validator_monitor.clone(),
                committee,
                Duration::from_secs(cli.effects_timeout_secs),
                Duration::from_millis(cli.effects_fallback_delay_ms),
                cli.effects_fallback_fanout.max(1),
            ));

            // QuorumDriver: the production pipeline
            let qd = Arc::new(QuorumDriver::new(
                submitter,
                certifier,
                analyzer,
                validator_monitor,
                None, // no gas pool for bench
                epoch_monitor,
                stats,
                metrics,
                owned_amplification,
                max_validators,
            ));

            info!("QuorumDriver initialized for direct mode");
            (true, Some(qd))
        }
        Err(e) => {
            warn!(error = %e, "Failed to fetch committee, direct validator path disabled");
            (false, None)
        }
    };

    // Create shared components
    let ptb_builder = Arc::new(PtbBuilder::new(cli.rpc_url.clone()));

    // Racer for geyser (gRPC fullnode) path
    let grpc_racer = Arc::new(FullnodeRacer::with_grpc(
        vec![cli.rpc_url.clone()],
        grpc_endpoints.clone(),
        Duration::from_secs(30),
    ));

    // Racer for vanilla (JSON-RPC) path
    let vanilla_racer = Arc::new(FullnodeRacer::new(
        vec![cli.rpc_url.clone()],
        Duration::from_secs(30),
    ));

    let package_id = cli.package_id.unwrap_or_default();
    let counter_id = cli.counter_id.unwrap_or_default();

    let config = runner::RunConfig {
        iterations: cli.iterations,
        warmup: cli.warmup,
        delay: Duration::from_millis(cli.delay_ms),
    };

    info!(
        grpc_endpoints = ?grpc_endpoints,
        has_validators,
        max_validators,
        owned_amplification,
        rpc_url = cli.rpc_url,
        workload = cli.workload,
        iterations = cli.iterations,
        sender = sender.as_str(),
        "Starting benchmark"
    );

    let mut workloads: Vec<(&str, Box<dyn Workload>)> = Vec::new();
    let should_run = |name: &str| cli.workload == "all" || cli.workload == name;

    // Parse CDN URLs, supporting optional labels: "sjc=http://...,nrt=http://..."
    let cdn_entries: Vec<(String, String)> = cli
        .cdn_url
        .as_ref()
        .map(|s| {
            s.split(',')
                .filter_map(|entry| {
                    let entry = entry.trim();
                    if entry.is_empty() {
                        return None;
                    }
                    if let Some((label, url)) = entry.split_once('=') {
                        Some((label.trim().to_string(), url.trim().to_string()))
                    } else {
                        Some((extract_cdn_label(entry).to_string(), entry.to_string()))
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let cdn_urls: Vec<String> = cdn_entries.iter().map(|(_, url)| url.clone()).collect();
    let cdn_labels: Vec<String> = cdn_entries.iter().map(|(label, _)| label.clone()).collect();

    let mode_filter = |label: &str| -> bool {
        match cli.mode.as_str() {
            "direct" => label.starts_with("direct-"),
            "grpc" => label.starts_with("grpc-"),
            "vanilla" => label.starts_with("vanilla-"),
            "cdn-fast" => label.starts_with("cdn-fast-"),
            "cdn-per-server" => label.starts_with("cdn-per-"),
            "all" => !label.starts_with("cdn-per-"),
            _ => true,
        }
    };

    if should_run("transfer") {
        // Direct validator path (via QuorumDriver)
        if let Some(ref qd) = quorum_driver {
            workloads.push((
                "direct-transfer",
                Box::new(
                    TransferWorkload::new(
                        ptb_builder.clone(),
                        vanilla_racer.clone(),
                        keypair.clone(),
                        sender.clone(),
                        SubmitMode::DirectValidator,
                        max_validators,
                    )
                    .with_quorum_driver(qd.clone()),
                ),
            ));
        }
        // CDN fast mode (raw BCS)
        if !cdn_urls.is_empty() {
            workloads.push((
                "cdn-fast-transfer",
                Box::new(
                    TransferWorkload::new(
                        ptb_builder.clone(),
                        vanilla_racer.clone(),
                        keypair.clone(),
                        sender.clone(),
                        SubmitMode::CdnFast,
                        max_validators,
                    )
                    .with_cdn_urls(cdn_urls.clone())
                    .with_cdn_labels(cdn_labels.clone()),
                ),
            ));
        }
        // CDN per-server mode (one workload per server)
        if cdn_entries.len() > 1 {
            for (label, url) in &cdn_entries {
                let name = format!("cdn-per-{}-transfer", label);
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                workloads.push((
                    leaked,
                    Box::new(
                        TransferWorkload::new(
                            ptb_builder.clone(),
                            vanilla_racer.clone(),
                            keypair.clone(),
                            sender.clone(),
                            SubmitMode::CdnPerServer,
                            max_validators,
                        )
                        .with_cdn_urls(vec![url.clone()])
                        .with_cdn_labels(vec![label.clone()])
                        .with_name_override(name),
                    ),
                ));
            }
        }
        // gRPC fullnode path
        workloads.push((
            "grpc-transfer",
            Box::new(TransferWorkload::new(
                ptb_builder.clone(),
                grpc_racer.clone(),
                keypair.clone(),
                sender.clone(),
                SubmitMode::GrpcFullnode,
                max_validators,
            )),
        ));
        // Vanilla JSON-RPC path
        workloads.push((
            "vanilla-transfer",
            Box::new(TransferWorkload::new(
                ptb_builder.clone(),
                vanilla_racer.clone(),
                keypair.clone(),
                sender.clone(),
                SubmitMode::JsonRpc,
                max_validators,
            )),
        ));
    }

    if should_run("counter") && !package_id.is_empty() && !counter_id.is_empty() {
        if let Some(ref qd) = quorum_driver {
            workloads.push((
                "direct-counter",
                Box::new(
                    CounterWorkload::new(
                        ptb_builder.clone(),
                        vanilla_racer.clone(),
                        keypair.clone(),
                        sender.clone(),
                        package_id.clone(),
                        counter_id.clone(),
                        SubmitMode::DirectValidator,
                        max_validators,
                    )
                    .with_quorum_driver(qd.clone()),
                ),
            ));
        }
        if !cdn_urls.is_empty() {
            workloads.push((
                "cdn-fast-counter",
                Box::new(
                    CounterWorkload::new(
                        ptb_builder.clone(),
                        vanilla_racer.clone(),
                        keypair.clone(),
                        sender.clone(),
                        package_id.clone(),
                        counter_id.clone(),
                        SubmitMode::CdnFast,
                        max_validators,
                    )
                    .with_cdn_urls(cdn_urls.clone())
                    .with_cdn_labels(cdn_labels.clone()),
                ),
            ));
        }
        // CDN per-server mode (one workload per server)
        if cdn_entries.len() > 1 {
            for (label, url) in &cdn_entries {
                let name = format!("cdn-per-{}-counter", label);
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                workloads.push((
                    leaked,
                    Box::new(
                        CounterWorkload::new(
                            ptb_builder.clone(),
                            vanilla_racer.clone(),
                            keypair.clone(),
                            sender.clone(),
                            package_id.clone(),
                            counter_id.clone(),
                            SubmitMode::CdnPerServer,
                            max_validators,
                        )
                        .with_cdn_urls(vec![url.clone()])
                        .with_cdn_labels(vec![label.clone()])
                        .with_name_override(name),
                    ),
                ));
            }
        }
        workloads.push((
            "grpc-counter",
            Box::new(CounterWorkload::new(
                ptb_builder.clone(),
                grpc_racer.clone(),
                keypair.clone(),
                sender.clone(),
                package_id.clone(),
                counter_id.clone(),
                SubmitMode::GrpcFullnode,
                max_validators,
            )),
        ));
        workloads.push((
            "vanilla-counter",
            Box::new(CounterWorkload::new(
                ptb_builder.clone(),
                vanilla_racer.clone(),
                keypair.clone(),
                sender.clone(),
                package_id.clone(),
                counter_id.clone(),
                SubmitMode::JsonRpc,
                max_validators,
            )),
        ));
    }

    if should_run("mint") && !package_id.is_empty() {
        if let Some(ref qd) = quorum_driver {
            workloads.push((
                "direct-mint",
                Box::new(
                    TokenWorkload::new(
                        ptb_builder.clone(),
                        vanilla_racer.clone(),
                        keypair.clone(),
                        sender.clone(),
                        package_id.clone(),
                        SubmitMode::DirectValidator,
                        max_validators,
                    )
                    .with_quorum_driver(qd.clone()),
                ),
            ));
        }
        if !cdn_urls.is_empty() {
            workloads.push((
                "cdn-fast-mint",
                Box::new(
                    TokenWorkload::new(
                        ptb_builder.clone(),
                        vanilla_racer.clone(),
                        keypair.clone(),
                        sender.clone(),
                        package_id.clone(),
                        SubmitMode::CdnFast,
                        max_validators,
                    )
                    .with_cdn_urls(cdn_urls.clone())
                    .with_cdn_labels(cdn_labels.clone()),
                ),
            ));
        }
        // CDN per-server mode (one workload per server)
        if cdn_entries.len() > 1 {
            for (label, url) in &cdn_entries {
                let name = format!("cdn-per-{}-mint", label);
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                workloads.push((
                    leaked,
                    Box::new(
                        TokenWorkload::new(
                            ptb_builder.clone(),
                            vanilla_racer.clone(),
                            keypair.clone(),
                            sender.clone(),
                            package_id.clone(),
                            SubmitMode::CdnPerServer,
                            max_validators,
                        )
                        .with_cdn_urls(vec![url.clone()])
                        .with_cdn_labels(vec![label.clone()])
                        .with_name_override(name),
                    ),
                ));
            }
        }
        workloads.push((
            "grpc-mint",
            Box::new(TokenWorkload::new(
                ptb_builder.clone(),
                grpc_racer.clone(),
                keypair.clone(),
                sender.clone(),
                package_id.clone(),
                SubmitMode::GrpcFullnode,
                max_validators,
            )),
        ));
        workloads.push((
            "vanilla-mint",
            Box::new(TokenWorkload::new(
                ptb_builder.clone(),
                vanilla_racer.clone(),
                keypair.clone(),
                sender.clone(),
                package_id.clone(),
                SubmitMode::JsonRpc,
                max_validators,
            )),
        ));
    }

    // Apply mode filter (--mode direct | geyser | vanilla | all)
    let workloads: Vec<(&str, Box<dyn Workload>)> = workloads
        .into_iter()
        .filter(|(label, _)| mode_filter(label))
        .collect();

    if workloads.is_empty() {
        println!("No workloads selected. Use --workload and provide required IDs.");
        println!("  --workload transfer                                (no extra args needed)");
        println!("  --workload counter  --package-id X --counter-id Y");
        println!("  --workload mint     --package-id X");
        println!("  --workload all      --package-id X --counter-id Y");
        println!(
            "  --mode direct|grpc|vanilla|cdn-fast|cdn-per-server|all  (filter submission path)"
        );
        println!("  --cdn-url https://txn.yourdomain.com                       (enable CDN paths)");
        println!(
            "  --cdn-url \"sjc=http://1.2.3.4:8080,nrt=http://5.6.7.8:8080\" (per-server labels)"
        );
        return Ok(());
    }

    let bench_report = runner::run_all(workloads, &config).await;

    match cli.output.as_str() {
        "json" => println!("{}", report::format_json(&bench_report)),
        _ => println!("{}", report::format_table(&bench_report)),
    }

    Ok(())
}

/// Extract a human-readable label from a CDN server URL.
/// Uses hostname prefix when available; otherwise returns full host/IP.
fn extract_cdn_label(url: &str) -> &str {
    let host = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or(url)
        .split(':')
        .next()
        .unwrap_or(url);

    let is_ipv4_like = host.chars().all(|c| c.is_ascii_digit() || c == '.');

    if is_ipv4_like {
        host
    } else {
        host.split('.').next().unwrap_or(host)
    }
}
