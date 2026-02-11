use anyhow::{anyhow, bail, Context, Result};
use clap::{ArgGroup, Parser, ValueEnum};
use geyser_core::analyzer::TxAnalyzer;
use geyser_core::committee::CommitteeManager;
use geyser_core::effects_certifier::EffectsCertifier;
use geyser_core::signer;
use geyser_core::tx_submitter::{SubmitStatus, TxSubmitter};
use geyser_core::validator_monitor::ValidatorMonitor;
use parking_lot::RwLock;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

#[derive(Parser)]
#[command(name = "geyser-submit")]
#[command(about = "Submit a signed Sui transaction directly to validators from raw tx bytes")]
#[command(group(
    ArgGroup::new("tx_input")
        .required(true)
        .args(["tx_bytes_b64", "tx_bytes_hex", "tx_bytes_file"])
))]
struct Cli {
    /// JSON-RPC endpoint used to fetch current committee metadata.
    #[arg(long, default_value = "https://fullnode.mainnet.sui.io:443")]
    rpc_url: String,

    /// Transaction bytes as base64.
    #[arg(long)]
    tx_bytes_b64: Option<String>,

    /// Transaction bytes as hex.
    #[arg(long)]
    tx_bytes_hex: Option<String>,

    /// Transaction bytes loaded from a file.
    #[arg(long)]
    tx_bytes_file: Option<PathBuf>,

    /// Encoding used by --tx-bytes-file.
    #[arg(long, value_enum, default_value_t = FileEncoding::Raw)]
    tx_bytes_file_encoding: FileEncoding,

    /// One or more signatures as base64 (repeat flag to pass multiple signatures).
    #[arg(long, action = clap::ArgAction::Append)]
    signature_b64: Vec<String>,

    /// One or more signatures as hex (repeat flag to pass multiple signatures).
    #[arg(long, action = clap::ArgAction::Append)]
    signature_hex: Vec<String>,

    /// Keystore used for signing if no signatures are provided.
    #[arg(long, default_value = "~/.sui/sui_config/sui.keystore")]
    keystore: String,

    /// Sender address used when selecting a key from keystore.
    #[arg(long)]
    sender: Option<String>,

    /// Optional JSON file with transaction kind data for precise owned/shared classification.
    #[arg(long)]
    tx_kind_json_file: Option<PathBuf>,

    /// Force a fixed number of validators to race.
    #[arg(long)]
    amplification: Option<usize>,

    /// Race all validators in the current committee (overrides --max-validators when amplification is not set).
    #[arg(long, default_value_t = false)]
    submit_all: bool,

    /// Max validators for auto amplification mode.
    #[arg(long, default_value_t = 8)]
    max_validators: usize,

    /// Owned-object amplification used when --amplification is not provided.
    #[arg(long, default_value_t = 5)]
    owned_amplification: usize,

    /// Return after SubmitTransaction acceptance only (skip quorum effects certification).
    #[arg(long, default_value_t = false)]
    submit_only: bool,

    /// SubmitTransaction timeout per validator request.
    #[arg(long, default_value_t = 10)]
    submit_timeout_secs: u64,

    /// WaitForEffects timeout.
    #[arg(long, default_value_t = 10)]
    effects_timeout_secs: u64,

    /// Delay before fallback WaitForEffects fan-out.
    #[arg(long, default_value_t = 20)]
    effects_fallback_delay_ms: u64,

    /// Number of validators queried during effects fallback fan-out.
    #[arg(long, default_value_t = 8)]
    effects_fallback_fanout: usize,

    /// Initial retry delay after failed submit.
    #[arg(long, default_value_t = 10)]
    initial_retry_delay_ms: u64,

    /// Maximum retry delay after failed submit.
    #[arg(long, default_value_t = 5)]
    max_retry_delay_secs: u64,

    /// Maximum submit retries.
    #[arg(long, default_value_t = 3)]
    max_retries: usize,

    /// Latency delta for top-tier validator shuffling.
    #[arg(long, default_value_t = 0.02)]
    latency_delta: f64,

    /// Health ping interval for monitor initialization.
    #[arg(long, default_value_t = 10)]
    health_ping_interval_secs: u64,

    /// Skip startup TCP probe of all validators.
    #[arg(long, default_value_t = false)]
    no_probe: bool,

    /// Skip startup gRPC pre-warm.
    #[arg(long, default_value_t = false)]
    no_prewarm: bool,

    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Pretty)]
    output: OutputFormat,
}

#[derive(Clone, Copy, ValueEnum)]
enum FileEncoding {
    Raw,
    Base64,
    Hex,
}

#[derive(Clone, Copy, ValueEnum)]
enum OutputFormat {
    Pretty,
    Json,
}

#[derive(Serialize)]
struct SubmitReport {
    digest_base58: String,
    digest_hex: String,
    tx_path: String,
    committee_size: usize,
    requested_amplification: usize,
    used_amplification: usize,
    submit_only: bool,
    submit_status: String,
    submit_validator: String,
    submit_validator_name: Option<String>,
    submit_latency_ms: u128,
    total_latency_ms: u128,
    effects_latency_ms: Option<u128>,
    certified_effects_digest_hex: Option<String>,
    certified_signatures: Option<usize>,
}

fn expand_tilde(input: &str) -> String {
    input.replace('~', &std::env::var("HOME").unwrap_or_default())
}

fn decode_hex_maybe_prefixed(input: &str) -> Result<Vec<u8>> {
    let raw = input.trim();
    let stripped = raw.strip_prefix("0x").unwrap_or(raw);
    hex::decode(stripped).with_context(|| format!("invalid hex input: {}", input))
}

fn decode_file(path: &PathBuf, encoding: FileEncoding) -> Result<Vec<u8>> {
    match encoding {
        FileEncoding::Raw => std::fs::read(path)
            .with_context(|| format!("failed reading raw tx bytes file: {}", path.display())),
        FileEncoding::Base64 => {
            let content = std::fs::read_to_string(path).with_context(|| {
                format!("failed reading base64 tx bytes file: {}", path.display())
            })?;
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, content.trim())
                .with_context(|| format!("invalid base64 in tx bytes file: {}", path.display()))
        }
        FileEncoding::Hex => {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("failed reading hex tx bytes file: {}", path.display()))?;
            decode_hex_maybe_prefixed(content.trim())
        }
    }
}

fn load_tx_bytes(cli: &Cli) -> Result<Vec<u8>> {
    let tx_bytes = if let Some(v) = &cli.tx_bytes_b64 {
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, v.trim())
            .context("invalid --tx-bytes-b64 value")?
    } else if let Some(v) = &cli.tx_bytes_hex {
        decode_hex_maybe_prefixed(v).context("invalid --tx-bytes-hex value")?
    } else if let Some(path) = &cli.tx_bytes_file {
        decode_file(path, cli.tx_bytes_file_encoding)?
    } else {
        bail!("missing tx bytes input")
    };

    if tx_bytes.is_empty() {
        bail!("tx bytes are empty");
    }
    Ok(tx_bytes)
}

fn load_signatures(cli: &Cli, tx_bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut signatures = Vec::new();

    for sig in &cli.signature_b64 {
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, sig.trim())
            .with_context(|| "invalid --signature-b64 value".to_string())?;
        signatures.push(bytes);
    }

    for sig in &cli.signature_hex {
        signatures
            .push(decode_hex_maybe_prefixed(sig).with_context(|| "invalid --signature-hex value")?);
    }

    if !signatures.is_empty() {
        return Ok(signatures);
    }

    let keystore_path = expand_tilde(&cli.keystore);
    let keypair = if let Some(addr) = &cli.sender {
        signer::load_keypair_for_address(&keystore_path, addr)?
    } else {
        let keypairs = signer::load_keystore(&keystore_path)?;
        keypairs
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("no ed25519 keypairs found in keystore {}", keystore_path))?
    };

    Ok(vec![keypair.sign_transaction(tx_bytes)])
}

fn parse_tx_kind(path: Option<&PathBuf>) -> Result<Option<serde_json::Value>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed reading tx kind file: {}", path.display()))?;
    let value = serde_json::from_str(&content)
        .with_context(|| format!("invalid JSON in tx kind file: {}", path.display()))?;
    Ok(Some(value))
}

fn print_pretty(report: &SubmitReport) {
    println!("digest: {}", report.digest_base58);
    println!("digest_hex: {}", report.digest_hex);
    println!("tx_path: {}", report.tx_path);
    println!(
        "amplification: requested={} used={} committee={}",
        report.requested_amplification, report.used_amplification, report.committee_size
    );
    println!(
        "submit: status={} validator={}{} latency={}ms",
        report.submit_status,
        report.submit_validator,
        report
            .submit_validator_name
            .as_ref()
            .map(|s| format!(" ({})", s))
            .unwrap_or_default(),
        report.submit_latency_ms
    );
    if report.submit_only {
        println!("finality: skipped (--submit-only)");
    } else {
        println!("finality_total: {}ms", report.total_latency_ms);
        if let Some(ms) = report.effects_latency_ms {
            println!("effects_latency: {}ms", ms);
        }
        if let Some(digest) = &report.certified_effects_digest_hex {
            println!("effects_digest: {}", digest);
        }
        if let Some(sigs) = report.certified_signatures {
            println!("effects_signatures: {}", sigs);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("geyser_submit=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    let tx_bytes = load_tx_bytes(&cli)?;
    let signatures = load_signatures(&cli, &tx_bytes)?;
    let tx_kind_json = parse_tx_kind(cli.tx_kind_json_file.as_ref())?;

    let digest_vec = signer::tx_digest_bytes(&tx_bytes);
    if digest_vec.len() != 32 {
        bail!("unexpected tx digest length {}", digest_vec.len());
    }
    let mut tx_digest = [0u8; 32];
    tx_digest.copy_from_slice(&digest_vec[..32]);

    let digest_base58 = signer::tx_digest(&tx_bytes);
    let digest_hex = hex::encode(tx_digest);

    info!(digest = %digest_base58, "Fetching latest committee");
    let committee_manager = Arc::new(CommitteeManager::new(cli.rpc_url.clone()));
    committee_manager.refresh().await?;
    let committee = Arc::new(RwLock::new(committee_manager.get()));
    let committee_size = committee.read().validators.len();
    if committee_size == 0 {
        bail!("committee is empty");
    }

    let analyzer = Arc::new(TxAnalyzer::new());
    let tx_path = tx_kind_json
        .as_ref()
        .map(|json| analyzer.classify_from_tx_kind(json))
        .unwrap_or_else(|| analyzer.classify_heuristic(&tx_bytes));

    let requested_amplification = if let Some(explicit) = cli.amplification {
        explicit
    } else if cli.submit_all {
        committee_size
    } else {
        match tx_path {
            geyser_core::analyzer::TxPath::OwnedOnly => cli.owned_amplification.max(1),
            geyser_core::analyzer::TxPath::Shared => cli.max_validators.max(1),
        }
    };
    let used_amplification = requested_amplification.clamp(1, committee_size);

    let validator_monitor = Arc::new(ValidatorMonitor::new(
        committee.clone(),
        cli.latency_delta,
        Duration::from_secs(cli.health_ping_interval_secs),
    ));
    validator_monitor.init_from_committee();

    if !cli.no_probe {
        info!("Probing validators for initial latency seed");
        validator_monitor.probe_and_seed().await;
    }

    let submitter = Arc::new(TxSubmitter::new(
        validator_monitor.clone(),
        committee.clone(),
        analyzer.clone(),
        Duration::from_secs(cli.submit_timeout_secs),
        Duration::from_millis(cli.initial_retry_delay_ms),
        Duration::from_secs(cli.max_retry_delay_secs),
    ));

    if !cli.no_prewarm {
        let prewarm_count = if cli.submit_all {
            committee_size
        } else {
            used_amplification.max(3)
        };
        info!(prewarm_count, "Pre-warming validator channels");
        submitter.prewarm_channels(prewarm_count).await;
    }

    let started = Instant::now();
    let submit_result = submitter
        .submit_with_retry(
            &tx_bytes,
            &signatures,
            &tx_digest,
            used_amplification,
            cli.max_retries,
        )
        .await?;
    let submit_latency_ms = submit_result.latency.as_millis();

    let validator = committee
        .read()
        .get_validator(&submit_result.validator)
        .cloned();

    let submit_status = match submit_result.status {
        SubmitStatus::Executed => "executed".to_string(),
        SubmitStatus::Submitted => "submitted".to_string(),
    };

    let report = if cli.submit_only {
        SubmitReport {
            digest_base58,
            digest_hex,
            tx_path: tx_path.to_string(),
            committee_size,
            requested_amplification,
            used_amplification,
            submit_only: true,
            submit_status,
            submit_validator: hex::encode(submit_result.validator),
            submit_validator_name: validator.map(|v| v.description),
            submit_latency_ms,
            total_latency_ms: started.elapsed().as_millis(),
            effects_latency_ms: None,
            certified_effects_digest_hex: None,
            certified_signatures: None,
        }
    } else {
        let certifier = EffectsCertifier::new(
            validator_monitor,
            committee,
            Duration::from_secs(cli.effects_timeout_secs),
            Duration::from_millis(cli.effects_fallback_delay_ms),
            cli.effects_fallback_fanout.max(1),
        );
        let certified = certifier.certify(&tx_digest, &submit_result).await?;
        SubmitReport {
            digest_base58,
            digest_hex,
            tx_path: tx_path.to_string(),
            committee_size,
            requested_amplification,
            used_amplification,
            submit_only: false,
            submit_status,
            submit_validator: hex::encode(submit_result.validator),
            submit_validator_name: validator.map(|v| v.description),
            submit_latency_ms,
            total_latency_ms: started.elapsed().as_millis(),
            effects_latency_ms: Some(certified.latency.as_millis()),
            certified_effects_digest_hex: Some(hex::encode(certified.effects_digest)),
            certified_signatures: Some(certified.signatures.len()),
        }
    };

    match cli.output {
        OutputFormat::Pretty => print_pretty(&report),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
    }

    Ok(())
}
