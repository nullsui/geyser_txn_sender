mod geolocate;
mod report;
mod resolve;

use anyhow::Result;
use clap::Parser;
use geyser_core::committee::{parse_multiaddr, CommitteeManager};
use geyser_core::fullnode_racer::FullnodeRacer;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "geyser-map", about = "Map Sui validator datacenter locations")]
struct Args {
    /// Sui fullnode RPC URL
    #[arg(long, default_value = "https://fullnode.mainnet.sui.io:443")]
    rpc_url: String,

    /// TCP-probe validators for latency from this machine
    #[arg(long)]
    probe: bool,

    /// Output format: table or json
    #[arg(long, default_value = "table")]
    output: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "geyser_map=info,warn".into()),
        )
        .init();

    let args = Args::parse();

    // 1. Fetch validator committee
    tracing::info!("Fetching validator committee from {}", args.rpc_url);
    let cm = CommitteeManager::new(args.rpc_url);
    cm.refresh().await?;
    let committee = cm.get();

    tracing::info!(
        "Got {} validators (epoch {}), total stake: {}",
        committee.validators.len(),
        committee.epoch,
        committee.total_stake
    );

    // 2. Extract endpoints with validator info
    let validators_with_endpoints: Vec<(String, String, u64)> = committee
        .validators
        .iter()
        .filter_map(|v| {
            let endpoint = parse_multiaddr(&v.network_address)?;
            Some((v.description.clone(), endpoint, v.stake))
        })
        .collect();

    tracing::info!(
        "Parsed {} endpoints from multiaddrs",
        validators_with_endpoints.len()
    );

    // 3. DNS resolve hostnames → IPs
    tracing::info!("Resolving hostnames...");
    let resolved = resolve::resolve_validators(&validators_with_endpoints).await;
    tracing::info!("Resolved {} validators to IPs", resolved.len());

    // 4. Batch geolocate IPs
    tracing::info!("Geolocating IPs via ip-api.com...");
    let client = reqwest::Client::new();
    let unique_ips: Vec<std::net::Ipv4Addr> = {
        let mut ips: Vec<_> = resolved.iter().map(|r| r.ip).collect();
        ips.sort();
        ips.dedup();
        ips
    };
    let geo_results = geolocate::batch_geolocate(&client, &unique_ips).await?;
    tracing::info!("Geolocated {} IPs", geo_results.len());

    // 5. Optionally probe latency
    let mut latencies = HashMap::new();
    if args.probe {
        tracing::info!("Probing validator latency...");
        let endpoints: Vec<String> = resolved.iter().map(|r| r.endpoint.clone()).collect();
        let probed = FullnodeRacer::probe_validators(&endpoints, Duration::from_secs(5)).await;
        for (ep, lat) in probed {
            latencies.insert(ep, lat);
        }
        tracing::info!("Probed {} reachable validators", latencies.len());
    }

    // 6. Build and print report
    let report = report::build_report(&resolved, &geo_results, &latencies, committee.total_stake);

    match args.output.as_str() {
        "json" => report::print_json(&report),
        _ => report::print_table(&report, args.probe),
    }

    Ok(())
}
