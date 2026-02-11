use crate::committee::{parse_multiaddr, AuthorityName, Committee};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use tracing::{debug, trace, warn};

use crate::gen::{
    transaction_execution_service_client::TransactionExecutionServiceClient, Bcs,
    ExecuteTransactionRequest, Transaction, UserSignature,
};

/// Health state for a fullnode endpoint.
#[derive(Debug, Clone)]
struct FullnodeHealth {
    ema_latency_ms: f64,
    success_count: u64,
    error_count: u64,
    consecutive_failures: u32,
}

impl FullnodeHealth {
    fn new() -> Self {
        Self {
            ema_latency_ms: f64::MAX,
            success_count: 0,
            error_count: 0,
            consecutive_failures: 0,
        }
    }

    fn record_success(&mut self, latency: Duration) {
        let ms = latency.as_secs_f64() * 1000.0;
        if self.ema_latency_ms == f64::MAX {
            self.ema_latency_ms = ms;
        } else {
            self.ema_latency_ms = 0.3 * ms + 0.7 * self.ema_latency_ms;
        }
        self.success_count += 1;
        self.consecutive_failures = 0;
    }

    fn record_failure(&mut self) {
        self.error_count += 1;
        self.consecutive_failures += 1;
    }

    fn is_healthy(&self) -> bool {
        self.consecutive_failures < 3
    }
}

/// Multi-fullnode racing for transaction execution.
/// Supports JSON-RPC, gRPC V2 fullnode, and direct validator submission paths.
///
/// Strategy: Race the same signed TX across multiple endpoints.
/// First successful response wins. This is safe because identical signed TXs
/// are idempotent — same digest regardless of which node processes it.
pub struct FullnodeRacer {
    /// JSON-RPC fullnode endpoints.
    endpoints: Vec<String>,
    /// gRPC V2 fullnode endpoints (e.g. "http://localhost:9002").
    grpc_endpoints: Vec<String>,
    /// Direct validator gRPC endpoints (parsed from committee multiaddr, port 8080).
    validator_endpoints: Vec<String>,
    /// Cached gRPC channels (connected once, reused).
    grpc_channels: DashMap<String, Channel>,
    /// Health tracking per endpoint (JSON-RPC, gRPC, and validator).
    health: DashMap<String, FullnodeHealth>,
    /// HTTP client (shared, keep-alive connections).
    client: reqwest::Client,
    /// Timeout per request.
    request_timeout: Duration,
    /// Optional committee metadata for digest-aware validator ordering.
    committee: RwLock<Option<Arc<Committee>>>,
    /// Authority name -> validator endpoint mapping (http://host:port).
    name_to_endpoint: DashMap<AuthorityName, String>,
}

impl FullnodeRacer {
    pub fn new(endpoints: Vec<String>, request_timeout: Duration) -> Self {
        let health = DashMap::new();
        for ep in &endpoints {
            health.insert(ep.clone(), FullnodeHealth::new());
        }

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(60))
            .timeout(request_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            endpoints,
            grpc_endpoints: Vec::new(),
            validator_endpoints: Vec::new(),
            grpc_channels: DashMap::new(),
            health,
            client,
            request_timeout,
            committee: RwLock::new(None),
            name_to_endpoint: DashMap::new(),
        }
    }

    /// Create with both JSON-RPC and gRPC endpoints.
    pub fn with_grpc(
        endpoints: Vec<String>,
        grpc_endpoints: Vec<String>,
        request_timeout: Duration,
    ) -> Self {
        let health = DashMap::new();
        for ep in endpoints.iter().chain(grpc_endpoints.iter()) {
            health.insert(ep.clone(), FullnodeHealth::new());
        }

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(60))
            .timeout(request_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            endpoints,
            grpc_endpoints,
            validator_endpoints: Vec::new(),
            grpc_channels: DashMap::new(),
            health,
            client,
            request_timeout,
            committee: RwLock::new(None),
            name_to_endpoint: DashMap::new(),
        }
    }

    /// Create with JSON-RPC, gRPC fullnode, and direct validator endpoints.
    pub fn with_validators(
        endpoints: Vec<String>,
        grpc_endpoints: Vec<String>,
        validator_endpoints: Vec<String>,
        request_timeout: Duration,
    ) -> Self {
        let health = DashMap::new();
        for ep in endpoints
            .iter()
            .chain(grpc_endpoints.iter())
            .chain(validator_endpoints.iter())
        {
            health.insert(ep.clone(), FullnodeHealth::new());
        }

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(60))
            .timeout(request_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            endpoints,
            grpc_endpoints,
            validator_endpoints,
            grpc_channels: DashMap::new(),
            health,
            client,
            request_timeout,
            committee: RwLock::new(None),
            name_to_endpoint: DashMap::new(),
        }
    }

    /// Install committee metadata so direct-validator races can combine
    /// latency ranking with digest-aware consensus-optimal ordering.
    pub fn set_committee(&self, committee: Arc<Committee>) {
        self.name_to_endpoint.clear();

        for validator in &committee.validators {
            let endpoint = if validator.network_address.starts_with('/') {
                parse_multiaddr(&validator.network_address)
            } else {
                Some(validator.network_address.clone())
            };

            if let Some(endpoint) = endpoint {
                self.name_to_endpoint.insert(validator.name, endpoint);
            }
        }

        *self.committee.write() = Some(committee);
    }

    /// Get or create a cached gRPC channel for an endpoint.
    /// Uses `connect_lazy()` so channel creation is instant — the actual TCP connection
    /// happens on the first gRPC call (inside spawned tasks, so it's parallel).
    fn get_or_create_channel(&self, endpoint: &str) -> Result<Channel> {
        if let Some(ch) = self.grpc_channels.get(endpoint) {
            return Ok(ch.clone());
        }

        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| anyhow!("Invalid gRPC endpoint {}: {}", endpoint, e))?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(15))
            .keep_alive_timeout(Duration::from_secs(5))
            .http2_adaptive_window(true)
            .connect_lazy();

        self.grpc_channels
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// Seed the health tracker with probe latencies so the very first TX
    /// races the fastest validators instead of picking randomly.
    pub fn seed_health(&self, probed: &[(String, Duration)]) {
        for (ep, latency) in probed {
            if let Some(mut h) = self.health.get_mut(ep) {
                h.record_success(*latency);
            }
        }
    }

    /// Pre-warm TCP + HTTP/2 connections to the top N validators.
    /// Call after probe to eliminate first-TX handshake latency.
    /// Uses `connect()` (non-lazy) to force the actual TCP + HTTP/2 handshake.
    pub async fn prewarm_channels(&self, count: usize) {
        let endpoints = self.select_validator_endpoints(count);
        let mut handles = Vec::new();
        for ep in endpoints {
            let ep_clone = ep.clone();
            let channels = self.grpc_channels.clone();
            handles.push(tokio::spawn(async move {
                let result = Channel::from_shared(ep_clone.clone()).ok().map(|e| {
                    e.connect_timeout(Duration::from_secs(5))
                        .tcp_nodelay(true)
                        .tcp_keepalive(Some(Duration::from_secs(30)))
                        .http2_keep_alive_interval(Duration::from_secs(15))
                        .keep_alive_timeout(Duration::from_secs(5))
                        .http2_adaptive_window(true)
                });
                if let Some(endpoint) = result {
                    match endpoint.connect().await {
                        Ok(channel) => {
                            channels.insert(ep_clone, channel);
                        }
                        Err(_) => {}
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }

    /// Pre-warm TCP + HTTP/2 connections to ALL configured validator endpoints.
    /// Eliminates cold-start variance when digest-based ordering or speculative
    /// fan-out selects validators outside the latency top-N.
    pub async fn prewarm_all_validators(&self) {
        let endpoints = self.validator_endpoints.clone();
        if endpoints.is_empty() {
            return;
        }
        let mut handles = Vec::new();
        for ep in endpoints {
            let ep_clone = ep.clone();
            let channels = self.grpc_channels.clone();
            handles.push(tokio::spawn(async move {
                let result = Channel::from_shared(ep_clone.clone()).ok().map(|e| {
                    e.connect_timeout(Duration::from_secs(5))
                        .tcp_nodelay(true)
                        .tcp_keepalive(Some(Duration::from_secs(30)))
                        .http2_keep_alive_interval(Duration::from_secs(15))
                        .keep_alive_timeout(Duration::from_secs(5))
                        .http2_adaptive_window(true)
                });
                if let Some(endpoint) = result {
                    match endpoint.connect().await {
                        Ok(channel) => {
                            channels.insert(ep_clone, channel);
                        }
                        Err(_) => {}
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }

    /// Select healthy validator endpoints NOT in the exclusion list, sorted by latency.
    /// Used for speculative WaitForEffects fan-out to additional validators.
    fn select_additional_validator_endpoints(
        &self,
        count: usize,
        exclude: &[String],
    ) -> Vec<String> {
        let mut candidates: Vec<(String, f64)> = self
            .health
            .iter()
            .filter(|entry| {
                entry.value().is_healthy()
                    && self.validator_endpoints.contains(entry.key())
                    && !exclude.contains(entry.key())
            })
            .map(|entry| (entry.key().clone(), entry.value().ema_latency_ms))
            .collect();

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(count);
        candidates.into_iter().map(|(ep, _)| ep).collect()
    }

    /// WaitForEffects on a validator channel using only the transaction digest.
    /// Used for speculative fan-out — no consensus_position needed since the
    /// finalized `notify_read_executed_effects` path works on any validator.
    async fn wait_for_effects_on_channel(
        channel: Channel,
        bcs_digest: Vec<u8>,
        digest_str: String,
    ) -> Result<GrpcExecuteResponse> {
        use crate::validator_proto::*;

        let mut client = SuiValidatorClient::new(channel);

        let wait_request = RawWaitForEffectsRequest {
            transaction_digest: Some(bcs_digest),
            consensus_position: None,
            include_details: true,
            ping_type: None,
        };

        let wait_response = client
            .wait_for_effects(wait_request)
            .await
            .map_err(|e| anyhow!("Speculative WaitForEffects failed: {}", e))?;

        match wait_response.into_inner().inner {
            Some(RawValidatorTransactionStatus::Executed(status)) => {
                debug!(digest = %digest_str, "Speculative WaitForEffects → Executed");
                Ok(GrpcExecuteResponse {
                    digest: Some(digest_str),
                    effects_bcs: status.details.as_ref().map(|d| d.effects.clone()),
                    events_bcs: status.details.and_then(|d| d.events),
                    checkpoint: None,
                    was_inline_executed: false,
                })
            }
            Some(RawValidatorTransactionStatus::Rejected(status)) => Err(anyhow!(
                "Speculative WaitForEffects rejected: {:?}",
                status.error.map(|e| hex::encode(&e))
            )),
            Some(RawValidatorTransactionStatus::Expired(status)) => Err(anyhow!(
                "Speculative WaitForEffects expired: epoch={}, round={:?}",
                status.epoch,
                status.round
            )),
            None => Err(anyhow!("Empty speculative WaitForEffects response")),
        }
    }

    /// Race a transaction execution across gRPC V2 endpoints.
    /// Takes raw BCS bytes (not base64).
    /// Returns the first successful response.
    pub async fn race_execute_grpc(
        &self,
        tx_bcs: &[u8],
        signature_bytes: &[u8],
    ) -> Result<GrpcRaceResult> {
        let endpoints = self.select_grpc_endpoints(self.grpc_endpoints.len().max(1));

        if endpoints.is_empty() {
            return Err(anyhow!("No gRPC endpoints configured"));
        }

        debug!(
            endpoints = endpoints.len(),
            "Racing transaction across gRPC V2 fullnodes"
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<GrpcRaceResult>>(endpoints.len());

        for endpoint in &endpoints {
            // Pre-connect (or reuse cached channel) before spawning race tasks
            let channel = match self.get_or_create_channel(endpoint) {
                Ok(ch) => ch,
                Err(e) => {
                    warn!(endpoint, error = %e, "Failed to connect gRPC channel");
                    continue;
                }
            };

            let endpoint = endpoint.clone();
            let tx_bcs = tx_bcs.to_vec();
            let sig_bytes = signature_bytes.to_vec();
            let send = tx.clone();
            let health = self.health.clone();
            let timeout_dur = self.request_timeout;

            tokio::spawn(async move {
                let start = Instant::now();
                let result = tokio::time::timeout(
                    timeout_dur,
                    Self::execute_on_grpc_channel(channel, &tx_bcs, &sig_bytes),
                )
                .await;

                let latency = start.elapsed();

                let result = match result {
                    Ok(inner) => inner,
                    Err(_) => Err(anyhow!("gRPC request timed out after {:?}", timeout_dur)),
                };

                if let Some(mut h) = health.get_mut(&endpoint) {
                    match &result {
                        Ok(_) => h.record_success(latency),
                        Err(_) => h.record_failure(),
                    }
                }

                let result = result.map(|response| GrpcRaceResult {
                    endpoint: endpoint.clone(),
                    digest: response.digest,
                    effects_bcs: response.effects_bcs,
                    events_bcs: response.events_bcs,
                    checkpoint: response.checkpoint,
                    latency,
                    was_inline_executed: response.was_inline_executed,
                    was_speculative: false,
                });

                let _ = send.send(result).await;
            });
        }
        drop(tx);

        let mut last_error = None;
        while let Some(result) = rx.recv().await {
            match result {
                Ok(race_result) => {
                    debug!(
                        endpoint = race_result.endpoint,
                        latency_ms = race_result.latency.as_millis(),
                        digest = race_result.digest.as_deref().unwrap_or("?"),
                        "gRPC race won"
                    );
                    return Ok(race_result);
                }
                Err(e) => {
                    trace!(error = %e, "gRPC race participant failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("All gRPC fullnodes failed")))
    }

    /// Execute a transaction on a pre-connected gRPC V2 channel.
    async fn execute_on_grpc_channel(
        channel: Channel,
        tx_bcs: &[u8],
        signature_bytes: &[u8],
    ) -> Result<GrpcExecuteResponse> {
        let mut client = TransactionExecutionServiceClient::new(channel);

        let request = ExecuteTransactionRequest {
            transaction: Some(Transaction {
                bcs: Some(Bcs {
                    name: None,
                    value: Some(tx_bcs.to_vec()),
                }),
                digest: None,
                version: None,
                kind: None,
                sender: None,
                gas_payment: None,
                expiration: None,
            }),
            signatures: vec![UserSignature {
                bcs: Some(Bcs {
                    name: None,
                    value: Some(signature_bytes.to_vec()),
                }),
                scheme: None,
                signature: None,
            }],
            read_mask: Some(prost_types::FieldMask {
                paths: vec![
                    "effects".to_string(),
                    "events".to_string(),
                    "checkpoint".to_string(),
                    "effects.status".to_string(),
                ],
            }),
        };

        let response = client
            .execute_transaction(request)
            .await
            .map_err(|e| anyhow!("gRPC execute failed: {}", e))?;

        let inner = response.into_inner();
        let executed = inner
            .transaction
            .ok_or_else(|| anyhow!("No executed transaction in gRPC response"))?;

        Ok(GrpcExecuteResponse {
            digest: executed.digest,
            effects_bcs: executed.effects.and_then(|e| e.bcs).and_then(|b| b.value),
            events_bcs: executed.events.and_then(|e| e.bcs).and_then(|b| b.value),
            checkpoint: executed.checkpoint,
            was_inline_executed: true, // gRPC fullnode always returns effects inline
        })
    }

    /// Race a transaction execution across direct validator gRPC endpoints.
    /// Uses the `sui.validator.Validator/SubmitTransaction` + `WaitForEffects` protocol.
    /// Takes raw BCS bytes (not base64).
    /// Selects up to `max_validators` endpoints by health/latency and races them.
    ///
    /// After the initial Submit race, a speculative coordinator fans out
    /// `WaitForEffects` to additional validators once any task receives a
    /// `Submitted` response. This reduces P50 by racing more validators for
    /// the effects phase.
    pub async fn race_execute_validators(
        &self,
        tx_bcs: &[u8],
        signature_bytes: &[u8],
        max_validators: usize,
    ) -> Result<GrpcRaceResult> {
        let endpoints = self.select_validator_endpoints_for_tx(tx_bcs, max_validators);

        if endpoints.is_empty() {
            return Err(anyhow!("No validator endpoints configured"));
        }

        debug!(
            endpoints = endpoints.len(),
            "Racing transaction across direct validators"
        );

        // Watch channel for speculative fan-out: signals (bcs_digest, digest_str) on first Submitted.
        let (digest_tx, digest_rx) = tokio::sync::watch::channel(None::<(Vec<u8>, String)>);
        let digest_tx = Arc::new(digest_tx);

        // Pre-select additional validators for speculative WaitForEffects
        let speculative_count = 5usize;
        let additional_endpoints =
            self.select_additional_validator_endpoints(speculative_count, &endpoints);

        // Pre-create channels for additional endpoints (uses connect_lazy if not already warmed)
        let mut additional_channels: Vec<(String, Channel)> = Vec::new();
        for ep in &additional_endpoints {
            if let Ok(ch) = self.get_or_create_channel(ep) {
                additional_channels.push((ep.clone(), ch));
            }
        }

        let channel_count = endpoints.len() + additional_channels.len();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<GrpcRaceResult>>(channel_count);

        // Spawn per-validator Submit+WaitForEffects tasks (existing behavior)
        for endpoint in &endpoints {
            let channel = match self.get_or_create_channel(endpoint) {
                Ok(ch) => ch,
                Err(e) => {
                    warn!(endpoint, error = %e, "Failed to connect to validator");
                    continue;
                }
            };

            let endpoint = endpoint.clone();
            let tx_bcs = tx_bcs.to_vec();
            let sig_bytes = signature_bytes.to_vec();
            let send = tx.clone();
            let health = self.health.clone();
            let timeout_dur = self.request_timeout;
            let digest_signal = digest_tx.clone();

            tokio::spawn(async move {
                let start = Instant::now();
                let result = tokio::time::timeout(
                    timeout_dur,
                    Self::execute_on_validator_channel(channel, &tx_bcs, &sig_bytes, digest_signal),
                )
                .await;

                let latency = start.elapsed();

                let result = match result {
                    Ok(inner) => inner,
                    Err(_) => Err(anyhow!(
                        "Validator request timed out after {:?}",
                        timeout_dur
                    )),
                };

                if let Some(mut h) = health.get_mut(&endpoint) {
                    match &result {
                        Ok(_) => h.record_success(latency),
                        Err(_) => h.record_failure(),
                    }
                }

                let result = result.map(|response| GrpcRaceResult {
                    endpoint: endpoint.clone(),
                    digest: response.digest,
                    effects_bcs: response.effects_bcs,
                    events_bcs: response.events_bcs,
                    checkpoint: response.checkpoint,
                    latency,
                    was_inline_executed: response.was_inline_executed,
                    was_speculative: false,
                });

                let _ = send.send(result).await;
            });
        }

        // Drop our copy of the watch sender so the coordinator can detect
        // when all per-validator tasks have finished without signalling.
        drop(digest_tx);

        // Speculative coordinator: wait for first Submitted, then fan out WaitForEffects
        if !additional_channels.is_empty() {
            let send = tx.clone();
            let health = self.health.clone();
            let timeout_dur = self.request_timeout;
            let mut digest_rx = digest_rx;

            tokio::spawn(async move {
                // Wait for any per-validator task to signal Submitted
                let result =
                    tokio::time::timeout(timeout_dur, digest_rx.wait_for(|v| v.is_some())).await;
                let (bcs_digest, digest_str) = match result {
                    Ok(Ok(guard)) => guard.as_ref().unwrap().clone(),
                    _ => return, // timeout or all senders dropped (all tasks got Executed/error)
                };

                debug!(
                    additional_validators = additional_channels.len(),
                    digest = %digest_str,
                    "Speculative fan-out: WaitForEffects on additional validators"
                );

                for (ep, channel) in additional_channels {
                    let bcs_digest = bcs_digest.clone();
                    let digest_str = digest_str.clone();
                    let send = send.clone();
                    let health = health.clone();

                    tokio::spawn(async move {
                        let start = Instant::now();
                        let result = tokio::time::timeout(
                            timeout_dur,
                            Self::wait_for_effects_on_channel(channel, bcs_digest, digest_str),
                        )
                        .await;

                        let latency = start.elapsed();

                        let result = match result {
                            Ok(inner) => inner,
                            Err(_) => Err(anyhow!("Speculative WaitForEffects timed out")),
                        };

                        if let Some(mut h) = health.get_mut(&ep) {
                            match &result {
                                Ok(_) => h.record_success(latency),
                                Err(_) => h.record_failure(),
                            }
                        }

                        let result = result.map(|response| GrpcRaceResult {
                            endpoint: ep,
                            digest: response.digest,
                            effects_bcs: response.effects_bcs,
                            events_bcs: response.events_bcs,
                            checkpoint: response.checkpoint,
                            latency,
                            was_inline_executed: false,
                            was_speculative: true,
                        });

                        let _ = send.send(result).await;
                    });
                }
            });
        }
        drop(tx);

        let mut last_error = None;
        while let Some(result) = rx.recv().await {
            match result {
                Ok(race_result) => {
                    debug!(
                        endpoint = race_result.endpoint,
                        latency_ms = race_result.latency.as_millis(),
                        digest = race_result.digest.as_deref().unwrap_or("?"),
                        was_inline_executed = race_result.was_inline_executed,
                        was_speculative = race_result.was_speculative,
                        "Validator race won"
                    );
                    return Ok(race_result);
                }
                Err(e) => {
                    trace!(error = %e, "Validator race participant failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("All validators failed")))
    }

    /// Select direct validator endpoints using:
    /// 1) latency ranking, and
    /// 2) consensus-optimal ordering seeded by tx digest (if committee installed).
    fn select_validator_endpoints_for_tx(&self, tx_bcs: &[u8], max: usize) -> Vec<String> {
        let latency_top = self.select_validator_endpoints(max);
        if latency_top.is_empty() {
            return latency_top;
        }

        let committee = match self.committee.read().as_ref() {
            Some(c) => c.clone(),
            None => return latency_top,
        };

        let digest_vec = crate::signer::tx_digest_bytes(tx_bcs);
        if digest_vec.len() != 32 {
            return latency_top;
        }

        let mut digest = [0u8; 32];
        digest.copy_from_slice(&digest_vec[..32]);

        let optimal_names = committee.shuffle_by_stake_from_digest(&digest);
        let mut optimal_endpoints = Vec::new();
        for name in optimal_names {
            if let Some(endpoint) = self.name_to_endpoint.get(&name) {
                let endpoint = endpoint.value().clone();
                if self.validator_endpoints.contains(&endpoint) && self.is_healthy(&endpoint) {
                    optimal_endpoints.push(endpoint);
                }
            }
        }

        if optimal_endpoints.is_empty() {
            return latency_top;
        }

        Self::merge_endpoint_selections(&latency_top, &optimal_endpoints, max)
    }

    /// Probe validator endpoints in parallel via TCP connect.
    /// Returns only reachable endpoints with their latencies, sorted by connection latency (fastest first).
    pub async fn probe_validators(
        endpoints: &[String],
        timeout: Duration,
    ) -> Vec<(String, Duration)> {
        use tokio::net::TcpStream;

        let mut handles = Vec::with_capacity(endpoints.len());

        for endpoint in endpoints {
            let ep = endpoint.clone();
            let timeout = timeout;
            handles.push(tokio::spawn(async move {
                // Parse host:port from "http://host:port"
                let addr = ep
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                let start = Instant::now();
                let result = tokio::time::timeout(timeout, TcpStream::connect(addr)).await;
                let latency = start.elapsed();
                match result {
                    Ok(Ok(_)) => Some((ep, latency)),
                    _ => None,
                }
            }));
        }

        let mut reachable: Vec<(String, Duration)> = Vec::new();
        for handle in handles {
            if let Ok(Some(result)) = handle.await {
                reachable.push(result);
            }
        }

        reachable.sort_by_key(|(_, lat)| *lat);
        reachable
    }

    /// Execute a transaction on a validator via `sui.validator.Validator` gRPC service.
    ///
    /// Flow:
    /// 1. Build BCS Transaction from tx_data + signature
    /// 2. Call `SubmitTransaction` on the validator
    /// 3. If `Executed` → effects inline, return immediately (Mysticeti v2)
    /// 4. If `Submitted` → signal digest for speculative fan-out, then call `WaitForEffects`
    /// 5. If `Rejected` → return error
    ///
    /// The `digest_signal` is used to notify the speculative coordinator that a
    /// Submitted response was received, providing the BCS-serialized digest so
    /// additional validators can race WaitForEffects.
    async fn execute_on_validator_channel(
        channel: Channel,
        tx_data_bcs: &[u8],
        sig_bytes: &[u8],
        digest_signal: Arc<tokio::sync::watch::Sender<Option<(Vec<u8>, String)>>>,
    ) -> Result<GrpcExecuteResponse> {
        use crate::validator_proto::*;

        let bcs_tx = build_bcs_transaction(tx_data_bcs, sig_bytes);
        let mut client = SuiValidatorClient::new(channel);

        // Compute digest from raw TransactionData (with Signable type prefix),
        // NOT from the full BCS Transaction envelope.
        let digest_str = tx_digest_of_tx_data(tx_data_bcs);

        let request = RawSubmitTxRequest {
            transactions: vec![bcs_tx.clone()],
            submit_type: SubmitTxType::Default as i32,
        };

        let response = client
            .submit_transaction(request)
            .await
            .map_err(|e| anyhow!("Validator SubmitTransaction failed: {}", e))?;

        let inner = response.into_inner();

        if inner.results.is_empty() {
            return Err(anyhow!("Validator returned empty results"));
        }

        let result = inner.results.into_iter().next().unwrap();

        match result.inner {
            Some(RawValidatorSubmitStatus::Executed(status)) => {
                debug!(digest = %digest_str, "SubmitTransaction → Executed (already finalized)");
                Ok(GrpcExecuteResponse {
                    digest: Some(digest_str),
                    effects_bcs: status.details.as_ref().map(|d| d.effects.clone()),
                    events_bcs: status.details.and_then(|d| d.events),
                    checkpoint: None,
                    was_inline_executed: true,
                })
            }
            Some(RawValidatorSubmitStatus::Submitted(consensus_position)) => {
                // Submitted to consensus — call WaitForEffects.
                // From datacenter VPS, validators typically return Executed inline
                // (Mysticeti v2), but from higher-latency clients, Submitted is common.
                debug!(digest = %digest_str, "SubmitTransaction → Submitted, waiting for effects...");

                let bcs_digest = bcs_serialize_digest(tx_data_bcs);

                // Signal the speculative coordinator so it can fan out WaitForEffects
                // to additional validators. Only the first Submitted signal matters.
                let _ = digest_signal.send_if_modified(|v| {
                    if v.is_none() {
                        *v = Some((bcs_digest.clone(), digest_str.clone()));
                        true
                    } else {
                        false
                    }
                });

                let wait_request = RawWaitForEffectsRequest {
                    transaction_digest: Some(bcs_digest),
                    consensus_position: Some(consensus_position),
                    include_details: true,
                    ping_type: None,
                };

                let wait_response = client
                    .wait_for_effects(wait_request)
                    .await
                    .map_err(|e| anyhow!("Validator WaitForEffects failed: {}", e))?;

                match wait_response.into_inner().inner {
                    Some(RawValidatorTransactionStatus::Executed(status)) => {
                        debug!(digest = %digest_str, "WaitForEffects → Executed");
                        Ok(GrpcExecuteResponse {
                            digest: Some(digest_str),
                            effects_bcs: status.details.as_ref().map(|d| d.effects.clone()),
                            events_bcs: status.details.and_then(|d| d.events),
                            checkpoint: None,
                            was_inline_executed: false,
                        })
                    }
                    Some(RawValidatorTransactionStatus::Rejected(status)) => Err(anyhow!(
                        "Transaction rejected after consensus: {:?}",
                        status.error.map(|e| hex::encode(&e))
                    )),
                    Some(RawValidatorTransactionStatus::Expired(status)) => Err(anyhow!(
                        "Transaction expired: epoch={}, round={:?}",
                        status.epoch,
                        status.round
                    )),
                    None => Err(anyhow!("Empty WaitForEffects response")),
                }
            }
            Some(RawValidatorSubmitStatus::Rejected(status)) => {
                warn!(digest = %digest_str, "SubmitTransaction → Rejected");
                Err(anyhow!(
                    "Transaction rejected by validator: {:?}",
                    status.error.map(|e| hex::encode(&e))
                ))
            }
            None => Err(anyhow!("Empty SubmitTransaction result")),
        }
    }

    /// Race a transaction execution across multiple fullnodes via JSON-RPC.
    /// Returns the first successful response.
    pub async fn race_execute(
        &self,
        tx_bytes_b64: &str,
        signatures_b64: &[String],
        max_endpoints: usize,
    ) -> Result<RaceResult> {
        let endpoints = self.select_endpoints(max_endpoints);

        if endpoints.is_empty() {
            return Err(anyhow!("No healthy fullnode endpoints available"));
        }

        debug!(
            endpoints = endpoints.len(),
            "Racing transaction across fullnodes"
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<RaceResult>>(endpoints.len());

        for endpoint in &endpoints {
            let client = self.client.clone();
            let endpoint = endpoint.clone();
            let tx_bytes = tx_bytes_b64.to_string();
            let sigs: Vec<String> = signatures_b64.to_vec();
            let send = tx.clone();
            let health = self.health.clone();

            tokio::spawn(async move {
                let start = Instant::now();
                let result = Self::execute_on_fullnode(&client, &endpoint, &tx_bytes, &sigs).await;
                let latency = start.elapsed();

                if let Some(mut h) = health.get_mut(&endpoint) {
                    match &result {
                        Ok(_) => h.record_success(latency),
                        Err(_) => h.record_failure(),
                    }
                }

                let result = result.map(|response| RaceResult {
                    endpoint: endpoint.clone(),
                    response,
                    latency,
                });

                let _ = send.send(result).await;
            });
        }
        drop(tx);

        // Return first success
        let mut last_error = None;
        while let Some(result) = rx.recv().await {
            match result {
                Ok(race_result) => {
                    debug!(
                        endpoint = race_result.endpoint,
                        latency_ms = race_result.latency.as_millis(),
                        "Race won"
                    );
                    return Ok(race_result);
                }
                Err(e) => {
                    trace!(error = %e, "Race participant failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("All fullnodes failed")))
    }

    /// Forward a raw JSON-RPC request to the fastest healthy fullnode.
    pub async fn forward_jsonrpc(
        &self,
        request_body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let endpoints = self.select_endpoints(1);
        let endpoint = endpoints
            .first()
            .ok_or_else(|| anyhow!("No healthy fullnode endpoints"))?;

        let start = Instant::now();
        let response = self.client.post(endpoint).json(request_body).send().await?;

        let latency = start.elapsed();

        if let Some(mut h) = self.health.get_mut(endpoint) {
            h.record_success(latency);
        }

        let body: serde_json::Value = response.json().await?;
        Ok(body)
    }

    /// Select the best JSON-RPC endpoints by latency.
    fn select_endpoints(&self, max: usize) -> Vec<String> {
        let mut candidates: Vec<(String, f64)> = self
            .health
            .iter()
            .filter(|entry| entry.value().is_healthy() && self.endpoints.contains(entry.key()))
            .map(|entry| (entry.key().clone(), entry.value().ema_latency_ms))
            .collect();

        if candidates.is_empty() {
            warn!("No healthy fullnodes, trying all endpoints");
            return self.endpoints.iter().take(max).cloned().collect();
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(max);
        candidates.into_iter().map(|(ep, _)| ep).collect()
    }

    /// Select the best gRPC endpoints by latency.
    fn select_grpc_endpoints(&self, max: usize) -> Vec<String> {
        let mut candidates: Vec<(String, f64)> = self
            .health
            .iter()
            .filter(|entry| entry.value().is_healthy() && self.grpc_endpoints.contains(entry.key()))
            .map(|entry| (entry.key().clone(), entry.value().ema_latency_ms))
            .collect();

        if candidates.is_empty() {
            return self.grpc_endpoints.iter().take(max).cloned().collect();
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(max);
        candidates.into_iter().map(|(ep, _)| ep).collect()
    }

    /// Select the best direct validator endpoints by latency.
    fn select_validator_endpoints(&self, max: usize) -> Vec<String> {
        let mut candidates: Vec<(String, f64)> = self
            .health
            .iter()
            .filter(|entry| {
                entry.value().is_healthy() && self.validator_endpoints.contains(entry.key())
            })
            .map(|entry| (entry.key().clone(), entry.value().ema_latency_ms))
            .collect();

        if candidates.is_empty() {
            return self.validator_endpoints.iter().take(max).cloned().collect();
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(max);
        candidates.into_iter().map(|(ep, _)| ep).collect()
    }

    /// Merge latency-based and digest-optimal endpoint orderings.
    /// Endpoints in both sets are prioritized first.
    fn merge_endpoint_selections(
        latency_top: &[String],
        optimal: &[String],
        max: usize,
    ) -> Vec<String> {
        if max == 0 {
            return Vec::new();
        }

        let optimal_top: Vec<String> = optimal.iter().take(max).cloned().collect();
        let optimal_set: HashSet<String> = optimal_top.iter().cloned().collect();

        let mut merged: Vec<String> = latency_top
            .iter()
            .filter(|ep| optimal_set.contains(*ep))
            .cloned()
            .collect();
        merged.truncate(max);

        if merged.len() >= max {
            return merged;
        }

        let mut latency_only: VecDeque<String> = latency_top
            .iter()
            .filter(|ep| !merged.contains(*ep))
            .cloned()
            .collect();
        let mut optimal_only: VecDeque<String> = optimal_top
            .into_iter()
            .filter(|ep| !merged.contains(ep))
            .collect();

        // Keep the fastest observed endpoint first when there is no overlap.
        if merged.is_empty() {
            if let Some(first_latency) = latency_only.pop_front() {
                merged.push(first_latency);
            }
        }

        let mut prefer_optimal = true;
        while merged.len() < max {
            let next = if prefer_optimal {
                optimal_only
                    .pop_front()
                    .or_else(|| latency_only.pop_front())
            } else {
                latency_only
                    .pop_front()
                    .or_else(|| optimal_only.pop_front())
            };
            let Some(endpoint) = next else {
                break;
            };
            if !merged.contains(&endpoint) {
                merged.push(endpoint);
            }
            prefer_optimal = !prefer_optimal;
        }

        merged.truncate(max);
        merged
    }

    fn is_healthy(&self, endpoint: &str) -> bool {
        self.health
            .get(endpoint)
            .map(|h| h.is_healthy())
            .unwrap_or(true)
    }

    /// Execute a transaction on a single fullnode via JSON-RPC.
    async fn execute_on_fullnode(
        client: &reqwest::Client,
        endpoint: &str,
        tx_bytes_b64: &str,
        signatures_b64: &[String],
    ) -> Result<serde_json::Value> {
        let response = client
            .post(endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sui_executeTransactionBlock",
                "params": [
                    tx_bytes_b64,
                    signatures_b64,
                    {
                        "showEffects": true,
                        "showEvents": true,
                    },
                    "WaitForLocalExecution"
                ]
            }))
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;

        if let Some(error) = body.get("error") {
            return Err(anyhow!("RPC error: {}", error));
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("No result in response"))
    }

    /// Get health summary for all endpoints.
    pub fn health_summary(&self) -> Vec<FullnodeHealthSummary> {
        self.health
            .iter()
            .map(|entry| FullnodeHealthSummary {
                endpoint: entry.key().clone(),
                ema_latency_ms: entry.value().ema_latency_ms,
                success_count: entry.value().success_count,
                error_count: entry.value().error_count,
                healthy: entry.value().is_healthy(),
            })
            .collect()
    }
}

/// Result of a JSON-RPC race execution.
#[derive(Debug)]
pub struct RaceResult {
    pub endpoint: String,
    pub response: serde_json::Value,
    pub latency: Duration,
}

/// Response from a single gRPC execute call.
struct GrpcExecuteResponse {
    digest: Option<String>,
    effects_bcs: Option<Vec<u8>>,
    events_bcs: Option<Vec<u8>>,
    checkpoint: Option<u64>,
    /// True if SubmitTransaction returned Executed directly (1 RTT, no WaitForEffects needed).
    was_inline_executed: bool,
}

/// Result of a gRPC V2 race execution.
#[derive(Debug)]
pub struct GrpcRaceResult {
    pub endpoint: String,
    pub digest: Option<String>,
    pub effects_bcs: Option<Vec<u8>>,
    pub events_bcs: Option<Vec<u8>>,
    pub checkpoint: Option<u64>,
    pub latency: Duration,
    /// True if SubmitTransaction returned Executed directly (1 RTT).
    pub was_inline_executed: bool,
    /// True if result came from speculative WaitForEffects fan-out (not the original Submit validator).
    pub was_speculative: bool,
}

/// Health summary for API responses.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FullnodeHealthSummary {
    pub endpoint: String,
    pub ema_latency_ms: f64,
    pub success_count: u64,
    pub error_count: u64,
    pub healthy: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committee::{Committee, ValidatorInfo};

    #[test]
    fn test_select_endpoints_ordering() {
        let racer = FullnodeRacer::new(
            vec![
                "http://a:443".into(),
                "http://b:443".into(),
                "http://c:443".into(),
            ],
            Duration::from_secs(10),
        );

        // Record some latencies
        racer
            .health
            .get_mut("http://a:443")
            .unwrap()
            .record_success(Duration::from_millis(100));
        racer
            .health
            .get_mut("http://b:443")
            .unwrap()
            .record_success(Duration::from_millis(50));
        racer
            .health
            .get_mut("http://c:443")
            .unwrap()
            .record_success(Duration::from_millis(200));

        let selected = racer.select_endpoints(2);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0], "http://b:443");
        assert_eq!(selected[1], "http://a:443");
    }

    #[test]
    fn test_unhealthy_endpoints_excluded() {
        let racer = FullnodeRacer::new(
            vec!["http://a:443".into(), "http://b:443".into()],
            Duration::from_secs(10),
        );

        for _ in 0..3 {
            racer
                .health
                .get_mut("http://a:443")
                .unwrap()
                .record_failure();
        }
        racer
            .health
            .get_mut("http://b:443")
            .unwrap()
            .record_success(Duration::from_millis(50));

        let selected = racer.select_endpoints(2);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0], "http://b:443");
    }

    #[test]
    fn test_grpc_endpoint_selection() {
        let racer = FullnodeRacer::with_grpc(
            vec!["http://rpc:443".into()],
            vec!["http://grpc1:9002".into(), "http://grpc2:9002".into()],
            Duration::from_secs(10),
        );

        let grpc = racer.select_grpc_endpoints(2);
        assert_eq!(grpc.len(), 2);

        // JSON-RPC selection should not include gRPC endpoints
        let rpc = racer.select_endpoints(2);
        assert_eq!(rpc.len(), 1);
        assert_eq!(rpc[0], "http://rpc:443");
    }

    #[test]
    fn test_validator_endpoint_selection() {
        let racer = FullnodeRacer::with_validators(
            vec!["http://rpc:443".into()],
            vec!["http://grpc:9002".into()],
            vec![
                "http://val1:8080".into(),
                "http://val2:8080".into(),
                "http://val3:8080".into(),
            ],
            Duration::from_secs(10),
        );

        // All 3 validator endpoints available
        let vals = racer.select_validator_endpoints(3);
        assert_eq!(vals.len(), 3);

        // Limit to 2
        let vals = racer.select_validator_endpoints(2);
        assert_eq!(vals.len(), 2);

        // Validator selection should not include fullnode endpoints
        let grpc = racer.select_grpc_endpoints(5);
        assert_eq!(grpc.len(), 1);
        assert_eq!(grpc[0], "http://grpc:9002");
    }

    #[test]
    fn test_validator_endpoints_sorted_by_latency() {
        let racer = FullnodeRacer::with_validators(
            vec![],
            vec![],
            vec![
                "http://slow:8080".into(),
                "http://fast:8080".into(),
                "http://mid:8080".into(),
            ],
            Duration::from_secs(10),
        );

        racer
            .health
            .get_mut("http://slow:8080")
            .unwrap()
            .record_success(Duration::from_millis(300));
        racer
            .health
            .get_mut("http://fast:8080")
            .unwrap()
            .record_success(Duration::from_millis(50));
        racer
            .health
            .get_mut("http://mid:8080")
            .unwrap()
            .record_success(Duration::from_millis(150));

        let selected = racer.select_validator_endpoints(2);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0], "http://fast:8080");
        assert_eq!(selected[1], "http://mid:8080");
    }

    #[test]
    fn test_seed_health_from_probe() {
        let racer = FullnodeRacer::with_validators(
            vec![],
            vec![],
            vec![
                "http://a:8080".into(),
                "http://b:8080".into(),
                "http://c:8080".into(),
            ],
            Duration::from_secs(10),
        );

        // Before seeding, all validators have f64::MAX latency (unknown)
        let selected = racer.select_validator_endpoints(3);
        assert_eq!(selected.len(), 3);
        // Order is arbitrary since all are f64::MAX

        // Seed with probe latencies
        let probed = vec![
            ("http://c:8080".to_string(), Duration::from_millis(50)),
            ("http://a:8080".to_string(), Duration::from_millis(200)),
            ("http://b:8080".to_string(), Duration::from_millis(100)),
        ];
        racer.seed_health(&probed);

        // Now selection should respect probe order
        let selected = racer.select_validator_endpoints(2);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0], "http://c:8080");
        assert_eq!(selected[1], "http://b:8080");
    }

    #[test]
    fn test_digest_aware_validator_selection_merges_with_latency() {
        let endpoints = vec![
            "http://val1:8080".to_string(),
            "http://val2:8080".to_string(),
            "http://val3:8080".to_string(),
        ];
        let racer =
            FullnodeRacer::with_validators(vec![], vec![], endpoints, Duration::from_secs(10));

        // Pure latency order would be: val3, val2, val1
        racer
            .health
            .get_mut("http://val1:8080")
            .unwrap()
            .record_success(Duration::from_millis(300));
        racer
            .health
            .get_mut("http://val2:8080")
            .unwrap()
            .record_success(Duration::from_millis(120));
        racer
            .health
            .get_mut("http://val3:8080")
            .unwrap()
            .record_success(Duration::from_millis(50));

        let mk_name = |n: u8| {
            let mut name = [0u8; 32];
            name[0] = n;
            name
        };

        // Stakes enforce consensus-optimal order: val1, val2, val3.
        let committee = Arc::new(Committee::new(
            1,
            vec![
                ValidatorInfo {
                    name: mk_name(1),
                    network_address: "http://val1:8080".to_string(),
                    stake: 3000,
                    description: "val1".to_string(),
                },
                ValidatorInfo {
                    name: mk_name(2),
                    network_address: "http://val2:8080".to_string(),
                    stake: 2000,
                    description: "val2".to_string(),
                },
                ValidatorInfo {
                    name: mk_name(3),
                    network_address: "http://val3:8080".to_string(),
                    stake: 1000,
                    description: "val3".to_string(),
                },
            ],
        ));
        racer.set_committee(committee);

        let tx_bcs = b"tx-bytes";
        let selected = racer.select_validator_endpoints_for_tx(tx_bcs, 2);

        let digest_vec = crate::signer::tx_digest_bytes(tx_bcs);
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&digest_vec[..32]);
        let committee = racer.committee.read().as_ref().unwrap().clone();
        let optimal_endpoints: Vec<String> = committee
            .shuffle_by_stake_from_digest(&digest)
            .into_iter()
            .filter_map(|name| racer.name_to_endpoint.get(&name).map(|e| e.value().clone()))
            .collect();

        // Digest-aware selection should match the merge policy fed by committee ordering.
        let expected = FullnodeRacer::merge_endpoint_selections(
            &vec![
                "http://val3:8080".to_string(),
                "http://val2:8080".to_string(),
            ],
            &optimal_endpoints,
            2,
        );
        assert_eq!(selected, expected);
    }

    #[test]
    fn test_select_additional_validator_endpoints_excludes_given() {
        let racer = FullnodeRacer::with_validators(
            vec![],
            vec![],
            vec![
                "http://val1:8080".into(),
                "http://val2:8080".into(),
                "http://val3:8080".into(),
                "http://val4:8080".into(),
                "http://val5:8080".into(),
            ],
            Duration::from_secs(10),
        );

        racer
            .health
            .get_mut("http://val1:8080")
            .unwrap()
            .record_success(Duration::from_millis(50));
        racer
            .health
            .get_mut("http://val2:8080")
            .unwrap()
            .record_success(Duration::from_millis(100));
        racer
            .health
            .get_mut("http://val3:8080")
            .unwrap()
            .record_success(Duration::from_millis(150));
        racer
            .health
            .get_mut("http://val4:8080")
            .unwrap()
            .record_success(Duration::from_millis(200));
        racer
            .health
            .get_mut("http://val5:8080")
            .unwrap()
            .record_success(Duration::from_millis(250));

        // Exclude val1 and val2 (the fastest)
        let exclude = vec![
            "http://val1:8080".to_string(),
            "http://val2:8080".to_string(),
        ];
        let additional = racer.select_additional_validator_endpoints(3, &exclude);

        assert_eq!(additional.len(), 3);
        assert_eq!(additional[0], "http://val3:8080");
        assert_eq!(additional[1], "http://val4:8080");
        assert_eq!(additional[2], "http://val5:8080");

        // Requesting more than available
        let additional = racer.select_additional_validator_endpoints(5, &exclude);
        assert_eq!(additional.len(), 3);
    }

    #[test]
    fn test_merge_endpoint_selections_keeps_fastest_when_no_overlap() {
        let latency_top = vec![
            "http://fast:8080".to_string(),
            "http://mid:8080".to_string(),
            "http://slow:8080".to_string(),
        ];
        let optimal = vec!["http://leader:8080".to_string()];

        let merged = FullnodeRacer::merge_endpoint_selections(&latency_top, &optimal, 2);
        assert_eq!(
            merged,
            vec![
                "http://fast:8080".to_string(),
                "http://leader:8080".to_string()
            ]
        );
    }
}
