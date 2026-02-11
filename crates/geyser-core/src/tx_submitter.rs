use crate::analyzer::TxAnalyzer;
use crate::committee::{AuthorityName, Committee};
use crate::validator_monitor::{OpType, ValidatorMonitor};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tonic::transport::Channel;
use tracing::{debug, trace, warn};

/// Result of a successful submission to a validator.
#[derive(Debug, Clone)]
pub struct SubmitResult {
    /// The validator that accepted the transaction.
    pub validator: AuthorityName,
    /// Whether the TX was executed immediately (fast path) or submitted to consensus.
    pub status: SubmitStatus,
    /// Raw effects bytes (if fast path executed immediately).
    pub effects_bytes: Option<Vec<u8>>,
    /// Raw events bytes (if fast path executed immediately).
    pub events_bytes: Option<Vec<u8>>,
    /// Consensus position returned by `SubmitTransaction::Submitted`, when available.
    pub consensus_position: Option<Vec<u8>>,
    /// Time taken for the submission.
    pub latency: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitStatus {
    /// Transaction was executed immediately (owned objects, fast path).
    Executed,
    /// Transaction was submitted to consensus (shared objects).
    Submitted,
}

/// Optional per-request submit tuning overrides.
#[derive(Debug, Clone, Default)]
pub struct SubmitOverrides {
    /// Per-validator SubmitTransaction timeout.
    pub submit_timeout: Option<Duration>,
    /// Initial exponential-backoff retry delay.
    pub initial_retry_delay: Option<Duration>,
    /// Maximum exponential-backoff retry delay.
    pub max_retry_delay: Option<Duration>,
}

/// Error classification for submission failures.
#[derive(Debug, Clone)]
pub enum SubmitError {
    /// Non-retriable: invalid signature, invalid TX, etc.
    NonRetriable(String),
    /// Retriable: timeout, overloaded, temporary failure.
    Retriable(String),
    /// Epoch ending: validator is halting.
    EpochEnding,
}

impl SubmitError {
    pub fn is_retriable(&self) -> bool {
        matches!(self, SubmitError::Retriable(_))
    }
}

/// Handles parallel submission of transactions to validators.
pub struct TxSubmitter {
    validator_monitor: Arc<ValidatorMonitor>,
    committee: Arc<RwLock<Arc<Committee>>>,
    analyzer: Arc<TxAnalyzer>,
    submit_timeout: Duration,
    initial_retry_delay: Duration,
    max_retry_delay: Duration,
    /// Cached gRPC channels to validators, keyed by endpoint URL.
    channels: Arc<DashMap<String, Channel>>,
}

/// Small grace period to prefer `Executed` over `Submitted` when racing validators.
const EXECUTED_PREFERENCE_GRACE: Duration = Duration::from_millis(20);

impl TxSubmitter {
    pub fn new(
        validator_monitor: Arc<ValidatorMonitor>,
        committee: Arc<RwLock<Arc<Committee>>>,
        analyzer: Arc<TxAnalyzer>,
        submit_timeout: Duration,
        initial_retry_delay: Duration,
        max_retry_delay: Duration,
    ) -> Self {
        Self {
            validator_monitor,
            committee,
            analyzer,
            submit_timeout,
            initial_retry_delay,
            max_retry_delay,
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Get or create a cached gRPC channel to a validator endpoint.
    fn get_or_create_channel(
        channels: &DashMap<String, Channel>,
        endpoint: &str,
    ) -> Result<Channel, SubmitError> {
        if let Some(ch) = channels.get(endpoint) {
            return Ok(ch.value().clone());
        }
        // Channel::from_shared creates a lazy channel — no actual TCP connection yet.
        // The connection is established on the first RPC call. tonic handles reconnection.
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| SubmitError::NonRetriable(format!("Invalid endpoint: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(15))
            .keep_alive_timeout(Duration::from_secs(5))
            .http2_adaptive_window(true);

        // Use connect_lazy so we don't block here — connection happens on first use
        let channel = channel.connect_lazy();
        channels.insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// Pre-warm gRPC channels to the top validators by eagerly connecting.
    /// Call after `probe_and_seed()` so latency data is available.
    pub async fn prewarm_channels(&self, count: usize) {
        let endpoints = self.validator_monitor.top_endpoints(count, OpType::Submit);
        let mut handles = Vec::with_capacity(endpoints.len());

        for (name, endpoint) in &endpoints {
            let endpoint = endpoint.clone();
            let name = *name;
            let channels = self.channels.clone();

            handles.push(tokio::spawn(async move {
                // Build a non-lazy channel that forces TCP+TLS+HTTP2 handshake
                let result = Channel::from_shared(endpoint.clone())
                    .map_err(|e| e.to_string())
                    .and_then(|ch| {
                        Ok(ch
                            .connect_timeout(Duration::from_secs(5))
                            .tcp_nodelay(true)
                            .tcp_keepalive(Some(Duration::from_secs(30)))
                            .http2_keep_alive_interval(Duration::from_secs(15))
                            .keep_alive_timeout(Duration::from_secs(5))
                            .http2_adaptive_window(true))
                    });

                match result {
                    Ok(builder) => match builder.connect().await {
                        Ok(channel) => {
                            channels.insert(endpoint.clone(), channel);
                            debug!(
                                validator = hex::encode(&name[..8]),
                                endpoint = %endpoint,
                                "Pre-warmed gRPC channel"
                            );
                            true
                        }
                        Err(e) => {
                            warn!(
                                validator = hex::encode(&name[..8]),
                                endpoint = %endpoint,
                                error = %e,
                                "Failed to pre-warm gRPC channel"
                            );
                            false
                        }
                    },
                    Err(e) => {
                        warn!(endpoint = %endpoint, error = %e, "Invalid endpoint for pre-warm");
                        false
                    }
                }
            }));
        }

        let mut warmed = 0usize;
        for handle in handles {
            if let Ok(true) = handle.await {
                warmed += 1;
            }
        }

        debug!(warmed, requested = count, "Pre-warm complete");
    }

    /// Submit a transaction to validators with the given amplification factor.
    ///
    /// Algorithm:
    /// 1. Select top `amplification` validators by latency
    /// 2. Compute consensus-optimal position from TX digest
    /// 3. Merge selections: prioritize validators in BOTH sets
    /// 4. Submit to all selected validators concurrently
    /// 5. First success wins
    /// 6. Classify errors: non-retriable vs retriable
    /// 7. If f+1 non-retriable → abort immediately
    pub async fn submit(
        &self,
        tx_bytes: &[u8],
        signatures: &[Vec<u8>],
        tx_digest: &[u8; 32],
        amplification: usize,
    ) -> Result<SubmitResult> {
        self.submit_with_timeout(
            tx_bytes,
            signatures,
            tx_digest,
            amplification,
            self.submit_timeout,
        )
        .await
    }

    async fn submit_with_timeout(
        &self,
        tx_bytes: &[u8],
        signatures: &[Vec<u8>],
        tx_digest: &[u8; 32],
        amplification: usize,
        submit_timeout: Duration,
    ) -> Result<SubmitResult> {
        let committee = self.committee.read().clone();

        // 1. Select by latency
        let latency_top = self
            .validator_monitor
            .select_validators(amplification, OpType::Submit);

        // 2. Get consensus-optimal ordering
        let optimal = self.analyzer.get_submission_position(&committee, tx_digest);

        // 3. Merge: validators in both sets get priority
        let merged = self.merge_selections(&latency_top, &optimal, amplification);

        debug!(
            amplification,
            validators = merged.len(),
            digest = hex::encode(&tx_digest[..8]),
            "Submitting to validators"
        );

        // 4. Submit to all concurrently
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Result<SubmitResult, SubmitError>>(merged.len());

        for validator_name in &merged {
            let name = *validator_name;
            let validator_info = committee.get_validator(&name).cloned();
            let tx_bytes = tx_bytes.to_vec();
            let signatures = signatures.to_vec();
            let tx = tx.clone();
            let monitor = self.validator_monitor.clone();
            let channels = self.channels.clone();

            tokio::spawn(async move {
                let start = Instant::now();
                let result = timeout(
                    submit_timeout,
                    Self::submit_to_validator(
                        &name,
                        validator_info.as_ref(),
                        &tx_bytes,
                        &signatures,
                        &channels,
                    ),
                )
                .await;

                let latency = start.elapsed();

                match result {
                    Ok(Ok(mut submit_result)) => {
                        submit_result.latency = latency;
                        // Only record Submit EMA for Executed responses — these
                        // validators returned effects inline and represent the true
                        // end-to-end latency. For Submitted responses, the
                        // QuorumDriver records the full pipeline latency (submit +
                        // effects collection) instead, which naturally penalizes
                        // validators that require a separate WaitForEffects phase.
                        if submit_result.status == SubmitStatus::Executed {
                            monitor.record_success(&name, OpType::Submit, latency);
                        }
                        let _ = tx.send(Ok(submit_result)).await;
                    }
                    Ok(Err(e)) => {
                        monitor.record_failure(&name, OpType::Submit);
                        let _ = tx.send(Err(e)).await;
                    }
                    Err(_) => {
                        monitor.record_failure(&name, OpType::Submit);
                        let _ = tx
                            .send(Err(SubmitError::Retriable("timeout".to_string())))
                            .await;
                    }
                }
            });
        }
        drop(tx); // Close sender so rx completes when all tasks finish

        // 5. Collect results:
        // - Return `Executed` immediately.
        // - On first `Submitted`, wait briefly for a possible `Executed` from another racer.
        let mut non_retriable_count = 0;
        let validity_threshold = (merged.len() + 2) / 3; // f+1
        let mut pending_submitted: Option<SubmitResult> = None;
        let mut submitted_deadline: Option<tokio::time::Instant> = None;

        loop {
            let recv_result = if let Some(deadline) = submitted_deadline {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(v) => v,
                    Err(_) => {
                        if let Some(submitted) = pending_submitted.take() {
                            debug!(
                                validator = hex::encode(&submitted.validator[..8]),
                                latency_ms = submitted.latency.as_millis(),
                                "Returning submitted result after executed-preference grace"
                            );
                            return Ok(submitted);
                        }
                        None
                    }
                }
            } else {
                rx.recv().await
            };

            let Some(result) = recv_result else {
                // Channel closed. If we have a pending submitted result, return it.
                if let Some(submitted) = pending_submitted {
                    return Ok(submitted);
                }
                break;
            };

            match result {
                Ok(submit_result) => {
                    debug!(
                        validator = hex::encode(&submit_result.validator[..8]),
                        status = ?submit_result.status,
                        latency_ms = submit_result.latency.as_millis(),
                        "Submission accepted"
                    );
                    if submit_result.status == SubmitStatus::Executed {
                        return Ok(submit_result);
                    }

                    if pending_submitted.is_none() {
                        pending_submitted = Some(submit_result);
                        submitted_deadline =
                            Some(tokio::time::Instant::now() + EXECUTED_PREFERENCE_GRACE);
                    }
                }
                Err(SubmitError::NonRetriable(msg)) => {
                    non_retriable_count += 1;
                    warn!(error = msg, "Non-retriable submission error");
                    // If f+1 validators say non-retriable, abort
                    if non_retriable_count >= validity_threshold {
                        return Err(anyhow!(
                            "Transaction rejected by {} validators: {}",
                            non_retriable_count,
                            msg
                        ));
                    }
                }
                Err(SubmitError::EpochEnding) => {
                    return Err(anyhow!("Epoch ending, validators halting"));
                }
                Err(SubmitError::Retriable(msg)) => {
                    trace!(error = msg, "Retriable submission error");
                }
            }
        }

        Err(anyhow!(
            "All {} validators failed to accept transaction",
            merged.len()
        ))
    }

    /// Submit with exponential backoff retry.
    pub async fn submit_with_retry(
        &self,
        tx_bytes: &[u8],
        signatures: &[Vec<u8>],
        tx_digest: &[u8; 32],
        amplification: usize,
        max_retries: usize,
    ) -> Result<SubmitResult> {
        self.submit_with_retry_overrides(
            tx_bytes,
            signatures,
            tx_digest,
            amplification,
            max_retries,
            None,
        )
        .await
    }

    /// Submit with exponential backoff retry plus optional per-request overrides.
    pub async fn submit_with_retry_overrides(
        &self,
        tx_bytes: &[u8],
        signatures: &[Vec<u8>],
        tx_digest: &[u8; 32],
        amplification: usize,
        max_retries: usize,
        overrides: Option<&SubmitOverrides>,
    ) -> Result<SubmitResult> {
        let submit_timeout = overrides
            .and_then(|o| o.submit_timeout)
            .unwrap_or(self.submit_timeout);
        let initial_retry_delay = overrides
            .and_then(|o| o.initial_retry_delay)
            .unwrap_or(self.initial_retry_delay);
        let max_retry_delay = overrides
            .and_then(|o| o.max_retry_delay)
            .unwrap_or(self.max_retry_delay);

        let mut delay = initial_retry_delay;

        for attempt in 0..=max_retries {
            match self
                .submit_with_timeout(
                    tx_bytes,
                    signatures,
                    tx_digest,
                    amplification,
                    submit_timeout,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempt == max_retries {
                        return Err(e);
                    }
                    warn!(
                        attempt,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "Submission failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_retry_delay);
                }
            }
        }

        unreachable!()
    }

    /// Merge latency-based and consensus-optimal selections.
    /// Validators appearing in both sets get priority.
    fn merge_selections(
        &self,
        latency_top: &[AuthorityName],
        optimal: &[AuthorityName],
        max: usize,
    ) -> Vec<AuthorityName> {
        if max == 0 {
            return Vec::new();
        }

        let optimal_top: Vec<AuthorityName> = optimal.iter().take(max).copied().collect();
        let optimal_set: HashSet<AuthorityName> = optimal_top.iter().copied().collect();

        // Priority: validators that are both low-latency and digest-optimal.
        let mut merged: Vec<AuthorityName> = latency_top
            .iter()
            .filter(|v| optimal_set.contains(*v))
            .copied()
            .collect();
        merged.truncate(max);

        if merged.len() >= max {
            return merged;
        }

        let mut latency_only: VecDeque<AuthorityName> = latency_top
            .iter()
            .copied()
            .filter(|v| !merged.contains(v))
            .collect();
        let mut optimal_only: VecDeque<AuthorityName> = optimal_top
            .into_iter()
            .filter(|v| !merged.contains(v))
            .collect();

        // Always keep the fastest latency candidate when there is no overlap.
        if merged.is_empty() {
            if let Some(first_latency) = latency_only.pop_front() {
                merged.push(first_latency);
            }
        }

        // Then interleave digest-optimal and latency-only candidates.
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
            let Some(name) = next else {
                break;
            };
            if !merged.contains(&name) {
                merged.push(name);
            }
            prefer_optimal = !prefer_optimal;
        }

        merged.truncate(max);
        merged
    }

    /// Submit a transaction to a single validator via the native
    /// `sui.validator.Validator/SubmitTransaction` gRPC service (port 8080).
    async fn submit_to_validator(
        name: &AuthorityName,
        validator_info: Option<&crate::committee::ValidatorInfo>,
        tx_bytes: &[u8],
        signatures: &[Vec<u8>],
        channels: &DashMap<String, Channel>,
    ) -> Result<SubmitResult, SubmitError> {
        use crate::validator_proto::*;

        let raw_addr = match validator_info {
            Some(info) => info.network_address.clone(),
            None => {
                return Err(SubmitError::Retriable(
                    "No network address for validator".to_string(),
                ));
            }
        };

        // Parse multiaddr format (/dns/host/tcp/port/http) into URL (http://host:port)
        let endpoint = if raw_addr.starts_with('/') {
            crate::committee::parse_multiaddr(&raw_addr).ok_or_else(|| {
                SubmitError::NonRetriable(format!("Unparseable multiaddr: {}", raw_addr))
            })?
        } else {
            raw_addr
        };

        let channel = Self::get_or_create_channel(channels, &endpoint)?;

        let mut client = SuiValidatorClient::new(channel);

        // Build BCS Transaction envelope from raw tx_bytes + first signature
        let sig = signatures
            .first()
            .ok_or_else(|| SubmitError::NonRetriable("No signatures provided".to_string()))?;
        let bcs_tx = build_bcs_transaction(tx_bytes, sig);

        let request = RawSubmitTxRequest {
            transactions: vec![bcs_tx],
            submit_type: SubmitTxType::Default as i32,
        };

        let response = client.submit_transaction(request).await.map_err(|e| {
            let msg = e.message().to_string();
            if msg.contains("epoch") || msg.contains("halting") {
                SubmitError::EpochEnding
            } else if msg.contains("invalid") || msg.contains("rejected") {
                SubmitError::NonRetriable(msg)
            } else {
                SubmitError::Retriable(msg)
            }
        })?;

        let inner = response.into_inner();

        if inner.results.is_empty() {
            return Err(SubmitError::Retriable(
                "Validator returned empty results".to_string(),
            ));
        }

        let result = inner.results.into_iter().next().unwrap();

        match result.inner {
            Some(RawValidatorSubmitStatus::Executed(status)) => {
                let effects_bytes = status.details.as_ref().map(|d| d.effects.clone());
                let events_bytes = status.details.as_ref().and_then(|d| d.events.clone());
                Ok(SubmitResult {
                    validator: *name,
                    status: SubmitStatus::Executed,
                    effects_bytes,
                    events_bytes,
                    consensus_position: None,
                    latency: Duration::ZERO,
                })
            }
            Some(RawValidatorSubmitStatus::Submitted(consensus_position)) => {
                // Submitted to consensus — return immediately.
                // The EffectsCertifier handles collecting effects from quorum.
                // Not blocking here lets other racing validators return Executed first.
                Ok(SubmitResult {
                    validator: *name,
                    status: SubmitStatus::Submitted,
                    effects_bytes: None,
                    events_bytes: None,
                    consensus_position: Some(consensus_position),
                    latency: Duration::ZERO,
                })
            }
            Some(RawValidatorSubmitStatus::Rejected(rej)) => {
                Err(SubmitError::NonRetriable(format!(
                    "Validator rejected: {:?}",
                    rej.error.map(|e| hex::encode(&e))
                )))
            }
            None => Err(SubmitError::Retriable(
                "Validator returned no status".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committee::{Committee, ValidatorInfo};
    use crate::validator_monitor::ValidatorMonitor;

    fn setup() -> (
        Arc<ValidatorMonitor>,
        Arc<RwLock<Arc<Committee>>>,
        Arc<TxAnalyzer>,
    ) {
        let validators: Vec<ValidatorInfo> = (0..4)
            .map(|i| {
                let mut name = [0u8; 32];
                name[0] = i;
                ValidatorInfo {
                    name,
                    network_address: format!("127.0.0.1:{}", 8000 + i as u16),
                    stake: 2500,
                    description: format!("v{}", i),
                }
            })
            .collect();
        let committee = Arc::new(RwLock::new(Arc::new(Committee::new(1, validators))));
        let monitor = Arc::new(ValidatorMonitor::new(
            committee.clone(),
            0.02,
            Duration::from_secs(10),
        ));
        monitor.init_from_committee();

        // Give validators some latency data
        for i in 0..4u8 {
            let mut name = [0u8; 32];
            name[0] = i;
            monitor.record_success(
                &name,
                OpType::Submit,
                Duration::from_millis(10 + i as u64 * 20),
            );
        }

        let analyzer = Arc::new(TxAnalyzer::new());
        (monitor, committee, analyzer)
    }

    #[test]
    fn test_merge_selections() {
        let (monitor, committee, analyzer) = setup();
        let submitter = TxSubmitter::new(
            monitor,
            committee,
            analyzer,
            Duration::from_secs(10),
            Duration::from_millis(10),
            Duration::from_secs(5),
        );

        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        let mut c = [0u8; 32];
        let mut d = [0u8; 32];
        a[0] = 0;
        b[0] = 1;
        c[0] = 2;
        d[0] = 3;

        let latency_top = vec![a, b, c];
        let optimal = vec![b, d, a];

        let merged = submitter.merge_selections(&latency_top, &optimal, 3);
        // a and b are both in the overlap set (latency + optimal), followed by digest-optimal d.
        assert_eq!(merged.len(), 3);
        assert_eq!(merged, vec![a, b, d]);
    }

    #[test]
    fn test_merge_selections_keeps_fastest_even_without_overlap() {
        let (monitor, committee, analyzer) = setup();
        let submitter = TxSubmitter::new(
            monitor,
            committee,
            analyzer,
            Duration::from_secs(10),
            Duration::from_millis(10),
            Duration::from_secs(5),
        );

        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        let mut c = [0u8; 32];
        let mut d = [0u8; 32];
        a[0] = 0;
        b[0] = 1;
        c[0] = 2;
        d[0] = 3;

        let latency_top = vec![a, b, c];
        let optimal = vec![d];

        // With no overlap, keep the fastest latency endpoint first, then include digest-optimal.
        let merged = submitter.merge_selections(&latency_top, &optimal, 2);
        assert_eq!(merged, vec![a, d]);
    }
}
