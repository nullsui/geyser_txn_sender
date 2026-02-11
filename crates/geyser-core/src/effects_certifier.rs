use crate::committee::{AuthorityName, Committee};
use crate::tx_submitter::{SubmitResult, SubmitStatus};
use crate::validator_monitor::{OpType, ValidatorMonitor};
use crate::validator_proto::{
    self, RawValidatorTransactionStatus, RawWaitForEffectsRequest, SuiValidatorClient,
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;
use tracing::{debug, trace, warn};

/// Certified transaction effects with quorum of validator signatures.
#[derive(Debug, Clone)]
pub struct CertifiedEffects {
    /// The effects digest (all validators must agree on this).
    pub effects_digest: [u8; 32],
    /// Raw effects bytes.
    pub effects_bytes: Vec<u8>,
    /// Validator signatures on the effects.
    pub signatures: Vec<(AuthorityName, Vec<u8>)>,
    /// Total stake weight of collected signatures.
    pub stake_weight: u64,
    /// Events bytes (if available).
    pub events_bytes: Option<Vec<u8>>,
    /// Time taken to certify.
    pub latency: Duration,
}

/// Result from a single validator's WaitForEffects query.
type EffectsQueryResult = Result<(AuthorityName, Vec<u8>, Vec<u8>, Option<Vec<u8>>)>;

/// Optional per-request effects certification tuning overrides.
#[derive(Debug, Clone, Default)]
pub struct EffectsCertifyOverrides {
    /// Overall timeout for collecting quorum-certified effects.
    pub effects_timeout: Option<Duration>,
    /// Delay before fallback fan-out to additional validators.
    pub effects_fallback_delay: Option<Duration>,
    /// Number of validators queried during fallback fan-out.
    pub effects_fallback_fanout: Option<usize>,
}

/// Collects certified effects from a quorum of validators.
pub struct EffectsCertifier {
    validator_monitor: Arc<ValidatorMonitor>,
    committee: Arc<RwLock<Arc<Committee>>>,
    effects_timeout: Duration,
    effects_fallback_delay: Duration,
    effects_fallback_fanout: usize,
    /// Cached gRPC channels to validators for WaitForEffects queries.
    channels: Arc<DashMap<String, Channel>>,
}

impl EffectsCertifier {
    pub fn new(
        validator_monitor: Arc<ValidatorMonitor>,
        committee: Arc<RwLock<Arc<Committee>>>,
        effects_timeout: Duration,
        effects_fallback_delay: Duration,
        effects_fallback_fanout: usize,
    ) -> Self {
        Self {
            validator_monitor,
            committee,
            effects_timeout,
            effects_fallback_delay,
            effects_fallback_fanout,
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Collect certified effects from quorum of validators.
    ///
    /// Algorithm:
    /// 1. If submit returned Executed (fast path) → already have effects, done
    /// 2. If Submitted (consensus) → start polling:
    ///    a. Send wait_for_effects to initial validator
    ///    b. After effects_fallback_delay, ALSO query fallback validators
    ///    c. Collect effects from 2/3+1 stake weight
    ///    d. Validate all effects digests match (Byzantine check)
    ///    e. Return certified effects
    pub async fn certify(
        &self,
        tx_digest: &[u8; 32],
        submit_result: &SubmitResult,
    ) -> Result<CertifiedEffects> {
        self.certify_with_overrides(tx_digest, submit_result, None)
            .await
    }

    /// Collect certified effects with optional per-request overrides.
    pub async fn certify_with_overrides(
        &self,
        tx_digest: &[u8; 32],
        submit_result: &SubmitResult,
        overrides: Option<&EffectsCertifyOverrides>,
    ) -> Result<CertifiedEffects> {
        let start = Instant::now();
        let committee = self.committee.read().clone();
        let effects_timeout = overrides
            .and_then(|o| o.effects_timeout)
            .unwrap_or(self.effects_timeout);
        let effects_fallback_delay = overrides
            .and_then(|o| o.effects_fallback_delay)
            .unwrap_or(self.effects_fallback_delay);
        let effects_fallback_fanout = overrides
            .and_then(|o| o.effects_fallback_fanout)
            .unwrap_or(self.effects_fallback_fanout)
            .max(1);

        // Fast path: already have effects from submission
        if submit_result.status == SubmitStatus::Executed {
            if let Some(effects_bytes) = &submit_result.effects_bytes {
                debug!(
                    digest = hex::encode(&tx_digest[..8]),
                    "Fast path: effects received from submission"
                );
                let stake = committee
                    .stake_map
                    .get(&submit_result.validator)
                    .copied()
                    .unwrap_or(0);

                return Ok(CertifiedEffects {
                    effects_digest: Self::hash_effects(effects_bytes),
                    effects_bytes: effects_bytes.clone(),
                    signatures: vec![(submit_result.validator, vec![])],
                    stake_weight: stake,
                    events_bytes: submit_result.events_bytes.clone(),
                    latency: start.elapsed(),
                });
            }
        }

        // Consensus path: need to collect effects from quorum
        debug!(
            digest = hex::encode(&tx_digest[..8]),
            "Consensus path: collecting effects from quorum via WaitForEffects"
        );

        let quorum_stake = committee.quorum_stake();
        let (effects_tx, mut effects_rx) =
            tokio::sync::mpsc::channel::<EffectsQueryResult>(committee.validators.len());
        let consensus_position = submit_result.consensus_position.clone();

        // Phase 1: Query the initial validator immediately
        self.spawn_effects_query(
            submit_result.validator,
            tx_digest,
            consensus_position.clone(),
            effects_tx.clone(),
        );

        // Phase 2: After fallback delay, query additional validators
        let fallback_delay = effects_fallback_delay;
        let fallback_fanout = effects_fallback_fanout;
        let monitor = self.validator_monitor.clone();
        let committee_clone = committee.clone();
        let tx_digest_clone = *tx_digest;
        let effects_tx_clone = effects_tx.clone();
        let consensus_position_clone = consensus_position.clone();
        let initial_validator = submit_result.validator;
        let channels_clone = self.channels.clone();

        tokio::spawn(async move {
            sleep(fallback_delay).await;

            // Select additional validators to query
            let additional = monitor.select_validators(fallback_fanout, OpType::Effects);
            for name in additional {
                if name != initial_validator {
                    Self::spawn_effects_query_static(
                        name,
                        &tx_digest_clone,
                        consensus_position_clone.clone(),
                        &committee_clone,
                        channels_clone.clone(),
                        effects_tx_clone.clone(),
                    );
                }
            }
        });

        drop(effects_tx); // Close sender

        // Collect until quorum
        // Tuple: (effects_bytes, effects_digest_bytes, events_bytes)
        let mut collected: HashMap<AuthorityName, (Vec<u8>, Vec<u8>)> = HashMap::new();
        let mut effects_digest: Option<[u8; 32]> = None;
        let mut accumulated_stake: u64 = 0;
        let mut first_events_bytes: Option<Vec<u8>> = None;

        let collect_result = timeout(effects_timeout, async {
            while let Some(result) = effects_rx.recv().await {
                if let Ok((name, effects_bytes, effects_digest_bytes, events_bytes)) = result {
                    let digest = Self::parse_effects_digest(&effects_digest_bytes)
                        .unwrap_or_else(|| Self::hash_effects(&effects_bytes));

                    // Byzantine check: all effects must have the same digest
                    match &effects_digest {
                        Some(expected) => {
                            if digest != *expected {
                                warn!(
                                    validator = hex::encode(&name[..8]),
                                    expected = hex::encode(&expected[..8]),
                                    got = hex::encode(&digest[..8]),
                                    "Effects digest mismatch — possible Byzantine validator"
                                );
                                continue;
                            }
                        }
                        None => {
                            effects_digest = Some(digest);
                        }
                    }

                    let stake = committee.stake_map.get(&name).copied().unwrap_or(0);
                    if collected
                        .insert(name, (effects_bytes, effects_digest_bytes))
                        .is_none()
                    {
                        accumulated_stake += stake;
                        self.validator_monitor.record_success(
                            &name,
                            OpType::Effects,
                            start.elapsed(),
                        );
                    }

                    // Capture events from the first validator that provides them
                    if first_events_bytes.is_none() {
                        first_events_bytes = events_bytes;
                    }

                    trace!(
                        accumulated_stake,
                        quorum_stake,
                        validators = collected.len(),
                        "Effects progress"
                    );

                    if accumulated_stake >= quorum_stake {
                        return Ok(());
                    }
                }
            }
            Err(anyhow!("Channel closed before reaching quorum"))
        })
        .await;

        match collect_result {
            Ok(Ok(())) => {
                let effects_digest = effects_digest.unwrap();
                let mut effects_bytes_out = Vec::new();
                let mut signatures = Vec::new();
                for (name, (eb, sig)) in collected {
                    if effects_bytes_out.is_empty() {
                        effects_bytes_out = eb;
                    }
                    signatures.push((name, sig));
                }

                let latency = start.elapsed();
                debug!(
                    digest = hex::encode(&effects_digest[..8]),
                    effects_len = effects_bytes_out.len(),
                    has_events = first_events_bytes.is_some(),
                    stake = accumulated_stake,
                    validators = signatures.len(),
                    latency_ms = latency.as_millis(),
                    "Effects certified with quorum"
                );

                Ok(CertifiedEffects {
                    effects_digest,
                    effects_bytes: effects_bytes_out,
                    signatures,
                    stake_weight: accumulated_stake,
                    events_bytes: first_events_bytes,
                    latency,
                })
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(anyhow!(
                "Effects collection timed out after {:?} (collected {}/{} stake)",
                effects_timeout,
                accumulated_stake,
                quorum_stake
            )),
        }
    }

    fn spawn_effects_query(
        &self,
        validator: AuthorityName,
        tx_digest: &[u8; 32],
        consensus_position: Option<Vec<u8>>,
        effects_tx: tokio::sync::mpsc::Sender<EffectsQueryResult>,
    ) {
        let digest = *tx_digest;
        let monitor = self.validator_monitor.clone();
        let committee = self.committee.read().clone();
        let channels = self.channels.clone();

        tokio::spawn(async move {
            let result = Self::query_effects(
                &validator,
                &digest,
                consensus_position.as_deref(),
                &committee,
                &channels,
            )
            .await;
            match &result {
                Ok(_) => {}
                Err(e) => {
                    monitor.record_failure(&validator, OpType::Effects);
                    warn!(
                        validator = hex::encode(&validator[..8]),
                        error = %e,
                        "Effects query failed"
                    );
                }
            }
            let _ = effects_tx.send(result).await;
        });
    }

    fn spawn_effects_query_static(
        validator: AuthorityName,
        tx_digest: &[u8; 32],
        consensus_position: Option<Vec<u8>>,
        committee: &Arc<Committee>,
        channels: Arc<DashMap<String, Channel>>,
        effects_tx: tokio::sync::mpsc::Sender<EffectsQueryResult>,
    ) {
        let digest = *tx_digest;
        let committee = committee.clone();

        tokio::spawn(async move {
            let result = Self::query_effects(
                &validator,
                &digest,
                consensus_position.as_deref(),
                &committee,
                &channels,
            )
            .await;
            let _ = effects_tx.send(result).await;
        });
    }

    /// Query a single validator for effects via WaitForEffects gRPC.
    ///
    /// Blocks until the validator has executed the transaction and returns
    /// the BCS-encoded effects and events.
    async fn query_effects(
        validator: &AuthorityName,
        tx_digest: &[u8; 32],
        consensus_position: Option<&[u8]>,
        committee: &Committee,
        channels: &DashMap<String, Channel>,
    ) -> Result<(AuthorityName, Vec<u8>, Vec<u8>, Option<Vec<u8>>)> {
        let validator_info = committee
            .get_validator(validator)
            .ok_or_else(|| anyhow!("No validator info for effects query"))?;

        let endpoint = if validator_info.network_address.starts_with('/') {
            crate::committee::parse_multiaddr(&validator_info.network_address).ok_or_else(|| {
                anyhow!("Unparseable multiaddr: {}", validator_info.network_address)
            })?
        } else {
            validator_info.network_address.clone()
        };

        let channel = if let Some(ch) = channels.get(&endpoint) {
            ch.value().clone()
        } else {
            let ch = Channel::from_shared(endpoint.clone())
                .map_err(|e| anyhow!("Invalid endpoint: {}", e))?
                .connect_timeout(Duration::from_secs(5))
                .tcp_nodelay(true)
                .tcp_keepalive(Some(Duration::from_secs(30)))
                .http2_keep_alive_interval(Duration::from_secs(15))
                .keep_alive_timeout(Duration::from_secs(5))
                .http2_adaptive_window(true)
                .connect_lazy();
            channels.insert(endpoint, ch.clone());
            ch
        };

        let mut client = SuiValidatorClient::new(channel);

        // TransactionDigest BCS = ULEB128(32) + 32 bytes = 33 bytes
        // (Digest uses serde_with::Bytes which calls serialize_bytes in BCS)
        let bcs_digest = validator_proto::bcs_wrap_digest(tx_digest);
        let request = RawWaitForEffectsRequest {
            transaction_digest: Some(bcs_digest),
            consensus_position: consensus_position.map(|pos| pos.to_vec()),
            include_details: true,
            ping_type: None,
        };

        let response = client
            .wait_for_effects(request)
            .await
            .map_err(|e| anyhow!("WaitForEffects RPC failed: {}", e))?;
        let inner = response.into_inner();

        match inner.inner {
            Some(RawValidatorTransactionStatus::Executed(status)) => {
                let effects_bytes = status
                    .details
                    .as_ref()
                    .map(|d| d.effects.clone())
                    .ok_or_else(|| anyhow!("No effects details in WaitForEffects response"))?;
                let events_bytes = status.details.as_ref().and_then(|d| d.events.clone());
                let effects_digest = status.effects_digest;

                debug!(
                    validator = hex::encode(&validator[..8]),
                    effects_len = effects_bytes.len(),
                    has_events = events_bytes.is_some(),
                    "WaitForEffects: received effects"
                );

                Ok((*validator, effects_bytes, effects_digest, events_bytes))
            }
            Some(RawValidatorTransactionStatus::Rejected(rej)) => Err(anyhow!(
                "WaitForEffects rejected: {:?}",
                rej.error.map(|e| hex::encode(&e))
            )),
            Some(RawValidatorTransactionStatus::Expired(exp)) => {
                Err(anyhow!("WaitForEffects expired at epoch {}", exp.epoch))
            }
            None => Err(anyhow!("Empty WaitForEffects response")),
        }
    }

    /// Parse a 32-byte effects digest from validator response bytes.
    fn parse_effects_digest(bytes: &[u8]) -> Option<[u8; 32]> {
        if bytes.len() != 32 {
            return None;
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(bytes);
        Some(out)
    }

    /// Cryptographic hash fallback for effects comparison.
    fn hash_effects(effects_bytes: &[u8]) -> [u8; 32] {
        use blake2::{digest::consts::U32, Blake2b, Digest};
        type Blake2b256 = Blake2b<U32>;

        let mut hasher = Blake2b256::new();
        hasher.update(effects_bytes);
        let hash = hasher.finalize();

        let mut out = [0u8; 32];
        out.copy_from_slice(&hash);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committee::{Committee, ValidatorInfo};

    fn setup() -> (Arc<ValidatorMonitor>, Arc<RwLock<Arc<Committee>>>) {
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
        (monitor, committee)
    }

    #[test]
    fn test_hash_effects_deterministic() {
        let effects = vec![1u8, 2, 3, 4];
        let h1 = EffectsCertifier::hash_effects(&effects);
        let h2 = EffectsCertifier::hash_effects(&effects);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_effects_different_inputs() {
        let h1 = EffectsCertifier::hash_effects(&[1, 2, 3]);
        let h2 = EffectsCertifier::hash_effects(&[4, 5, 6]);
        assert_ne!(h1, h2);
    }

    #[tokio::test]
    async fn test_certify_fast_path() {
        let (monitor, committee) = setup();
        let certifier = EffectsCertifier::new(
            monitor,
            committee,
            Duration::from_secs(10),
            Duration::from_millis(100),
            5,
        );

        let mut validator = [0u8; 32];
        validator[0] = 0;

        let submit_result = SubmitResult {
            validator,
            status: SubmitStatus::Executed,
            effects_bytes: Some(vec![1, 2, 3, 4]),
            events_bytes: Some(vec![5, 6, 7]),
            consensus_position: None,
            latency: Duration::from_millis(10),
        };

        let digest = [42u8; 32];
        let certified = certifier.certify(&digest, &submit_result).await.unwrap();
        assert!(!certified.effects_bytes.is_empty());
        assert_eq!(certified.signatures.len(), 1);
    }
}
