use crate::analyzer::{TxAnalyzer, TxPath};
use crate::committee::CommitteeManager;
use crate::config::GeyserConfig;
use crate::effects_certifier::{CertifiedEffects, EffectsCertifier, EffectsCertifyOverrides};
use crate::epoch_monitor::EpochMonitor;
use crate::gas_pool::GasPool;
use crate::metrics::GeyserMetrics;
use crate::stats::StatsCollector;
use crate::tx_submitter::{SubmitOverrides, TxSubmitter};
use crate::validator_monitor::{OpType, ValidatorMonitor};
use anyhow::Result;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Result of driving a transaction to finality.
#[derive(Debug, Clone)]
pub struct DriveResult {
    /// The transaction digest.
    pub tx_digest: [u8; 32],
    /// Certified effects.
    pub effects: CertifiedEffects,
    /// Transaction path (owned vs shared).
    pub tx_path: TxPath,
    /// Total time from submission to certified effects.
    pub total_latency: Duration,
    /// Time spent in the submit phase (SubmitTransaction RPCs).
    pub submit_latency: Duration,
    /// Number of validators contacted.
    pub validators_contacted: usize,
}

/// Optional per-request tuning overrides for the direct validator path.
#[derive(Debug, Clone, Default)]
pub struct DriveOverrides {
    pub max_amplification: Option<usize>,
    pub owned_amplification: Option<usize>,
    pub max_retries: Option<usize>,
    pub submit_timeout: Option<Duration>,
    pub initial_retry_delay: Option<Duration>,
    pub max_retry_delay: Option<Duration>,
    pub effects_timeout: Option<Duration>,
    pub effects_fallback_delay: Option<Duration>,
    pub effects_fallback_fanout: Option<usize>,
}

/// Orchestrates the full transaction lifecycle:
/// analyze → submit → certify effects → return.
pub struct QuorumDriver {
    submitter: Arc<TxSubmitter>,
    certifier: Arc<EffectsCertifier>,
    analyzer: Arc<TxAnalyzer>,
    validator_monitor: Arc<ValidatorMonitor>,
    gas_pool: Option<Arc<GasPool>>,
    epoch_monitor: Arc<EpochMonitor>,
    stats: Arc<StatsCollector>,
    metrics: Arc<GeyserMetrics>,
    max_amplification: usize,
    owned_amplification: usize,
    max_retries: usize,
}

impl QuorumDriver {
    pub fn new(
        submitter: Arc<TxSubmitter>,
        certifier: Arc<EffectsCertifier>,
        analyzer: Arc<TxAnalyzer>,
        validator_monitor: Arc<ValidatorMonitor>,
        gas_pool: Option<Arc<GasPool>>,
        epoch_monitor: Arc<EpochMonitor>,
        stats: Arc<StatsCollector>,
        metrics: Arc<GeyserMetrics>,
        owned_amplification: usize,
        max_amplification: usize,
    ) -> Self {
        Self {
            submitter,
            certifier,
            analyzer,
            validator_monitor,
            gas_pool,
            epoch_monitor,
            stats,
            metrics,
            max_amplification,
            owned_amplification,
            max_retries: 3,
        }
    }

    /// Build a QuorumDriver from a GeyserConfig and shared components.
    pub fn from_config(
        config: &GeyserConfig,
        committee_manager: &CommitteeManager,
        validator_monitor: Arc<ValidatorMonitor>,
        gas_pool: Option<Arc<GasPool>>,
        epoch_monitor: Arc<EpochMonitor>,
        stats: Arc<StatsCollector>,
        metrics: Arc<GeyserMetrics>,
    ) -> Self {
        let committee = Arc::new(RwLock::new(committee_manager.get()));
        let analyzer = Arc::new(TxAnalyzer::new());

        let submitter = Arc::new(TxSubmitter::new(
            validator_monitor.clone(),
            committee.clone(),
            analyzer.clone(),
            config.submit_timeout(),
            config.initial_retry_delay(),
            config.max_retry_delay(),
        ));

        let certifier = Arc::new(EffectsCertifier::new(
            validator_monitor.clone(),
            committee,
            config.effects_timeout(),
            config.effects_fallback_delay(),
            config.quorum.effects_fallback_fanout,
        ));

        Self::new(
            submitter,
            certifier,
            analyzer,
            validator_monitor,
            gas_pool,
            epoch_monitor,
            stats,
            metrics,
            config.quorum.owned_amplification,
            config.quorum.max_amplification,
        )
    }

    /// Drive a signed transaction to finality.
    ///
    /// Flow:
    /// 1. Check epoch status (accepting TXs?)
    /// 2. Analyze TX (owned vs shared objects)
    /// 3. Compute amplification factor
    /// 4. Submit to validators with retry
    /// 5. Certify effects (collect from quorum)
    /// 6. Record stats and metrics
    pub async fn drive(
        &self,
        tx_bytes: &[u8],
        signatures: &[Vec<u8>],
        tx_digest: [u8; 32],
        tx_kind_json: Option<&serde_json::Value>,
    ) -> Result<DriveResult> {
        self.drive_with_overrides(tx_bytes, signatures, tx_digest, tx_kind_json, None)
            .await
    }

    /// Drive a signed transaction with optional per-request tuning overrides.
    pub async fn drive_with_overrides(
        &self,
        tx_bytes: &[u8],
        signatures: &[Vec<u8>],
        tx_digest: [u8; 32],
        tx_kind_json: Option<&serde_json::Value>,
        overrides: Option<&DriveOverrides>,
    ) -> Result<DriveResult> {
        let timer = Instant::now();

        // 1. Check epoch status
        self.epoch_monitor.check_accepting()?;

        // 2. Analyze TX path
        let tx_path = match tx_kind_json {
            Some(json) => self.analyzer.classify_from_tx_kind(json),
            None => self.analyzer.classify_heuristic(tx_bytes),
        };

        // 3. Compute amplification
        let max_amplification = overrides
            .and_then(|o| o.max_amplification)
            .unwrap_or(self.max_amplification)
            .max(1);
        let owned_amplification = overrides
            .and_then(|o| o.owned_amplification)
            .unwrap_or(self.owned_amplification)
            .max(1)
            .min(max_amplification);
        let max_retries = overrides
            .and_then(|o| o.max_retries)
            .unwrap_or(self.max_retries);

        let amplification = match tx_path {
            TxPath::OwnedOnly => owned_amplification,
            TxPath::Shared => max_amplification,
        };

        let submit_overrides = SubmitOverrides {
            submit_timeout: overrides.and_then(|o| o.submit_timeout),
            initial_retry_delay: overrides.and_then(|o| o.initial_retry_delay),
            max_retry_delay: overrides.and_then(|o| o.max_retry_delay),
        };
        let effects_overrides = EffectsCertifyOverrides {
            effects_timeout: overrides.and_then(|o| o.effects_timeout),
            effects_fallback_delay: overrides.and_then(|o| o.effects_fallback_delay),
            effects_fallback_fanout: overrides.and_then(|o| o.effects_fallback_fanout),
        };

        debug!(
            digest = hex::encode(&tx_digest[..8]),
            path = %tx_path,
            amplification,
            "Driving transaction"
        );

        // 4. Submit with retry
        let submit_result = self
            .submitter
            .submit_with_retry_overrides(
                tx_bytes,
                signatures,
                &tx_digest,
                amplification,
                max_retries,
                Some(&submit_overrides),
            )
            .await?;

        let submit_latency = submit_result.latency;

        // 5. Certify effects
        let effects = self
            .certifier
            .certify_with_overrides(&tx_digest, &submit_result, Some(&effects_overrides))
            .await?;

        let total_latency = timer.elapsed();

        // 6. Correct validator EMA with full pipeline latency.
        // TxSubmitter records the fast ack time (e.g. 13ms for Submitted),
        // but validators returning Executed inline (e.g. 65ms) are actually
        // faster end-to-end. Recording total_latency as a Submit sample
        // penalizes Submitted validators (13ms ack + 500ms effects = 513ms)
        // and rewards Executed validators (65ms total), so the monitor
        // converges on validators that return effects inline.
        self.validator_monitor.record_success(
            &submit_result.validator,
            OpType::Submit,
            total_latency,
        );

        // 7. Record stats
        self.stats.record(tx_path, total_latency);
        self.metrics.observe_finality(tx_path, total_latency);
        self.metrics.observe_submit(tx_path, submit_latency);
        self.metrics.inc_validators_contacted(amplification);

        info!(
            digest = hex::encode(&tx_digest[..8]),
            path = %tx_path,
            total_ms = total_latency.as_millis(),
            submit_ms = submit_latency.as_millis(),
            effects_ms = effects.latency.as_millis(),
            validators = amplification,
            "Transaction finalized"
        );

        Ok(DriveResult {
            tx_digest,
            effects,
            tx_path,
            total_latency,
            submit_latency,
            validators_contacted: amplification,
        })
    }

    /// Get a reference to the gas pool (if available).
    pub fn gas_pool(&self) -> Option<&Arc<GasPool>> {
        self.gas_pool.as_ref()
    }

    /// Get a reference to the stats collector.
    pub fn stats(&self) -> &Arc<StatsCollector> {
        &self.stats
    }
}
