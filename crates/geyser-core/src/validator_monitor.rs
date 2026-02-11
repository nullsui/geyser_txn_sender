use crate::committee::{AuthorityName, Committee};
use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, trace, warn};

/// Operation types for per-operation latency tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpType {
    Submit,
    Effects,
    FastPath,
    Consensus,
    Ping,
}

/// Health state for a single validator.
#[derive(Debug, Clone)]
pub struct ValidatorHealth {
    /// EMA latency per operation type (alpha = 0.3).
    pub ema_latency: [f64; 5],
    /// Sliding window of recent latencies for percentile calculation.
    pub recent_latencies: VecDeque<f64>,
    /// Total successful operations.
    pub success_count: u64,
    /// Total failed operations.
    pub error_count: u64,
    /// Consecutive failures (reset on success).
    pub consecutive_failures: u32,
    /// Last successful contact.
    pub last_success: Option<Instant>,
    /// Network address.
    pub address: String,
}

const EMA_ALPHA: f64 = 0.3;
const LATENCY_WINDOW: usize = 100;

impl ValidatorHealth {
    pub fn new(address: String) -> Self {
        Self {
            ema_latency: [f64::MAX; 5],
            recent_latencies: VecDeque::with_capacity(LATENCY_WINDOW),
            success_count: 0,
            error_count: 0,
            consecutive_failures: 0,
            last_success: None,
            address,
        }
    }

    /// Record a successful operation latency.
    pub fn record_success(&mut self, op: OpType, latency: Duration) {
        let ms = latency.as_secs_f64() * 1000.0;
        let idx = op as usize;

        // Update EMA
        if self.ema_latency[idx] == f64::MAX {
            self.ema_latency[idx] = ms;
        } else {
            self.ema_latency[idx] = EMA_ALPHA * ms + (1.0 - EMA_ALPHA) * self.ema_latency[idx];
        }

        // Update sliding window
        if self.recent_latencies.len() >= LATENCY_WINDOW {
            self.recent_latencies.pop_front();
        }
        self.recent_latencies.push_back(ms);

        self.success_count += 1;
        self.consecutive_failures = 0;
        self.last_success = Some(Instant::now());
    }

    /// Record a failed operation.
    pub fn record_failure(&mut self, _op: OpType) {
        self.error_count += 1;
        self.consecutive_failures += 1;
    }

    /// Get the EMA latency for a specific operation type (in ms).
    pub fn latency(&self, op: OpType) -> f64 {
        self.ema_latency[op as usize]
    }

    /// Is this validator considered healthy?
    pub fn is_healthy(&self) -> bool {
        self.consecutive_failures < 5
    }

    /// P50 latency from the sliding window.
    pub fn p50(&self) -> Option<f64> {
        self.percentile(0.50)
    }

    /// P99 latency from the sliding window.
    pub fn p99(&self) -> Option<f64> {
        self.percentile(0.99)
    }

    fn percentile(&self, p: f64) -> Option<f64> {
        if self.recent_latencies.is_empty() {
            return None;
        }
        let mut sorted: Vec<f64> = self.recent_latencies.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
        Some(sorted[idx])
    }
}

/// Monitors all validators, provides latency-ranked selection.
pub struct ValidatorMonitor {
    /// Health data per validator.
    health: DashMap<AuthorityName, ValidatorHealth>,
    /// Current committee reference.
    committee: Arc<RwLock<Arc<Committee>>>,
    /// Latency delta for selection (default: 0.02 = 2%).
    latency_delta: f64,
    /// Health ping interval.
    ping_interval: Duration,
}

impl ValidatorMonitor {
    pub fn new(
        committee: Arc<RwLock<Arc<Committee>>>,
        latency_delta: f64,
        ping_interval: Duration,
    ) -> Self {
        Self {
            health: DashMap::new(),
            committee,
            latency_delta,
            ping_interval,
        }
    }

    /// Initialize health entries for all current committee validators.
    pub fn init_from_committee(&self) {
        let committee = self.committee.read().clone();
        for validator in &committee.validators {
            self.health
                .entry(validator.name)
                .or_insert_with(|| ValidatorHealth::new(validator.network_address.clone()));
        }
        debug!(
            count = committee.validators.len(),
            "Initialized validator health entries"
        );
    }

    /// Record a successful operation for a validator.
    pub fn record_success(&self, name: &AuthorityName, op: OpType, latency: Duration) {
        if let Some(mut health) = self.health.get_mut(name) {
            health.record_success(op, latency);
        }
    }

    /// Record a failed operation for a validator.
    pub fn record_failure(&self, name: &AuthorityName, op: OpType) {
        if let Some(mut health) = self.health.get_mut(name) {
            health.record_failure(op);
        }
    }

    /// Select the best validators by latency, matching Sui's proven algorithm.
    ///
    /// Algorithm (from request_retrier.rs:42-76):
    /// 1. Sort validators by EMA latency ascending
    /// 2. Find the fastest latency
    /// 3. Select all validators within `latency_delta` of the fastest
    /// 4. Shuffle that tier for load balancing
    /// 5. Append remaining validators sorted by latency
    /// 6. Return top `max` from the merged ordering
    pub fn select_validators(&self, max: usize, op: OpType) -> Vec<AuthorityName> {
        // Default EMA for validators with no data for this op type.
        // This sorts untried validators between known-good (Executed ~65ms)
        // and known-bad (Submitted ~700ms pipeline latency), enabling discovery
        // while preserving converged validator preferences.
        const NO_DATA_DEFAULT_MS: f64 = 150.0;

        let mut candidates: Vec<(AuthorityName, f64)> = self
            .health
            .iter()
            .filter(|entry| entry.value().is_healthy())
            .map(|entry| {
                let lat = entry.value().latency(op);
                let effective = if lat == f64::MAX {
                    NO_DATA_DEFAULT_MS
                } else {
                    lat
                };
                (*entry.key(), effective)
            })
            .collect();

        if candidates.is_empty() {
            // Fallback: return all validators regardless of health
            warn!("No healthy validators, falling back to all known validators");
            let committee = self.committee.read().clone();
            return committee
                .validators
                .iter()
                .take(max)
                .map(|v| v.name)
                .collect();
        }

        // Sort by latency ascending
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let fastest = candidates[0].1;
        let threshold = fastest * (1.0 + self.latency_delta);

        // Collect all within delta of fastest
        let mut tier: Vec<AuthorityName> = candidates
            .iter()
            .take_while(|(_, lat)| *lat <= threshold)
            .map(|(name, _)| *name)
            .collect();

        // Shuffle the tier for load balancing
        let mut rng = rand::thread_rng();
        for i in (1..tier.len()).rev() {
            let j = rng.gen_range(0..=i);
            tier.swap(i, j);
        }

        // Append remaining validators in latency order so we can still fill `max`.
        let mut selected = tier;
        for (name, _) in candidates {
            if !selected.contains(&name) {
                selected.push(name);
            }
        }
        selected.truncate(max);

        trace!(
            fastest_ms = fastest,
            threshold_ms = threshold,
            selected_size = selected.len(),
            "Selected validators"
        );

        selected
    }

    /// Return the top N validators by latency with their parsed endpoint URLs.
    /// Used for pre-warming gRPC channels on startup.
    pub fn top_endpoints(&self, count: usize, op: OpType) -> Vec<(AuthorityName, String)> {
        let committee = self.committee.read().clone();

        let mut candidates: Vec<(AuthorityName, f64, String)> = self
            .health
            .iter()
            .filter(|entry| entry.value().is_healthy())
            .filter_map(|entry| {
                let name = *entry.key();
                // Use requested op EMA, falling back to Ping EMA for
                // validators with no data (e.g. at startup before any TXs).
                let lat = entry.value().latency(op);
                let effective = if lat == f64::MAX {
                    let ping = entry.value().latency(OpType::Ping);
                    if ping == f64::MAX {
                        return None;
                    } else {
                        ping
                    }
                } else {
                    lat
                };
                // Look up the validator's network address from the committee
                let info = committee.get_validator(&name)?;
                let raw_addr = &info.network_address;
                let endpoint = if raw_addr.starts_with('/') {
                    crate::committee::parse_multiaddr(raw_addr)?
                } else {
                    raw_addr.clone()
                };
                Some((name, effective, endpoint))
            })
            .collect();

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(count);
        candidates
            .into_iter()
            .map(|(name, _, url)| (name, url))
            .collect()
    }

    /// Get health summary for all validators (for /health endpoint).
    pub fn health_summary(&self) -> Vec<ValidatorHealthSummary> {
        self.health
            .iter()
            .map(|entry| {
                let h = entry.value();
                ValidatorHealthSummary {
                    name: *entry.key(),
                    address: h.address.clone(),
                    ema_submit_ms: h.latency(OpType::Submit),
                    ema_effects_ms: h.latency(OpType::Effects),
                    p50_ms: h.p50(),
                    p99_ms: h.p99(),
                    success_count: h.success_count,
                    error_count: h.error_count,
                    consecutive_failures: h.consecutive_failures,
                    healthy: h.is_healthy(),
                }
            })
            .collect()
    }

    /// Start the background health ping task.
    pub fn start_ping_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let monitor = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = time::interval(monitor.ping_interval);
            loop {
                interval.tick().await;

                // Add jitter: 0-5s random delay
                let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..5000));
                time::sleep(jitter).await;

                monitor.ping_all_validators().await;
            }
        })
    }

    /// TCP-probe all validators and seed Submit latency data.
    /// Call this on startup before accepting traffic so the first TX
    /// races the closest validators instead of picking randomly.
    pub async fn probe_and_seed(&self) {
        use tokio::net::TcpStream;

        let committee = self.committee.read().clone();
        let mut handles = Vec::new();

        for validator in &committee.validators {
            let name = validator.name;
            let raw_addr = validator.network_address.clone();

            handles.push(tokio::spawn(async move {
                // Parse multiaddr → http://host:port, then extract host:port for TCP connect
                let url = if raw_addr.starts_with('/') {
                    match crate::committee::parse_multiaddr(&raw_addr) {
                        Some(u) => u,
                        None => return None,
                    }
                } else {
                    raw_addr
                };

                let addr = url
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");

                let start = Instant::now();
                let result =
                    tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr)).await;
                let latency = start.elapsed();

                match result {
                    Ok(Ok(_)) => Some((name, latency)),
                    _ => None,
                }
            }));
        }

        let mut probed = 0u32;
        let mut total = 0u32;
        for handle in handles {
            total += 1;
            if let Ok(Some((name, latency))) = handle.await {
                if let Some(mut h) = self.health.get_mut(&name) {
                    // Seed Ping latency (NOT Submit) — TCP probe only measures RTT,
                    // not whether the validator returns Executed vs Submitted.
                    // Submit EMA is left at f64::MAX until real TX data arrives.
                    h.record_success(OpType::Ping, latency);
                    probed += 1;
                }
            }
        }

        debug!(
            probed,
            total, "Validator probe complete — Submit latency seeded"
        );
    }

    /// Ping all validators and update health.
    async fn ping_all_validators(&self) {
        use tokio::net::TcpStream;

        let committee = self.committee.read().clone();
        let mut handles = Vec::new();

        for validator in &committee.validators {
            let name = validator.name;
            let raw_addr = validator.network_address.clone();
            let health = self.health.clone();

            handles.push(tokio::spawn(async move {
                // Parse multiaddr → URL → host:port for TCP probe
                let url = if raw_addr.starts_with('/') {
                    match crate::committee::parse_multiaddr(&raw_addr) {
                        Some(u) => u,
                        None => return,
                    }
                } else {
                    raw_addr
                };

                let addr = url
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");

                let start = Instant::now();
                let result =
                    tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr)).await;
                let latency = start.elapsed();

                if let Some(mut h) = health.get_mut(&name) {
                    match result {
                        Ok(Ok(_)) => h.record_success(OpType::Ping, latency),
                        _ => h.record_failure(OpType::Ping),
                    }
                }
            }));
        }

        // Wait for all pings to complete
        for handle in handles {
            let _ = handle.await;
        }

        debug!("Completed health ping cycle");
    }
}

/// Summary of a validator's health (for API responses).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidatorHealthSummary {
    #[serde(with = "hex_name")]
    pub name: AuthorityName,
    pub address: String,
    pub ema_submit_ms: f64,
    pub ema_effects_ms: f64,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub success_count: u64,
    pub error_count: u64,
    pub consecutive_failures: u32,
    pub healthy: bool,
}

mod hex_name {
    use serde::Serializer;

    pub fn serialize<S: Serializer>(name: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committee::{Committee, ValidatorInfo};

    fn test_monitor() -> (Arc<ValidatorMonitor>, Vec<AuthorityName>) {
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
        let names: Vec<AuthorityName> = validators.iter().map(|v| v.name).collect();
        let committee = Arc::new(RwLock::new(Arc::new(Committee::new(1, validators))));
        let monitor = Arc::new(ValidatorMonitor::new(
            committee,
            0.02,
            Duration::from_secs(10),
        ));
        monitor.init_from_committee();
        (monitor, names)
    }

    #[test]
    fn test_record_and_select() {
        let (monitor, names) = test_monitor();

        // Record varying latencies — first two are within 2% of each other
        monitor.record_success(&names[0], OpType::Submit, Duration::from_millis(100));
        monitor.record_success(&names[1], OpType::Submit, Duration::from_millis(101));
        monitor.record_success(&names[2], OpType::Submit, Duration::from_millis(500));
        monitor.record_success(&names[3], OpType::Submit, Duration::from_millis(1000));

        let selected = monitor.select_validators(2, OpType::Submit);
        assert_eq!(selected.len(), 2);
        // Both 100ms and 101ms are within 2% of fastest (100ms threshold = 102ms)
        assert!(selected.contains(&names[0]));
        assert!(selected.contains(&names[1]));
    }

    #[test]
    fn test_health_tracking() {
        let (monitor, names) = test_monitor();

        // Record failures
        for _ in 0..5 {
            monitor.record_failure(&names[0], OpType::Submit);
        }

        // Validator 0 should be unhealthy now
        let summary = monitor.health_summary();
        let v0 = summary.iter().find(|s| s.name == names[0]).unwrap();
        assert!(!v0.healthy);
        assert_eq!(v0.consecutive_failures, 5);

        // Other validators should still be healthy
        let v1 = summary.iter().find(|s| s.name == names[1]).unwrap();
        assert!(v1.healthy);
    }

    #[test]
    fn test_select_with_equal_latencies() {
        let (monitor, names) = test_monitor();

        // All same latency → all within delta → random selection
        for name in &names {
            monitor.record_success(name, OpType::Submit, Duration::from_millis(50));
        }

        let selected = monitor.select_validators(4, OpType::Submit);
        assert_eq!(selected.len(), 4);
    }
}
