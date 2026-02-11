use crate::analyzer::TxPath;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::Serialize;
use std::time::Duration;

/// Collects latency statistics using HdrHistogram for accurate percentile reporting.
pub struct StatsCollector {
    /// Histogram for owned-object (fast path) transactions.
    owned: Mutex<Histogram<u64>>,
    /// Histogram for shared-object (consensus) transactions.
    shared: Mutex<Histogram<u64>>,
    /// Histogram for all transactions regardless of path.
    all: Mutex<Histogram<u64>>,
    /// Total transaction count.
    total_count: Mutex<u64>,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            owned: Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000, 3).expect("Failed to create histogram"),
            ),
            shared: Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000, 3).expect("Failed to create histogram"),
            ),
            all: Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000, 3).expect("Failed to create histogram"),
            ),
            total_count: Mutex::new(0),
        }
    }

    /// Record a transaction latency.
    pub fn record(&self, path: TxPath, latency: Duration) {
        let us = latency.as_micros() as u64;

        match path {
            TxPath::OwnedOnly => {
                let _ = self.owned.lock().record(us);
            }
            TxPath::Shared => {
                let _ = self.shared.lock().record(us);
            }
        }

        let _ = self.all.lock().record(us);
        *self.total_count.lock() += 1;
    }

    /// Get a snapshot of current statistics.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            owned: self.histogram_stats(&self.owned.lock()),
            shared: self.histogram_stats(&self.shared.lock()),
            all: self.histogram_stats(&self.all.lock()),
            total_count: *self.total_count.lock(),
        }
    }

    /// Reset all histograms.
    pub fn reset(&self) {
        self.owned.lock().reset();
        self.shared.lock().reset();
        self.all.lock().reset();
        *self.total_count.lock() = 0;
    }

    fn histogram_stats(&self, h: &Histogram<u64>) -> LatencyStats {
        if h.is_empty() {
            return LatencyStats::default();
        }
        LatencyStats {
            count: h.len(),
            min_us: h.min(),
            max_us: h.max(),
            mean_us: h.mean() as u64,
            p50_us: h.value_at_percentile(50.0),
            p95_us: h.value_at_percentile(95.0),
            p99_us: h.value_at_percentile(99.0),
        }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of latency statistics.
#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub owned: LatencyStats,
    pub shared: LatencyStats,
    pub all: LatencyStats,
    pub total_count: u64,
}

/// Latency percentiles in microseconds.
#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencyStats {
    pub count: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
}

impl LatencyStats {
    /// Format latency values as milliseconds for display.
    pub fn format_ms(&self) -> FormattedStats {
        FormattedStats {
            count: self.count,
            min_ms: self.min_us as f64 / 1000.0,
            max_ms: self.max_us as f64 / 1000.0,
            mean_ms: self.mean_us as f64 / 1000.0,
            p50_ms: self.p50_us as f64 / 1000.0,
            p95_ms: self.p95_us as f64 / 1000.0,
            p99_ms: self.p99_us as f64 / 1000.0,
        }
    }
}

/// Formatted latency values in milliseconds.
#[derive(Debug, Clone, Serialize)]
pub struct FormattedStats {
    pub count: u64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

impl std::fmt::Display for FormattedStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} min={:.1}ms p50={:.1}ms p95={:.1}ms p99={:.1}ms max={:.1}ms avg={:.1}ms",
            self.count,
            self.min_ms,
            self.p50_ms,
            self.p95_ms,
            self.p99_ms,
            self.max_ms,
            self.mean_ms,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_snapshot() {
        let stats = StatsCollector::new();

        // Record some owned TX latencies
        for ms in [100, 150, 200, 250, 300] {
            stats.record(TxPath::OwnedOnly, Duration::from_millis(ms));
        }

        // Record some shared TX latencies
        for ms in [500, 600, 700, 800, 900] {
            stats.record(TxPath::Shared, Duration::from_millis(ms));
        }

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.total_count, 10);
        assert_eq!(snapshot.owned.count, 5);
        assert_eq!(snapshot.shared.count, 5);
        assert_eq!(snapshot.all.count, 10);

        // Owned p50 should be around 200ms = 200000us
        let owned_ms = snapshot.owned.format_ms();
        assert!(owned_ms.p50_ms >= 100.0 && owned_ms.p50_ms <= 300.0);
    }

    #[test]
    fn test_empty_stats() {
        let stats = StatsCollector::new();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.total_count, 0);
        assert_eq!(snapshot.owned.count, 0);
    }

    #[test]
    fn test_reset() {
        let stats = StatsCollector::new();
        stats.record(TxPath::OwnedOnly, Duration::from_millis(100));
        assert_eq!(stats.snapshot().total_count, 1);

        stats.reset();
        assert_eq!(stats.snapshot().total_count, 0);
    }

    #[test]
    fn test_display_format() {
        let stats = LatencyStats {
            count: 100,
            min_us: 50_000,
            max_us: 500_000,
            mean_us: 150_000,
            p50_us: 120_000,
            p95_us: 350_000,
            p99_us: 450_000,
        };
        let formatted = stats.format_ms();
        let display = format!("{}", formatted);
        assert!(display.contains("n=100"));
        assert!(display.contains("p50=120.0ms"));
    }
}
