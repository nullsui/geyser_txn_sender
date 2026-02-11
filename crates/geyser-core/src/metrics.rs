use crate::analyzer::TxPath;
use prometheus::{
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    HistogramVec, IntCounter, IntCounterVec, IntGauge,
};
use std::time::Duration;

/// Prometheus metrics for Geyser.
pub struct GeyserMetrics {
    /// TX finality latency (submission → certified effects).
    pub tx_finality_seconds: HistogramVec,
    /// TX submission latency (to validators).
    pub tx_submit_seconds: HistogramVec,
    /// TX effects collection latency.
    pub tx_effects_seconds: HistogramVec,
    /// Total transactions processed.
    pub txs_total: IntCounterVec,
    /// Total validators contacted.
    pub validators_contacted: IntCounter,
    /// Gas pool available coins.
    pub gas_pool_available: IntGauge,
    /// Gas pool in-use coins.
    pub gas_pool_in_use: IntGauge,
    /// Epoch transitions.
    pub epoch_transitions: IntCounter,
    /// Endpoint latency tracking.
    pub endpoint_latency_seconds: HistogramVec,
}

impl GeyserMetrics {
    pub fn new() -> Self {
        let buckets = vec![
            0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0,
        ];

        Self {
            tx_finality_seconds: register_histogram_vec!(
                "geyser_tx_finality_seconds",
                "Transaction finality latency in seconds",
                &["path", "mode"],
                buckets.clone()
            )
            .expect("Failed to register tx_finality_seconds"),

            tx_submit_seconds: register_histogram_vec!(
                "geyser_tx_submit_seconds",
                "Transaction submission latency in seconds",
                &["path"],
                buckets.clone()
            )
            .expect("Failed to register tx_submit_seconds"),

            tx_effects_seconds: register_histogram_vec!(
                "geyser_tx_effects_seconds",
                "Effects collection latency in seconds",
                &["path"],
                buckets.clone()
            )
            .expect("Failed to register tx_effects_seconds"),

            txs_total: register_int_counter_vec!(
                "geyser_txs_total",
                "Total transactions processed",
                &["path", "status"]
            )
            .expect("Failed to register txs_total"),

            validators_contacted: register_int_counter!(
                "geyser_validators_contacted_total",
                "Total validator contacts across all TXs"
            )
            .expect("Failed to register validators_contacted"),

            gas_pool_available: register_int_gauge!(
                "geyser_gas_pool_available",
                "Number of available gas coins"
            )
            .expect("Failed to register gas_pool_available"),

            gas_pool_in_use: register_int_gauge!(
                "geyser_gas_pool_in_use",
                "Number of in-use gas coins"
            )
            .expect("Failed to register gas_pool_in_use"),

            epoch_transitions: register_int_counter!(
                "geyser_epoch_transitions_total",
                "Total epoch transitions observed"
            )
            .expect("Failed to register epoch_transitions"),

            endpoint_latency_seconds: register_histogram_vec!(
                "geyser_endpoint_latency_seconds",
                "Per-endpoint latency in seconds",
                &["endpoint", "operation"],
                buckets
            )
            .expect("Failed to register endpoint_latency_seconds"),
        }
    }

    /// Record TX finality latency.
    pub fn observe_finality(&self, path: TxPath, latency: Duration) {
        self.tx_finality_seconds
            .with_label_values(&[&path.to_string(), "geyser"])
            .observe(latency.as_secs_f64());
        self.txs_total
            .with_label_values(&[&path.to_string(), "success"])
            .inc();
    }

    /// Record TX submission latency.
    pub fn observe_submit(&self, path: TxPath, latency: Duration) {
        self.tx_submit_seconds
            .with_label_values(&[&path.to_string()])
            .observe(latency.as_secs_f64());
    }

    /// Record effects collection latency.
    pub fn observe_effects(&self, path: TxPath, latency: Duration) {
        self.tx_effects_seconds
            .with_label_values(&[&path.to_string()])
            .observe(latency.as_secs_f64());
    }

    /// Record a failed TX.
    pub fn record_failure(&self, path: TxPath) {
        self.txs_total
            .with_label_values(&[&path.to_string(), "failure"])
            .inc();
    }

    /// Increment validators contacted counter.
    pub fn inc_validators_contacted(&self, count: usize) {
        self.validators_contacted.inc_by(count as u64);
    }

    /// Update gas pool gauges.
    pub fn update_gas_pool(&self, available: usize, in_use: usize) {
        self.gas_pool_available.set(available as i64);
        self.gas_pool_in_use.set(in_use as i64);
    }

    /// Record an epoch transition.
    pub fn record_epoch_transition(&self) {
        self.epoch_transitions.inc();
    }

    /// Record per-endpoint latency.
    pub fn observe_endpoint(&self, endpoint: &str, operation: &str, latency: Duration) {
        self.endpoint_latency_seconds
            .with_label_values(&[endpoint, operation])
            .observe(latency.as_secs_f64());
    }

    /// Encode all metrics to Prometheus text format.
    pub fn encode(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for GeyserMetrics {
    fn default() -> Self {
        Self::new()
    }
}
