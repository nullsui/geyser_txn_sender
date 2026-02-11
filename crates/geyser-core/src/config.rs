use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Top-level Geyser configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeyserConfig {
    pub server: ServerConfig,
    pub engine: EngineConfig,
    pub gas_pool: GasPoolConfig,
    pub timing: TimingConfig,
    pub quorum: QuorumConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub mode: GeyserMode,
    /// Validator gRPC endpoints. Auto-discovered from committee if empty.
    pub validator_endpoints: Vec<String>,
    /// Fullnode endpoints for fallback/reads.
    pub fullnode_endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasPoolConfig {
    pub pool_size: usize,
    pub min_balance_mist: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingConfig {
    pub health_ping_interval_secs: u64,
    pub submit_timeout_secs: u64,
    pub effects_timeout_secs: u64,
    pub initial_retry_delay_ms: u64,
    pub max_retry_delay_secs: u64,
    pub effects_fallback_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuorumConfig {
    pub max_amplification: usize,
    #[serde(default = "default_owned_amplification")]
    pub owned_amplification: usize,
    #[serde(default = "default_effects_fallback_fanout")]
    pub effects_fallback_fanout: usize,
    pub latency_delta: f64,
    pub read_mask: Vec<String>,
}

const fn default_owned_amplification() -> usize {
    3
}

const fn default_effects_fallback_fanout() -> usize {
    8
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeyserMode {
    /// Talk directly to validators (fastest).
    DirectValidator,
    /// Race across fullnodes (fallback).
    FullnodeProxy,
    /// Try validators first, fall back to fullnodes.
    Hybrid,
}

impl GeyserConfig {
    /// Load config from a TOML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: GeyserConfig = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Load from GEYSER_CONFIG env var or default path.
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("GEYSER_CONFIG").unwrap_or_else(|_| "geyser.toml".to_string());
        Self::from_file(&path)
    }

    pub fn health_ping_interval(&self) -> Duration {
        Duration::from_secs(self.timing.health_ping_interval_secs)
    }

    pub fn submit_timeout(&self) -> Duration {
        Duration::from_secs(self.timing.submit_timeout_secs)
    }

    pub fn effects_timeout(&self) -> Duration {
        Duration::from_secs(self.timing.effects_timeout_secs)
    }

    pub fn initial_retry_delay(&self) -> Duration {
        Duration::from_millis(self.timing.initial_retry_delay_ms)
    }

    pub fn max_retry_delay(&self) -> Duration {
        Duration::from_secs(self.timing.max_retry_delay_secs)
    }

    pub fn effects_fallback_delay(&self) -> Duration {
        Duration::from_millis(self.timing.effects_fallback_delay_ms)
    }
}

impl Default for GeyserConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig { port: 8080 },
            engine: EngineConfig {
                mode: GeyserMode::Hybrid,
                validator_endpoints: vec![],
                fullnode_endpoints: vec!["https://fullnode.mainnet.sui.io:443".to_string()],
            },
            gas_pool: GasPoolConfig {
                pool_size: 10,
                min_balance_mist: 100_000_000,
            },
            timing: TimingConfig {
                health_ping_interval_secs: 10,
                submit_timeout_secs: 10,
                effects_timeout_secs: 10,
                initial_retry_delay_ms: 10,
                max_retry_delay_secs: 5,
                effects_fallback_delay_ms: 20,
            },
            quorum: QuorumConfig {
                max_amplification: 5,
                owned_amplification: default_owned_amplification(),
                effects_fallback_fanout: default_effects_fallback_fanout(),
                latency_delta: 0.02,
                read_mask: vec!["effects".to_string()],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GeyserConfig::default();
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.engine.mode, GeyserMode::Hybrid);
        assert_eq!(config.gas_pool.pool_size, 10);
        assert_eq!(config.timing.initial_retry_delay_ms, 10);
        assert_eq!(config.quorum.max_amplification, 5);
        assert_eq!(config.quorum.owned_amplification, 3);
        assert_eq!(config.quorum.effects_fallback_fanout, 8);
    }

    #[test]
    fn test_duration_helpers() {
        let config = GeyserConfig::default();
        assert_eq!(config.health_ping_interval(), Duration::from_secs(10));
        assert_eq!(config.initial_retry_delay(), Duration::from_millis(10));
        assert_eq!(config.effects_fallback_delay(), Duration::from_millis(20));
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = GeyserConfig::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: GeyserConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.server.port, config.server.port);
        assert_eq!(parsed.engine.mode, config.engine.mode);
    }
}
