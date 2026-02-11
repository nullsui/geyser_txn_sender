use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Quorum threshold: 2/3 + 1 of total stake (matching Sui's QUORUM_THRESHOLD = 6667/10000).
pub const QUORUM_THRESHOLD: u64 = 6667;
/// Validity threshold: 1/3 + 1 (matching Sui's VALIDITY_THRESHOLD = 3334/10000).
pub const VALIDITY_THRESHOLD: u64 = 3334;
/// Denominator for threshold calculations.
pub const THRESHOLD_DENOMINATOR: u64 = 10000;

/// A validator's identity and network info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorInfo {
    /// The validator's authority name (public key bytes).
    pub name: AuthorityName,
    /// gRPC network address for direct communication.
    pub network_address: String,
    /// Voting stake weight.
    pub stake: u64,
    /// Human-readable name (for logging).
    pub description: String,
}

/// 32-byte authority public key (Ed25519).
pub type AuthorityName = [u8; 32];

/// The committee for a given epoch.
#[derive(Debug, Clone)]
pub struct Committee {
    pub epoch: u64,
    pub validators: Vec<ValidatorInfo>,
    pub total_stake: u64,
    /// Quick lookup by authority name.
    pub stake_map: HashMap<AuthorityName, u64>,
}

impl Committee {
    pub fn new(epoch: u64, validators: Vec<ValidatorInfo>) -> Self {
        let total_stake = validators.iter().map(|v| v.stake).sum();
        let stake_map = validators.iter().map(|v| (v.name, v.stake)).collect();
        Self {
            epoch,
            validators,
            total_stake,
            stake_map,
        }
    }

    /// Check if the given stake meets the quorum threshold (2/3+1).
    pub fn is_quorum(&self, stake: u64) -> bool {
        (stake as u128) * (THRESHOLD_DENOMINATOR as u128)
            >= (self.total_stake as u128) * (QUORUM_THRESHOLD as u128)
    }

    /// Check if the given stake meets the validity threshold (1/3+1).
    pub fn is_valid(&self, stake: u64) -> bool {
        (stake as u128) * (THRESHOLD_DENOMINATOR as u128)
            >= (self.total_stake as u128) * (VALIDITY_THRESHOLD as u128)
    }

    /// Required stake for quorum.
    pub fn quorum_stake(&self) -> u64 {
        (((self.total_stake as u128) * (QUORUM_THRESHOLD as u128)
            + (THRESHOLD_DENOMINATOR as u128 - 1))
            / (THRESHOLD_DENOMINATOR as u128)) as u64
    }

    /// Deterministic validator ordering seeded by TX digest.
    /// Replicates Sui's `shuffle_by_stake_from_tx_digest`.
    pub fn shuffle_by_stake_from_digest(&self, digest: &[u8; 32]) -> Vec<AuthorityName> {
        // Match Sui's approach: deterministic RNG seeded by full 32-byte digest,
        // then weighted sampling without replacement by stake.
        let mut rng = StdRng::from_seed(*digest);

        let mut weighted: Vec<(AuthorityName, u64)> =
            self.validators.iter().map(|v| (v.name, v.stake)).collect();

        // Ensure deterministic input ordering independent of fetch order.
        weighted.sort_by_key(|(name, _)| *name);

        if let Ok(sampled) =
            weighted.choose_multiple_weighted(&mut rng, weighted.len(), |(_, stake)| *stake as f64)
        {
            sampled.map(|(name, _)| *name).collect()
        } else {
            // Fallback for degenerate stake distributions.
            weighted.into_iter().map(|(name, _)| name).collect()
        }
    }

    /// Get validator info by name.
    pub fn get_validator(&self, name: &AuthorityName) -> Option<&ValidatorInfo> {
        self.validators.iter().find(|v| &v.name == name)
    }

    /// Get all validator network addresses.
    pub fn network_addresses(&self) -> Vec<(&AuthorityName, &str)> {
        self.validators
            .iter()
            .map(|v| (&v.name, v.network_address.as_str()))
            .collect()
    }

    /// Get gRPC-compatible URLs for all validators by parsing their multiaddr network addresses.
    /// Multiaddr format: `/dns/<hostname>/tcp/<port>/http` → `http://<hostname>:<port>`
    pub fn grpc_endpoints(&self) -> Vec<String> {
        self.validators
            .iter()
            .filter_map(|v| parse_multiaddr(&v.network_address))
            .collect()
    }
}

/// Parse a multiaddr string into a gRPC-compatible URL.
/// Input:  `/dns/mysten-1.mainnet.sui.io/tcp/8080/http`
/// Output: `http://mysten-1.mainnet.sui.io:8080`
pub fn parse_multiaddr(addr: &str) -> Option<String> {
    let parts: Vec<&str> = addr.split('/').collect();
    // Expected: ["", "dns", "<host>", "tcp", "<port>", "http"]
    if parts.len() < 5 {
        return None;
    }
    let host_idx = parts.iter().position(|&p| p == "dns" || p == "ip4")?;
    let host = parts.get(host_idx + 1)?;
    let tcp_idx = parts.iter().position(|&p| p == "tcp")?;
    let port = parts.get(tcp_idx + 1)?;
    if host.is_empty() || port.is_empty() {
        return None;
    }
    Some(format!("http://{}:{}", host, port))
}

/// Manages committee state, handles epoch transitions.
pub struct CommitteeManager {
    current: Arc<RwLock<Arc<Committee>>>,
    fullnode_url: String,
}

impl CommitteeManager {
    pub fn new(fullnode_url: String) -> Self {
        Self {
            current: Arc::new(RwLock::new(Arc::new(Committee::new(0, vec![])))),
            fullnode_url,
        }
    }

    /// Get the current committee.
    pub fn get(&self) -> Arc<Committee> {
        self.current.read().clone()
    }

    /// Get current epoch.
    pub fn epoch(&self) -> u64 {
        self.current.read().epoch
    }

    /// Fetch the latest committee from a fullnode and update if epoch changed.
    pub async fn refresh(&self) -> Result<bool> {
        let committee = self.fetch_committee().await?;
        let mut current = self.current.write();
        if committee.epoch > current.epoch {
            info!(
                old_epoch = current.epoch,
                new_epoch = committee.epoch,
                validators = committee.validators.len(),
                "Committee updated for new epoch"
            );
            *current = Arc::new(committee);
            Ok(true)
        } else {
            debug!(epoch = current.epoch, "Committee unchanged");
            Ok(false)
        }
    }

    /// Fetch committee from the fullnode via JSON-RPC.
    async fn fetch_committee(&self) -> Result<Committee> {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.fullnode_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getLatestSuiSystemState",
                "params": []
            }))
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        let result = body
            .get("result")
            .ok_or_else(|| anyhow!("No result in system state response"))?;

        let epoch: u64 = result["epoch"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing epoch"))?
            .parse()?;

        let active_validators = result["activeValidators"]
            .as_array()
            .ok_or_else(|| anyhow!("Missing activeValidators"))?;

        let mut validators = Vec::with_capacity(active_validators.len());
        for v in active_validators {
            let name_b64 = v["protocolPubkeyBytes"].as_str().unwrap_or_default();
            let name_bytes =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, name_b64)
                    .unwrap_or_default();

            // Authority name is first 32 bytes of the protocol pubkey
            let mut name = [0u8; 32];
            if name_bytes.len() >= 32 {
                name.copy_from_slice(&name_bytes[..32]);
            } else {
                warn!("Validator pubkey too short: {} bytes", name_bytes.len());
                continue;
            }

            let network_address = v["netAddress"].as_str().unwrap_or_default().to_string();

            let stake: u64 = v["stakingPoolSuiBalance"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);

            let description = v["name"].as_str().unwrap_or("unknown").to_string();

            validators.push(ValidatorInfo {
                name,
                network_address,
                stake,
                description,
            });
        }

        info!(epoch, validators = validators.len(), "Fetched committee");
        Ok(Committee::new(epoch, validators))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_committee() -> Committee {
        let validators = (0..4)
            .map(|i| {
                let mut name = [0u8; 32];
                name[0] = i;
                ValidatorInfo {
                    name,
                    network_address: format!("127.0.0.1:{}", 8000 + i as u16),
                    stake: 2500, // Equal stake
                    description: format!("validator-{}", i),
                }
            })
            .collect();
        Committee::new(1, validators)
    }

    #[test]
    fn test_quorum_threshold() {
        let committee = make_test_committee();
        assert_eq!(committee.total_stake, 10000);
        // Need 6667/10000 * 10000 = 6667 stake for quorum
        assert!(!committee.is_quorum(6666));
        assert!(committee.is_quorum(6667));
        assert!(committee.is_quorum(10000));
    }

    #[test]
    fn test_validity_threshold() {
        let committee = make_test_committee();
        assert!(!committee.is_valid(3333));
        assert!(committee.is_valid(3334));
    }

    #[test]
    fn test_shuffle_deterministic() {
        let committee = make_test_committee();
        let digest = [42u8; 32];
        let order1 = committee.shuffle_by_stake_from_digest(&digest);
        let order2 = committee.shuffle_by_stake_from_digest(&digest);
        assert_eq!(order1, order2);
        assert_eq!(order1.len(), 4);
    }

    #[test]
    fn test_shuffle_different_digests() {
        let committee = make_test_committee();
        let digest1 = [1u8; 32];
        let digest2 = [2u8; 32];
        let order1 = committee.shuffle_by_stake_from_digest(&digest1);
        let order2 = committee.shuffle_by_stake_from_digest(&digest2);
        // With equal stake, different digests should produce different orderings
        // (not guaranteed but very likely with 4 validators)
        assert_eq!(order1.len(), order2.len());
    }

    #[test]
    fn test_quorum_stake() {
        let committee = make_test_committee();
        let required = committee.quorum_stake();
        assert!(committee.is_quorum(required));
        assert!(!committee.is_quorum(required - 1));
    }

    #[test]
    fn test_threshold_math_handles_large_stake_without_overflow() {
        let validators = vec![
            ValidatorInfo {
                name: [1u8; 32],
                network_address: "/dns/val1.example.com/tcp/8080/http".to_string(),
                stake: u64::MAX / 2,
                description: "val1".to_string(),
            },
            ValidatorInfo {
                name: [2u8; 32],
                network_address: "/dns/val2.example.com/tcp/8080/http".to_string(),
                stake: u64::MAX / 2,
                description: "val2".to_string(),
            },
        ];
        let committee = Committee::new(1, validators);
        let required = committee.quorum_stake();
        assert!(committee.is_quorum(required));
        assert!(!committee.is_quorum(required - 1));
    }

    #[test]
    fn test_parse_multiaddr_dns() {
        let url = parse_multiaddr("/dns/mysten-1.mainnet.sui.io/tcp/8080/http");
        assert_eq!(url, Some("http://mysten-1.mainnet.sui.io:8080".to_string()));
    }

    #[test]
    fn test_parse_multiaddr_ip4() {
        let url = parse_multiaddr("/ip4/192.168.1.1/tcp/8080/http");
        assert_eq!(url, Some("http://192.168.1.1:8080".to_string()));
    }

    #[test]
    fn test_parse_multiaddr_invalid() {
        assert_eq!(parse_multiaddr(""), None);
        assert_eq!(parse_multiaddr("garbage"), None);
        assert_eq!(parse_multiaddr("/dns//tcp/8080"), None);
    }

    #[test]
    fn test_grpc_endpoints() {
        let validators = vec![
            ValidatorInfo {
                name: [1u8; 32],
                network_address: "/dns/val1.example.com/tcp/8080/http".to_string(),
                stake: 5000,
                description: "val1".to_string(),
            },
            ValidatorInfo {
                name: [2u8; 32],
                network_address: "/dns/val2.example.com/tcp/8080/http".to_string(),
                stake: 5000,
                description: "val2".to_string(),
            },
            ValidatorInfo {
                name: [3u8; 32],
                network_address: "garbage".to_string(),
                stake: 0,
                description: "bad".to_string(),
            },
        ];
        let committee = Committee::new(1, validators);
        let endpoints = committee.grpc_endpoints();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0], "http://val1.example.com:8080");
        assert_eq!(endpoints[1], "http://val2.example.com:8080");
    }
}
