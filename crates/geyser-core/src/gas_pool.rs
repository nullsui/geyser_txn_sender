use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tracing::{debug, info};

/// Reference to a Sui object (id, version, digest).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub object_id: [u8; 32],
    pub version: u64,
    pub digest: [u8; 32],
}

/// Manages a pool of pre-split gas coins for parallel transaction submission.
///
/// Problem: Sui locks gas coins per ObjectRef post-consensus. If you use the SAME
/// gas coin for 2 TXs, the second blocks until the first finishes.
///
/// Solution: Pre-split SUI into N gas coins. Each outbound TX gets a dedicated gas coin.
/// After TX confirms, gas coin returns to pool with updated version.
pub struct GasPool {
    /// Available gas coins (ready for checkout).
    available: Arc<Mutex<VecDeque<ObjectRef>>>,
    /// Currently checked-out coins.
    in_use: Arc<Mutex<HashSet<[u8; 32]>>>,
    /// Target pool size.
    target_size: usize,
    /// Minimum balance per gas coin in MIST.
    min_balance: u64,
}

impl GasPool {
    /// Create a new gas pool with the given target size.
    pub fn new(target_size: usize, min_balance: u64) -> Self {
        Self {
            available: Arc::new(Mutex::new(VecDeque::with_capacity(target_size))),
            in_use: Arc::new(Mutex::new(HashSet::new())),
            target_size,
            min_balance,
        }
    }

    /// Initialize the pool from a list of existing gas coins.
    pub fn init_with_coins(&self, coins: Vec<ObjectRef>) {
        let mut available = self.available.lock();
        for coin in coins {
            available.push_back(coin);
        }
        info!(
            available = available.len(),
            target = self.target_size,
            "Gas pool initialized"
        );
    }

    /// Checkout a gas coin for a transaction (non-blocking).
    /// Returns None if no coins are available.
    pub fn checkout(&self) -> Result<ObjectRef> {
        let mut available = self.available.lock();
        let coin = available
            .pop_front()
            .ok_or_else(|| anyhow!("Gas pool exhausted (0/{} available)", self.target_size))?;

        let mut in_use = self.in_use.lock();
        in_use.insert(coin.object_id);

        debug!(
            coin = hex::encode(&coin.object_id[..8]),
            remaining = available.len(),
            in_use = in_use.len(),
            "Gas coin checked out"
        );

        Ok(coin)
    }

    /// Return a gas coin after TX completes (with updated version/digest).
    pub fn checkin(&self, coin: ObjectRef) {
        let mut in_use = self.in_use.lock();
        in_use.remove(&coin.object_id);

        let mut available = self.available.lock();
        available.push_back(coin);

        debug!(
            coin = hex::encode(&available.back().unwrap().object_id[..8]),
            available = available.len(),
            in_use = in_use.len(),
            "Gas coin returned"
        );
    }

    /// Number of available gas coins.
    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    /// Number of in-use gas coins.
    pub fn in_use_count(&self) -> usize {
        self.in_use.lock().len()
    }

    /// Target pool size.
    pub fn target_size(&self) -> usize {
        self.target_size
    }

    /// Minimum balance per coin.
    pub fn min_balance(&self) -> u64 {
        self.min_balance
    }

    /// Check if the pool needs refilling.
    pub fn needs_refill(&self) -> bool {
        self.available_count() + self.in_use_count() < self.target_size
    }

    /// Refill the pool by splitting existing coins.
    /// Builds a SplitCoins PTB, signs, and submits to create more gas coins.
    pub async fn refill(
        &self,
        fullnode_url: &str,
        sender: &str,
        sign_fn: impl Fn(&[u8]) -> Vec<u8>,
    ) -> Result<usize> {
        let current = self.available_count() + self.in_use_count();
        let needed = self.target_size.saturating_sub(current);

        if needed == 0 {
            return Ok(0);
        }

        info!(
            current,
            needed,
            target = self.target_size,
            "Refilling gas pool"
        );

        // 1. Build SplitCoins PTB via JSON-RPC
        let client = reqwest::Client::new();
        let amounts: Vec<String> = (0..needed).map(|_| self.min_balance.to_string()).collect();
        let total_needed = self.min_balance * (needed as u64) + 50_000_000; // Extra for gas

        let response = client
            .post(fullnode_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "unsafe_splitCoin",
                "params": [
                    sender,
                    null,  // coin to split (null = gas coin)
                    amounts,
                    null,  // gas object
                    total_needed.to_string()  // gas budget
                ]
            }))
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        if let Some(error) = body.get("error") {
            return Err(anyhow!("SplitCoins RPC error: {}", error));
        }

        let tx_bytes_b64 = body["result"]["txBytes"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing txBytes in split response"))?;

        // 2. Decode, sign, and re-encode
        let tx_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, tx_bytes_b64)?;
        let sig_bytes = sign_fn(&tx_bytes);
        let sig_b64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &sig_bytes);

        // 3. Submit via JSON-RPC
        let exec_response = client
            .post(fullnode_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "sui_executeTransactionBlock",
                "params": [
                    tx_bytes_b64,
                    [sig_b64],
                    { "showEffects": true, "showObjectChanges": true },
                    "WaitForLocalExecution"
                ]
            }))
            .send()
            .await?;

        let exec_body: serde_json::Value = exec_response.json().await?;
        if let Some(error) = exec_body.get("error") {
            return Err(anyhow!("SplitCoins execute error: {}", error));
        }

        // 4. Parse created objects from effects
        let mut created_count = 0;
        if let Some(changes) = exec_body["result"]["objectChanges"].as_array() {
            for change in changes {
                if change["type"].as_str() == Some("created") {
                    if let (Some(id_str), Some(version), Some(digest_str)) = (
                        change["objectId"].as_str(),
                        change["version"].as_u64(),
                        change["digest"].as_str(),
                    ) {
                        let mut object_id = [0u8; 32];
                        if let Ok(bytes) = hex::decode(id_str.trim_start_matches("0x")) {
                            if bytes.len() == 32 {
                                object_id.copy_from_slice(&bytes);
                            }
                        }
                        let mut digest = [0u8; 32];
                        // Digest is base58 in Sui, but we just use it as opaque identifier
                        let digest_bytes = digest_str.as_bytes();
                        let copy_len = digest_bytes.len().min(32);
                        digest[..copy_len].copy_from_slice(&digest_bytes[..copy_len]);

                        let coin = ObjectRef {
                            object_id,
                            version,
                            digest,
                        };
                        let mut available = self.available.lock();
                        available.push_back(coin);
                        created_count += 1;
                    }
                }
            }
        }

        info!(
            created = created_count,
            available = self.available_count(),
            "Gas pool refilled"
        );

        Ok(created_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_coin(id: u8) -> ObjectRef {
        let mut object_id = [0u8; 32];
        object_id[0] = id;
        ObjectRef {
            object_id,
            version: 1,
            digest: [0u8; 32],
        }
    }

    #[test]
    fn test_checkout_and_checkin() {
        let pool = GasPool::new(3, 100_000_000);
        pool.init_with_coins(vec![make_coin(1), make_coin(2), make_coin(3)]);

        assert_eq!(pool.available_count(), 3);
        assert_eq!(pool.in_use_count(), 0);

        // Checkout
        let coin = pool.checkout().unwrap();
        assert_eq!(pool.available_count(), 2);
        assert_eq!(pool.in_use_count(), 1);

        // Checkin with updated version
        let mut updated = coin;
        updated.version = 2;
        pool.checkin(updated);
        assert_eq!(pool.available_count(), 3);
        assert_eq!(pool.in_use_count(), 0);
    }

    #[test]
    fn test_pool_exhaustion() {
        let pool = GasPool::new(2, 100_000_000);
        pool.init_with_coins(vec![make_coin(1), make_coin(2)]);

        let _c1 = pool.checkout().unwrap();
        let _c2 = pool.checkout().unwrap();

        // Pool should be empty now
        assert!(pool.checkout().is_err());
    }

    #[test]
    fn test_needs_refill() {
        let pool = GasPool::new(5, 100_000_000);
        pool.init_with_coins(vec![make_coin(1), make_coin(2)]);
        assert!(pool.needs_refill());

        // Fill to target
        pool.init_with_coins(vec![make_coin(3), make_coin(4), make_coin(5)]);
        assert!(!pool.needs_refill());
    }

    #[test]
    fn test_fifo_ordering() {
        let pool = GasPool::new(3, 100_000_000);
        pool.init_with_coins(vec![make_coin(1), make_coin(2), make_coin(3)]);

        let c1 = pool.checkout().unwrap();
        assert_eq!(c1.object_id[0], 1);
        let c2 = pool.checkout().unwrap();
        assert_eq!(c2.object_id[0], 2);

        // Return c1, then checkout should get c3 (FIFO), then c1 (returned to back)
        pool.checkin(c1);
        let c3 = pool.checkout().unwrap();
        assert_eq!(c3.object_id[0], 3);
        let c1_again = pool.checkout().unwrap();
        assert_eq!(c1_again.object_id[0], 1);
    }
}
