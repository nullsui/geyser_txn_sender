use geyser_core::fullnode_racer::FullnodeRacer;
use geyser_core::quorum_driver::DriveOverrides;
use geyser_core::quorum_driver::QuorumDriver;
use std::sync::Arc;
use tonic::Status;
use tracing::{debug, error, info};

/// gRPC service implementation for Geyser.
/// Implements a subset of Sui's TransactionExecutionService.
pub struct GeyserGrpcService {
    quorum_driver: Arc<QuorumDriver>,
    fullnode_racer: Arc<FullnodeRacer>,
}

impl GeyserGrpcService {
    pub fn new(quorum_driver: Arc<QuorumDriver>, fullnode_racer: Arc<FullnodeRacer>) -> Self {
        Self {
            quorum_driver,
            fullnode_racer,
        }
    }

    /// Execute a transaction through the Geyser quorum driver.
    ///
    /// Request format (JSON in gRPC metadata or body):
    /// - tx_bytes: Base64-encoded transaction bytes
    /// - signatures: Array of Base64-encoded signatures
    pub async fn execute_transaction(
        &self,
        tx_bytes: Vec<u8>,
        signatures: Vec<Vec<u8>>,
        overrides: Option<DriveOverrides>,
    ) -> Result<ExecuteResponse, Status> {
        // Compute TX digest (simplified: hash of tx_bytes)
        let tx_digest = Self::compute_digest(&tx_bytes);

        info!(
            digest = hex::encode(&tx_digest[..8]),
            "gRPC: Execute transaction"
        );

        match self
            .quorum_driver
            .drive_with_overrides(&tx_bytes, &signatures, tx_digest, None, overrides.as_ref())
            .await
        {
            Ok(result) => {
                debug!(
                    digest = hex::encode(&tx_digest[..8]),
                    latency_ms = result.total_latency.as_millis(),
                    path = %result.tx_path,
                    "gRPC: Transaction finalized"
                );

                Ok(ExecuteResponse {
                    effects_bytes: result.effects.effects_bytes,
                    effects_digest: result.effects.effects_digest.to_vec(),
                    events_bytes: result.effects.events_bytes,
                    latency_ms: result.total_latency.as_millis() as u64,
                    submit_ms: result.submit_latency.as_millis() as u64,
                    effects_ms: result.effects.latency.as_millis() as u64,
                })
            }
            Err(e) => {
                error!(error = %e, "gRPC: Transaction execution failed");
                Err(Status::internal(format!("Transaction failed: {}", e)))
            }
        }
    }

    /// Simulate a transaction (forwarded to fullnode).
    pub async fn simulate_transaction(
        &self,
        tx_bytes_b64: &str,
    ) -> Result<serde_json::Value, Status> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_dryRunTransactionBlock",
            "params": [tx_bytes_b64]
        });

        self.fullnode_racer
            .forward_jsonrpc(&request)
            .await
            .map_err(|e| Status::internal(format!("Simulation failed: {}", e)))
    }

    /// Compute the Sui TransactionDigest from raw TransactionData BCS bytes.
    /// Uses Sui's Signable trait: Blake2b-256("TransactionData::" || tx_bytes)
    fn compute_digest(tx_bytes: &[u8]) -> [u8; 32] {
        let digest_vec = geyser_core::signer::tx_digest_bytes(tx_bytes);
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&digest_vec);
        digest
    }
}

/// Response from transaction execution.
#[derive(Debug, Clone)]
pub struct ExecuteResponse {
    pub effects_bytes: Vec<u8>,
    pub effects_digest: Vec<u8>,
    pub events_bytes: Option<Vec<u8>>,
    pub latency_ms: u64,
    pub submit_ms: u64,
    pub effects_ms: u64,
}
