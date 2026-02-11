use anyhow::{anyhow, Result};
use std::time::Duration;
use tracing::debug;

/// Per-server timing from a detailed CDN race.
#[derive(Debug, Clone)]
pub struct ServerTiming {
    pub url: String,
    pub elapsed: Duration,
    /// `None` if the server errored.
    pub result: Option<serde_json::Value>,
    pub is_winner: bool,
}

/// Result of racing a transaction across multiple CDN servers.
#[derive(Debug, Clone)]
pub struct RaceResult {
    /// The winning (fastest successful) response.
    pub winner: serde_json::Value,
    /// URL of the winning server.
    pub winner_url: String,
    /// Per-server timing, sorted by elapsed (fastest first).
    pub server_results: Vec<ServerTiming>,
}

/// HTTP client for building Programmable Transaction Blocks via Sui JSON-RPC.
/// We use the fullnode's `unsafe_*` methods to construct transaction bytes,
/// then sign locally and submit via gRPC V2 or JSON-RPC.
pub struct PtbBuilder {
    client: reqwest::Client,
    rpc_url: String,
}

impl PtbBuilder {
    pub fn new(rpc_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            rpc_url,
        }
    }

    /// Get the first SUI gas coin for the sender.
    async fn get_gas_coin(&self, sender: &str) -> Result<String> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getCoins",
                "params": [
                    sender,
                    "0x2::sui::SUI",
                    null,
                    1
                ]
            }))
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        if let Some(error) = body.get("error") {
            return Err(anyhow!("getCoins error: {}", error));
        }

        body["result"]["data"][0]["coinObjectId"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("No SUI coins found for {}", sender))
    }

    /// Build a SUI self-transfer transaction.
    /// Returns raw tx_bytes (BCS-encoded TransactionData, NOT base64).
    pub async fn build_transfer_sui(
        &self,
        sender: &str,
        recipient: &str,
        amount_mist: u64,
        gas_budget: u64,
    ) -> Result<Vec<u8>> {
        debug!(
            sender,
            recipient, amount_mist, gas_budget, "Building transfer SUI PTB"
        );

        // Fetch a gas coin to use
        let gas_coin = self.get_gas_coin(sender).await?;

        let response = self
            .client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "unsafe_transferSui",
                "params": [
                    sender,
                    gas_coin,
                    gas_budget.to_string(),
                    recipient,
                    amount_mist.to_string()
                ]
            }))
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        self.extract_tx_bytes(&body)
    }

    /// Build a Move call transaction.
    /// Returns raw tx_bytes (BCS-encoded).
    pub async fn build_move_call(
        &self,
        sender: &str,
        package_id: &str,
        module: &str,
        function: &str,
        type_args: &[String],
        args: &[serde_json::Value],
        gas_budget: u64,
    ) -> Result<Vec<u8>> {
        debug!(
            sender,
            package_id, module, function, gas_budget, "Building move call PTB"
        );

        let response = self
            .client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "unsafe_moveCall",
                "params": [
                    sender,
                    package_id,
                    module,
                    function,
                    type_args,
                    args,
                    null,
                    gas_budget.to_string(),
                    null
                ]
            }))
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        self.extract_tx_bytes(&body)
    }

    /// Execute a signed transaction via JSON-RPC (vanilla path for benchmarking).
    /// Returns the full response JSON including effects.
    pub async fn execute_transaction(
        &self,
        tx_bytes_b64: &str,
        signature_b64: &str,
    ) -> Result<serde_json::Value> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sui_executeTransactionBlock",
                "params": [
                    tx_bytes_b64,
                    [signature_b64],
                    {
                        "showEffects": true,
                        "showEvents": true,
                    },
                    "WaitForLocalExecution"
                ]
            }))
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;

        if let Some(error) = body.get("error") {
            return Err(anyhow!("RPC error: {}", error));
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("No result in response"))
    }

    /// Execute a signed transaction via an arbitrary JSON-RPC endpoint (e.g. CDN).
    pub async fn execute_transaction_via(
        &self,
        url: &str,
        tx_bytes_b64: &str,
        signature_b64: &str,
        headers: Option<&[(String, String)]>,
    ) -> Result<serde_json::Value> {
        let mut req = self.client.post(url).json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_executeTransactionBlock",
            "params": [
                tx_bytes_b64,
                [signature_b64],
                {
                    "showEffects": true,
                    "showEvents": true,
                },
                "WaitForLocalExecution"
            ]
        }));

        if let Some(hdrs) = headers {
            for (k, v) in hdrs {
                req = req.header(k.as_str(), v.as_str());
            }
        }

        let response = req.send().await?;

        let body: serde_json::Value = response.json().await?;

        if let Some(error) = body.get("error") {
            return Err(anyhow!("RPC error: {}", error));
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("No result in response"))
    }

    /// Race a transaction across multiple CDN endpoints simultaneously.
    /// First successful response wins. Safe because identical signed TXs are idempotent.
    pub async fn race_execute_transaction_via(
        &self,
        urls: &[String],
        tx_bytes_b64: &str,
        signature_b64: &str,
        headers: Option<&[(String, String)]>,
    ) -> Result<serde_json::Value> {
        if urls.len() == 1 {
            return self
                .execute_transaction_via(&urls[0], tx_bytes_b64, signature_b64, headers)
                .await;
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<serde_json::Value>>(urls.len());

        // Clone headers into owned Vec for spawned tasks
        let owned_headers: Option<Vec<(String, String)>> = headers.map(|h| h.to_vec());

        for url in urls {
            let client = self.client.clone();
            let url = url.clone();
            let tx_b64 = tx_bytes_b64.to_string();
            let sig_b64 = signature_b64.to_string();
            let tx = tx.clone();
            let task_headers = owned_headers.clone();

            tokio::spawn(async move {
                let result = async {
                    let body = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "sui_executeTransactionBlock",
                        "params": [
                            tx_b64,
                            [sig_b64],
                            {
                                "showEffects": true,
                                "showEvents": true,
                            },
                            "WaitForLocalExecution"
                        ]
                    });
                    let mut req = client.post(&url).json(&body);

                    if let Some(ref hdrs) = task_headers {
                        for (k, v) in hdrs {
                            req = req.header(k.as_str(), v.as_str());
                        }
                    }

                    let response = req.send().await?;

                    let body: serde_json::Value = response.json().await?;

                    if let Some(error) = body.get("error") {
                        return Err(anyhow!("RPC error: {}", error));
                    }

                    body.get("result")
                        .cloned()
                        .ok_or_else(|| anyhow!("No result in response"))
                }
                .await;

                let _ = tx.send(result).await;
            });
        }
        drop(tx);

        let mut last_err = None;
        while let Some(result) = rx.recv().await {
            match result {
                Ok(val) => return Ok(val),
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("All CDN endpoints failed")))
    }

    /// Race a transaction across multiple CDN endpoints and collect ALL results.
    /// Returns the winner (first success) plus per-server timing for every endpoint.
    pub async fn race_execute_transaction_detailed(
        &self,
        urls: &[String],
        tx_bytes_b64: &str,
        signature_b64: &str,
        headers: Option<&[(String, String)]>,
    ) -> Result<RaceResult> {
        use std::time::Instant;

        if urls.len() == 1 {
            let start = Instant::now();
            let result = self
                .execute_transaction_via(&urls[0], tx_bytes_b64, signature_b64, headers)
                .await?;
            let elapsed = start.elapsed();
            return Ok(RaceResult {
                winner: result.clone(),
                winner_url: urls[0].clone(),
                server_results: vec![ServerTiming {
                    url: urls[0].clone(),
                    elapsed,
                    result: Some(result),
                    is_winner: true,
                }],
            });
        }

        let race_start = Instant::now();
        let owned_headers: Option<Vec<(String, String)>> = headers.map(|h| h.to_vec());
        let (tx, mut rx) = tokio::sync::mpsc::channel(urls.len());

        for url in urls {
            let client = self.client.clone();
            let url = url.clone();
            let tx_b64 = tx_bytes_b64.to_string();
            let sig_b64 = signature_b64.to_string();
            let task_headers = owned_headers.clone();
            let start = race_start;
            let tx = tx.clone();

            tokio::spawn(async move {
                let result = async {
                    let body = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "sui_executeTransactionBlock",
                        "params": [
                            tx_b64,
                            [sig_b64],
                            {
                                "showEffects": true,
                                "showEvents": true,
                            },
                            "WaitForLocalExecution"
                        ]
                    });
                    let mut req = client.post(&url).json(&body);
                    if let Some(ref hdrs) = task_headers {
                        for (k, v) in hdrs {
                            req = req.header(k.as_str(), v.as_str());
                        }
                    }
                    let response = req.send().await?;
                    let body: serde_json::Value = response.json().await?;
                    if let Some(error) = body.get("error") {
                        return Err(anyhow!("RPC error: {}", error));
                    }
                    body.get("result")
                        .cloned()
                        .ok_or_else(|| anyhow!("No result in response"))
                }
                .await;
                let elapsed = start.elapsed();
                let _ = tx.send((url, elapsed, result)).await;
            });
        }
        drop(tx);

        // True race semantics: return on the first successful response.
        // Keep partial server timings for diagnostics from responses that arrived
        // before/around the winner.
        let mut timings: Vec<ServerTiming> = Vec::with_capacity(urls.len());
        let mut last_err: Option<anyhow::Error> = None;

        while let Some((url, elapsed, result)) = rx.recv().await {
            let is_success = result.is_ok();
            let maybe_result = result.ok();
            if !is_success {
                last_err = Some(anyhow!("CDN endpoint {} failed", url));
            }

            timings.push(ServerTiming {
                url: url.clone(),
                elapsed,
                result: maybe_result.clone(),
                is_winner: false,
            });

            if let Some(winner) = maybe_result {
                let winner_idx = timings.len() - 1;
                timings[winner_idx].is_winner = true;

                // Drain already-ready responses without blocking so diagnostics
                // still include nearby racers, but don't wait for stragglers.
                while let Ok((u, e, r)) = rx.try_recv() {
                    timings.push(ServerTiming {
                        url: u,
                        elapsed: e,
                        result: r.ok(),
                        is_winner: false,
                    });
                }

                return Ok(RaceResult {
                    winner,
                    winner_url: url,
                    server_results: timings,
                });
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("All CDN endpoints failed")))
    }

    /// Extract raw tx_bytes from a JSON-RPC response.
    /// The response contains base64-encoded txBytes which we decode to raw bytes.
    fn extract_tx_bytes(&self, body: &serde_json::Value) -> Result<Vec<u8>> {
        if let Some(error) = body.get("error") {
            return Err(anyhow!("RPC error: {}", error));
        }

        let tx_bytes_b64 = body["result"]["txBytes"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing txBytes in response: {}", body))?;

        let tx_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, tx_bytes_b64)?;

        Ok(tx_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ptb_builder_creation() {
        let builder = PtbBuilder::new("https://fullnode.mainnet.sui.io:443".to_string());
        assert!(!builder.rpc_url.is_empty());
    }
}
