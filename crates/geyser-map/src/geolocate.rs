use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoResult {
    pub query: String,
    pub country: String,
    pub city: String,
    pub isp: String,
    #[serde(rename = "as")]
    pub asn: String,
    pub org: String,
    pub status: String,
}

impl GeoResult {
    pub fn ip(&self) -> &str {
        &self.query
    }
}

/// Batch-geolocate a set of IPs using ip-api.com's batch endpoint.
///
/// Free tier: 15 requests/minute, up to 100 IPs per batch request.
/// We request only the fields we need to minimize response size.
pub async fn batch_geolocate(client: &reqwest::Client, ips: &[Ipv4Addr]) -> Result<Vec<GeoResult>> {
    let mut all_results = Vec::with_capacity(ips.len());

    // Process in batches of 100 (API limit)
    for chunk in ips.chunks(100) {
        let payload: Vec<serde_json::Value> = chunk
            .iter()
            .map(|ip| {
                serde_json::json!({
                    "query": ip.to_string(),
                    "fields": "status,country,city,isp,as,org,query"
                })
            })
            .collect();

        let resp = client
            .post("http://ip-api.com/batch")
            .json(&payload)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("ip-api.com batch request failed: {} — {}", status, body);
        }

        let batch: Vec<GeoResult> = resp.json().await?;
        all_results.extend(batch);

        // If we have more chunks, respect rate limiting
        if chunk.len() == 100 && ips.len() > 100 {
            tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        }
    }

    Ok(all_results)
}
