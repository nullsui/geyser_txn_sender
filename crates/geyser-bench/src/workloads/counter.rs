use super::{
    cdn_fast_headers, decode_bcs_payload, decode_cdn_fast_response, parse_cdn_server_timing,
    LatencySample, RaceServerDetail, SubmitMode, Workload,
};
use anyhow::Result;
use geyser_core::fullnode_racer::FullnodeRacer;
use geyser_core::ptb::PtbBuilder;
use geyser_core::quorum_driver::QuorumDriver;
use geyser_core::signer::{self, SuiKeypair};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Shared counter increment workload.
/// Shared objects → consensus path (slower, the interesting benchmark).
pub struct CounterWorkload {
    ptb: Arc<PtbBuilder>,
    racer: Arc<FullnodeRacer>,
    keypair: SuiKeypair,
    sender: String,
    package_id: String,
    counter_id: String,
    mode: SubmitMode,
    max_validators: usize,
    cdn_urls: Vec<String>,
    cdn_labels: Vec<String>,
    quorum_driver: Option<Arc<QuorumDriver>>,
    name_override: Option<String>,
}

impl CounterWorkload {
    pub fn new(
        ptb: Arc<PtbBuilder>,
        racer: Arc<FullnodeRacer>,
        keypair: SuiKeypair,
        sender: String,
        package_id: String,
        counter_id: String,
        mode: SubmitMode,
        max_validators: usize,
    ) -> Self {
        Self {
            ptb,
            racer,
            keypair,
            sender,
            package_id,
            counter_id,
            mode,
            max_validators,
            cdn_urls: Vec::new(),
            cdn_labels: Vec::new(),
            quorum_driver: None,
            name_override: None,
        }
    }

    pub fn with_name_override(mut self, name: String) -> Self {
        self.name_override = Some(name);
        self
    }

    pub fn with_cdn_urls(mut self, urls: Vec<String>) -> Self {
        self.cdn_urls = urls;
        self
    }

    pub fn with_cdn_labels(mut self, labels: Vec<String>) -> Self {
        self.cdn_labels = labels;
        self
    }

    pub fn with_quorum_driver(mut self, qd: Arc<QuorumDriver>) -> Self {
        self.quorum_driver = Some(qd);
        self
    }

    fn label_for_url(&self, url: &str) -> String {
        self.cdn_urls
            .iter()
            .zip(self.cdn_labels.iter())
            .find(|(u, _)| u.as_str() == url)
            .map(|(_, l)| l.clone())
            .unwrap_or_else(|| url.to_string())
    }

    async fn execute_inner(&self) -> Result<LatencySample> {
        // 1. Build MoveCall PTB: counter::increment(counter)
        let tx_bytes = self
            .ptb
            .build_move_call(
                &self.sender,
                &self.package_id,
                "counter",
                "increment",
                &[],
                &[serde_json::json!(self.counter_id)],
                10_000_000,
            )
            .await?;

        // 2. Sign locally
        let sig_bytes = self.keypair.sign_transaction(&tx_bytes);

        // 3. Submit and decode timing
        let submit_start = Instant::now();
        let mut decode_bcs: Option<(Vec<u8>, Option<Vec<u8>>)> = None;

        match self.mode {
            SubmitMode::DirectValidator => {
                let qd = self
                    .quorum_driver
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("QuorumDriver required for direct mode"))?;

                let digest_vec = signer::tx_digest_bytes(&tx_bytes);
                let mut tx_digest = [0u8; 32];
                tx_digest.copy_from_slice(&digest_vec);

                let result = qd.drive(&tx_bytes, &[sig_bytes], tx_digest, None).await?;

                let submit = submit_start.elapsed();
                let decode_start = Instant::now();
                decode_bcs_payload(
                    &result.effects.effects_bytes,
                    result.effects.events_bytes.as_deref(),
                )?;
                let decode = decode_start.elapsed();

                return Ok(LatencySample::with_diagnostics(
                    submit + decode,
                    submit,
                    decode,
                    result.effects.latency == Duration::ZERO,
                    false,
                ));
            }
            SubmitMode::GrpcFullnode => {
                let response = self.racer.race_execute_grpc(&tx_bytes, &sig_bytes).await?;
                let effects_bcs = response
                    .effects_bcs
                    .ok_or_else(|| anyhow::anyhow!("gRPC response missing effects_bcs"))?;
                decode_bcs = Some((effects_bcs, response.events_bcs));
            }
            SubmitMode::JsonRpc => {
                let tx_b64 =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);
                let sig_b64 =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &sig_bytes);
                self.ptb.execute_transaction(&tx_b64, &sig_b64).await?;
            }
            SubmitMode::CdnFast | SubmitMode::CdnPerServer => {
                if self.cdn_urls.is_empty() {
                    return Err(anyhow::anyhow!("CDN URLs required for cdn-fast mode"));
                }
                let tx_b64 =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);
                let sig_b64 =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &sig_bytes);
                let headers = cdn_fast_headers();

                let race = self
                    .ptb
                    .race_execute_transaction_detailed(
                        &self.cdn_urls,
                        &tx_b64,
                        &sig_b64,
                        Some(&headers),
                    )
                    .await?;

                let decode_start = Instant::now();
                decode_cdn_fast_response(&race.winner)?;
                let decode = decode_start.elapsed();
                let submit = submit_start.elapsed();
                let mut sample = LatencySample::new(submit + decode, submit, decode);
                let (st, ss, se) = parse_cdn_server_timing(&race.winner);
                if let (Some(t), Some(s), Some(e)) = (st, ss, se) {
                    sample = sample.with_server_timing(t, s, e);
                }

                let details: Vec<RaceServerDetail> = race
                    .server_results
                    .iter()
                    .map(|sr| {
                        let (ss, se) = sr
                            .result
                            .as_ref()
                            .map(|r| {
                                let (_, sub, eff) = parse_cdn_server_timing(r);
                                (sub, eff)
                            })
                            .unwrap_or((None, None));
                        RaceServerDetail {
                            url: sr.url.clone(),
                            label: self.label_for_url(&sr.url),
                            elapsed: sr.elapsed,
                            server_submit_ms: ss,
                            server_effects_ms: se,
                            is_winner: sr.is_winner,
                        }
                    })
                    .collect();
                sample = sample.with_race_details(details);

                return Ok(sample);
            }
        }

        let submit = submit_start.elapsed();
        let decode_start = Instant::now();
        if let Some((effects_bcs, events_bcs)) = decode_bcs {
            decode_bcs_payload(&effects_bcs, events_bcs.as_deref())?;
        }
        let decode = decode_start.elapsed();

        Ok(LatencySample::new(submit + decode, submit, decode))
    }
}

impl Workload for CounterWorkload {
    fn name(&self) -> &str {
        if let Some(ref name) = self.name_override {
            return name;
        }
        match self.mode {
            SubmitMode::DirectValidator => "direct-counter",
            SubmitMode::GrpcFullnode => "grpc-counter",
            SubmitMode::JsonRpc => "vanilla-counter",
            SubmitMode::CdnFast => "cdn-fast-counter",
            SubmitMode::CdnPerServer => "cdn-per-server-counter",
        }
    }

    fn description(&self) -> &str {
        "Shared counter increment (consensus path)"
    }

    fn execute(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<LatencySample>> + Send + '_>>
    {
        Box::pin(self.execute_inner())
    }
}
