use crate::bcs_json::{self, ShowOptions};
use crate::grpc::GeyserGrpcService;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use geyser_core::fullnode_racer::FullnodeRacer;
use geyser_core::quorum_driver::DriveOverrides;
use geyser_core::signer;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const MAX_AMPLIFICATION_CAP: usize = 128;
const MAX_EFFECTS_FANOUT_CAP: usize = 128;
const MAX_TIMEOUT_MS_CAP: u64 = 30_000;
const MAX_DELAY_MS_CAP: u64 = 10_000;
const MAX_RETRIES_CAP: usize = 8;

/// JSON-RPC compatibility layer.
/// Routes sui_executeTransactionBlock through Geyser's quorum driver,
/// forwards all other methods to the fastest fullnode.
///
/// Two modes controlled by `X-Geyser-Fast` header:
/// - Standard (default): decode BCS effects into standard Sui JSON response
/// - Fast (`X-Geyser-Fast: true`): return raw BCS effects, client decodes
pub async fn handle_jsonrpc(
    State(state): State<Arc<JsonRpcState>>,
    headers: HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let id = request.get("id").cloned().unwrap_or(serde_json::json!(1));

    let fast_mode = headers
        .get("x-geyser-fast")
        .and_then(|v| v.to_str().ok())
        .map(|v| !v.eq_ignore_ascii_case("false"))
        .unwrap_or(false);

    debug!(method, fast_mode, "JSON-RPC request");

    match method {
        "sui_executeTransactionBlock" => {
            handle_execute_tx(&state, &request, &headers, &id, fast_mode).await
        }
        _ => {
            // Forward all other methods (including sui_dryRunTransactionBlock) to fullnode
            forward_to_fullnode(&state.fullnode_racer, &request).await
        }
    }
}

/// State shared across JSON-RPC handlers.
pub struct JsonRpcState {
    pub grpc_service: Arc<GeyserGrpcService>,
    pub fullnode_racer: Arc<FullnodeRacer>,
}

async fn handle_execute_tx(
    state: &JsonRpcState,
    request: &serde_json::Value,
    headers: &HeaderMap,
    id: &serde_json::Value,
    fast_mode: bool,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let params = request
        .get("params")
        .and_then(|v| v.as_array())
        .ok_or(StatusCode::BAD_REQUEST)?;

    if params.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let tx_bytes_b64 = params[0].as_str().ok_or(StatusCode::BAD_REQUEST)?;
    let signatures_b64: Vec<String> = params
        .get(1)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Decode tx_bytes
    let tx_bytes = base64_decode(tx_bytes_b64).map_err(|_| StatusCode::BAD_REQUEST)?;
    let signatures: Vec<Vec<u8>> = signatures_b64
        .iter()
        .filter_map(|s| base64_decode(s).ok())
        .collect();
    let overrides = parse_drive_overrides(headers, params);
    let overrides_json = overrides_to_json(overrides.as_ref());

    info!(
        tx_bytes_len = tx_bytes.len(),
        signatures = signatures.len(),
        fast_mode,
        has_overrides = overrides.is_some(),
        "JSON-RPC: Execute transaction"
    );

    if fast_mode {
        // FAST MODE: direct to validators, return BCS effects
        match state
            .grpc_service
            .execute_transaction(tx_bytes, signatures, overrides.clone())
            .await
        {
            Ok(response) => {
                let effects_b64 = base64_encode(&response.effects_bytes);
                let mut geyser = serde_json::json!({
                    "latencyMs": response.latency_ms,
                    "submitMs": response.submit_ms,
                    "effectsMs": response.effects_ms,
                });
                if let (Some(ov), Some(geyser_obj)) = (overrides_json, geyser.as_object_mut()) {
                    geyser_obj.insert("overrides".to_string(), ov);
                }

                let result = serde_json::json!({
                    "digest": hex::encode(&response.effects_digest),
                    "effects": {
                        "bcs": effects_b64,
                    },
                    "geyser": geyser
                });
                Ok(Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result
                })))
            }
            Err(status) => {
                error!(error = %status.message(), "JSON-RPC: Execute failed (fast)");
                Ok(Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": -32000,
                        "message": status.message()
                    }
                })))
            }
        }
    } else {
        // STANDARD MODE: submit to validators, decode BCS effects into standard JSON
        let digest_b58 = signer::tx_digest(&tx_bytes);
        let show = ShowOptions::from_params(params.get(2));

        match state
            .grpc_service
            .execute_transaction(tx_bytes, signatures, overrides)
            .await
        {
            Ok(response) => {
                info!(
                    digest = %digest_b58,
                    validator_ms = response.latency_ms,
                    "Validators confirmed, decoding BCS effects"
                );

                match bcs_json::effects_to_sui_response(
                    &response.effects_bytes,
                    response.events_bytes.as_deref(),
                    &digest_b58,
                    &show,
                    id,
                ) {
                    Ok(json_response) => Ok(Json(json_response)),
                    Err(e) => {
                        warn!(error = %e, "BCS decode failed, forwarding to fullnode");
                        forward_to_fullnode(&state.fullnode_racer, request).await
                    }
                }
            }
            Err(status) => {
                warn!(error = %status.message(), "QuorumDriver failed, forwarding to fullnode");
                forward_to_fullnode(&state.fullnode_racer, request).await
            }
        }
    }
}

async fn forward_to_fullnode(
    racer: &FullnodeRacer,
    request: &serde_json::Value,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match racer.forward_jsonrpc(request).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            error!(error = %e, "Fullnode forward failed");
            Err(StatusCode::BAD_GATEWAY)
        }
    }
}

fn base64_decode(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(input)
}

fn base64_encode(input: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(input)
}

fn parse_drive_overrides(headers: &HeaderMap, params: &[Value]) -> Option<DriveOverrides> {
    let mut overrides = DriveOverrides::default();
    let mut has_overrides = false;

    let options_geyser = params
        .get(2)
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("geyser"))
        .and_then(|v| v.as_object());

    let body_usize = |key: &str| -> Option<usize> {
        options_geyser
            .and_then(|o| o.get(key))
            .and_then(parse_json_usize)
    };
    let body_u64 = |key: &str| -> Option<u64> {
        options_geyser
            .and_then(|o| o.get(key))
            .and_then(parse_json_u64)
    };

    has_overrides |= set_usize_override(
        &mut overrides.max_amplification,
        body_usize("maxAmplification"),
        MAX_AMPLIFICATION_CAP,
    );
    has_overrides |= set_usize_override(
        &mut overrides.owned_amplification,
        body_usize("ownedAmplification"),
        MAX_AMPLIFICATION_CAP,
    );
    has_overrides |= set_usize_override(
        &mut overrides.effects_fallback_fanout,
        body_usize("effectsFallbackFanout"),
        MAX_EFFECTS_FANOUT_CAP,
    );
    has_overrides |= set_duration_override(
        &mut overrides.submit_timeout,
        body_u64("submitTimeoutMs"),
        MAX_TIMEOUT_MS_CAP,
    );
    has_overrides |= set_duration_override(
        &mut overrides.effects_timeout,
        body_u64("effectsTimeoutMs"),
        MAX_TIMEOUT_MS_CAP,
    );
    has_overrides |= set_delay_override(
        &mut overrides.initial_retry_delay,
        body_u64("initialRetryDelayMs"),
        MAX_DELAY_MS_CAP,
    );
    has_overrides |= set_delay_override(
        &mut overrides.max_retry_delay,
        body_u64("maxRetryDelayMs"),
        MAX_DELAY_MS_CAP,
    );
    has_overrides |= set_delay_override(
        &mut overrides.effects_fallback_delay,
        body_u64("effectsFallbackDelayMs"),
        MAX_DELAY_MS_CAP,
    );

    if let Some(v) = body_usize("maxRetries") {
        overrides.max_retries = Some(v.min(MAX_RETRIES_CAP));
        has_overrides = true;
    }

    has_overrides |= set_usize_override(
        &mut overrides.max_amplification,
        header_usize(headers, "x-geyser-max-amplification"),
        MAX_AMPLIFICATION_CAP,
    );
    has_overrides |= set_usize_override(
        &mut overrides.owned_amplification,
        header_usize(headers, "x-geyser-owned-amplification"),
        MAX_AMPLIFICATION_CAP,
    );
    has_overrides |= set_usize_override(
        &mut overrides.effects_fallback_fanout,
        header_usize(headers, "x-geyser-effects-fallback-fanout"),
        MAX_EFFECTS_FANOUT_CAP,
    );
    has_overrides |= set_duration_override(
        &mut overrides.submit_timeout,
        header_u64(headers, "x-geyser-submit-timeout-ms"),
        MAX_TIMEOUT_MS_CAP,
    );
    has_overrides |= set_duration_override(
        &mut overrides.effects_timeout,
        header_u64(headers, "x-geyser-effects-timeout-ms"),
        MAX_TIMEOUT_MS_CAP,
    );
    has_overrides |= set_delay_override(
        &mut overrides.initial_retry_delay,
        header_u64(headers, "x-geyser-initial-retry-delay-ms"),
        MAX_DELAY_MS_CAP,
    );
    has_overrides |= set_delay_override(
        &mut overrides.max_retry_delay,
        header_u64(headers, "x-geyser-max-retry-delay-ms"),
        MAX_DELAY_MS_CAP,
    );
    has_overrides |= set_delay_override(
        &mut overrides.effects_fallback_delay,
        header_u64(headers, "x-geyser-effects-fallback-delay-ms"),
        MAX_DELAY_MS_CAP,
    );

    if let Some(v) = header_usize(headers, "x-geyser-max-retries") {
        overrides.max_retries = Some(v.min(MAX_RETRIES_CAP));
        has_overrides = true;
    }

    if has_overrides {
        Some(overrides)
    } else {
        None
    }
}

fn set_usize_override(slot: &mut Option<usize>, value: Option<usize>, max: usize) -> bool {
    if let Some(v) = value {
        *slot = Some(v.clamp(1, max));
        true
    } else {
        false
    }
}

fn set_duration_override(slot: &mut Option<Duration>, value: Option<u64>, max_ms: u64) -> bool {
    if let Some(v) = value {
        *slot = Some(Duration::from_millis(v.clamp(1, max_ms)));
        true
    } else {
        false
    }
}

fn set_delay_override(slot: &mut Option<Duration>, value: Option<u64>, max_ms: u64) -> bool {
    if let Some(v) = value {
        *slot = Some(Duration::from_millis(v.min(max_ms)));
        true
    } else {
        false
    }
}

fn header_u64(headers: &HeaderMap, key: &str) -> Option<u64> {
    let raw = headers.get(key)?;
    match raw.to_str().ok()?.trim().parse::<u64>() {
        Ok(v) => Some(v),
        Err(e) => {
            warn!(header = key, error = %e, "Invalid numeric header override, ignoring");
            None
        }
    }
}

fn header_usize(headers: &HeaderMap, key: &str) -> Option<usize> {
    header_u64(headers, key).map(|v| v as usize)
}

fn parse_json_u64(v: &Value) -> Option<u64> {
    if let Some(num) = v.as_u64() {
        return Some(num);
    }
    v.as_str().and_then(|s| s.trim().parse::<u64>().ok())
}

fn parse_json_usize(v: &Value) -> Option<usize> {
    parse_json_u64(v).map(|n| n as usize)
}

fn overrides_to_json(overrides: Option<&DriveOverrides>) -> Option<Value> {
    let o = overrides?;
    let mut out = serde_json::Map::new();
    if let Some(v) = o.max_amplification {
        out.insert("maxAmplification".to_string(), Value::from(v as u64));
    }
    if let Some(v) = o.owned_amplification {
        out.insert("ownedAmplification".to_string(), Value::from(v as u64));
    }
    if let Some(v) = o.effects_fallback_fanout {
        out.insert("effectsFallbackFanout".to_string(), Value::from(v as u64));
    }
    if let Some(v) = o.max_retries {
        out.insert("maxRetries".to_string(), Value::from(v as u64));
    }
    if let Some(v) = o.submit_timeout {
        out.insert(
            "submitTimeoutMs".to_string(),
            Value::from(v.as_millis() as u64),
        );
    }
    if let Some(v) = o.effects_timeout {
        out.insert(
            "effectsTimeoutMs".to_string(),
            Value::from(v.as_millis() as u64),
        );
    }
    if let Some(v) = o.initial_retry_delay {
        out.insert(
            "initialRetryDelayMs".to_string(),
            Value::from(v.as_millis() as u64),
        );
    }
    if let Some(v) = o.max_retry_delay {
        out.insert(
            "maxRetryDelayMs".to_string(),
            Value::from(v.as_millis() as u64),
        );
    }
    if let Some(v) = o.effects_fallback_delay {
        out.insert(
            "effectsFallbackDelayMs".to_string(),
            Value::from(v.as_millis() as u64),
        );
    }

    if out.is_empty() {
        None
    } else {
        Some(Value::Object(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_parse_drive_overrides_from_headers_and_body() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-geyser-max-amplification",
            HeaderValue::from_static("500"),
        );
        headers.insert(
            "x-geyser-submit-timeout-ms",
            HeaderValue::from_static("25000"),
        );

        let params = vec![
            Value::Null,
            Value::Null,
            serde_json::json!({
                "showEffects": true,
                "geyser": {
                    "ownedAmplification": 9,
                    "effectsFallbackFanout": 12,
                    "maxRetries": 20,
                    "effectsFallbackDelayMs": 33
                }
            }),
        ];

        let overrides = parse_drive_overrides(&headers, &params).expect("expected overrides");
        assert_eq!(overrides.max_amplification, Some(MAX_AMPLIFICATION_CAP));
        assert_eq!(overrides.owned_amplification, Some(9));
        assert_eq!(overrides.effects_fallback_fanout, Some(12));
        assert_eq!(overrides.max_retries, Some(MAX_RETRIES_CAP));
        assert_eq!(
            overrides.effects_fallback_delay,
            Some(Duration::from_millis(33))
        );
        assert_eq!(
            overrides.submit_timeout,
            Some(Duration::from_millis(25_000))
        );
    }
}
