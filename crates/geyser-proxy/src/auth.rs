use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;

pub struct ApiKeyConfig {
    pub keys: Vec<String>, // empty = open access (default)
}

pub async fn validate_api_key(
    config: Arc<ApiKeyConfig>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if config.keys.is_empty() {
        return Ok(next.run(req).await);
    }
    match req
        .headers()
        .get("x-geyser-key")
        .and_then(|v| v.to_str().ok())
    {
        Some(k) if config.keys.iter().any(|key| key == k) => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}
