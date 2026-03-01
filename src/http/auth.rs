use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

use super::handlers::AppState;

/// Middleware that validates Bearer token authentication for internal API endpoints.
///
/// If `AppState::internal_token` is `None`, all requests are allowed (backwards
/// compatibility). Otherwise, the request must include a valid
/// `Authorization: Bearer <token>` header.
pub async fn require_internal_token(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let expected_token = match &state.internal_token {
        Some(token) => token,
        None => return next.run(request).await,
    };

    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let provided_token = match auth_header.and_then(|h| h.strip_prefix("Bearer ")) {
        Some(token) => token,
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                "missing or invalid Authorization header",
            )
                .into_response();
        }
    };

    // Constant-time comparison to prevent timing attacks.
    // Length difference is acceptable to leak; the token format is not secret.
    let expected_bytes = expected_token.as_bytes();
    let provided_bytes = provided_token.as_bytes();

    if expected_bytes.len() == provided_bytes.len()
        && bool::from(expected_bytes.ct_eq(provided_bytes))
    {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "invalid token").into_response()
    }
}
