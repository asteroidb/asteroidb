#[cfg(feature = "server")]
use axum::http::StatusCode;
#[cfg(feature = "server")]
use axum::response::{IntoResponse, Response};

/// Unified error type for the BFF server.
#[derive(Debug)]
pub enum AppError {
    /// Error communicating with AsteroidDB.
    Upstream(String),
    /// Bad client request.
    BadRequest(String),
    /// Resource not found.
    NotFound(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Upstream(msg) => write!(f, "upstream error: {msg}"),
            AppError::BadRequest(msg) => write!(f, "bad request: {msg}"),
            AppError::NotFound(msg) => write!(f, "not found: {msg}"),
        }
    }
}

impl std::error::Error for AppError {}

#[cfg(feature = "server")]
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Upstream(msg) => (StatusCode::BAD_GATEWAY, msg.clone()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
        };

        let body = serde_json::json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}

#[cfg(feature = "server")]
impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        AppError::Upstream(err.to_string())
    }
}
