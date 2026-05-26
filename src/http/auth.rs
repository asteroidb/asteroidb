use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

/// Compare two byte slices in constant time, independent of their lengths.
///
/// Pads the shorter slice with zeros before calling `ct_eq` so that the
/// comparison always processes `max(a.len(), b.len())` bytes, preventing
/// a timing side-channel when the two tokens have different lengths.
fn ct_eq_tokens(a: &[u8], b: &[u8]) -> bool {
    let max_len = a.len().max(b.len());
    let mut buf_a = vec![0u8; max_len];
    let mut buf_b = vec![0u8; max_len];
    buf_a[..a.len()].copy_from_slice(a);
    buf_b[..b.len()].copy_from_slice(b);
    buf_a.ct_eq(&buf_b).into()
}

/// Axum middleware that validates Bearer token authentication.
///
/// Extracts the `Authorization` header and checks that it contains a
/// valid `Bearer <token>` value matching the expected token. Returns
/// `401 Unauthorized` if the header is missing, malformed, or contains
/// the wrong token.
pub async fn require_bearer_token(
    request: Request<Body>,
    next: Next,
    expected_token: String,
) -> Response {
    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(header) => match header.strip_prefix("Bearer ") {
            Some(token) if ct_eq_tokens(token.as_bytes(), expected_token.as_bytes()) => {
                next.run(request).await
            }
            _ => StatusCode::UNAUTHORIZED.into_response(),
        },
        None => StatusCode::UNAUTHORIZED.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::middleware;
    use axum::routing::get;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn ok_handler() -> &'static str {
        "ok"
    }

    fn test_app(token: &str) -> Router {
        let token = token.to_string();
        Router::new()
            .route("/protected", get(ok_handler))
            .layer(middleware::from_fn(move |req, next| {
                let t = token.clone();
                require_bearer_token(req, next, t)
            }))
    }

    #[tokio::test]
    async fn request_with_correct_token_succeeds() {
        let app = test_app("secret-token");

        let req = Request::builder()
            .uri("/protected")
            .header("authorization", "Bearer secret-token")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"ok");
    }

    #[tokio::test]
    async fn request_without_token_returns_401() {
        let app = test_app("secret-token");

        let req = Request::builder()
            .uri("/protected")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn request_with_wrong_token_returns_401() {
        let app = test_app("secret-token");

        let req = Request::builder()
            .uri("/protected")
            .header("authorization", "Bearer wrong-token")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn request_with_malformed_header_returns_401() {
        let app = test_app("secret-token");

        let req = Request::builder()
            .uri("/protected")
            .header("authorization", "Basic secret-token")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn request_with_bearer_no_space_returns_401() {
        let app = test_app("secret-token");

        let req = Request::builder()
            .uri("/protected")
            .header("authorization", "Bearersecret-token")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // -----------------------------------------------------------------------
    // Unit tests for ct_eq_tokens (timing side-channel fix)
    // -----------------------------------------------------------------------

    #[test]
    fn ct_eq_tokens_equal_slices() {
        assert!(ct_eq_tokens(b"secret", b"secret"));
    }

    #[test]
    fn ct_eq_tokens_different_content_same_length() {
        assert!(!ct_eq_tokens(b"secret", b"notseq"));
    }

    #[test]
    fn ct_eq_tokens_different_lengths_both_wrong() {
        // Tokens of different lengths must always be rejected, regardless of
        // their content, to prevent a timing side-channel on length comparison.
        assert!(!ct_eq_tokens(b"short", b"longer-token"));
        assert!(!ct_eq_tokens(b"longer-token", b"short"));
    }

    #[test]
    fn ct_eq_tokens_length_mismatch_with_correct_prefix() {
        // A prefix match must not cause acceptance when lengths differ.
        assert!(!ct_eq_tokens(b"secret", b"secret-extra"));
        assert!(!ct_eq_tokens(b"secret-extra", b"secret"));
    }

    #[test]
    fn ct_eq_tokens_empty_vs_nonempty() {
        assert!(!ct_eq_tokens(b"", b"token"));
        assert!(!ct_eq_tokens(b"token", b""));
    }

    #[test]
    fn ct_eq_tokens_both_empty() {
        assert!(ct_eq_tokens(b"", b""));
    }
}
