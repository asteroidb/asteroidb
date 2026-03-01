use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

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
            Some(token) if token == expected_token => next.run(request).await,
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
}
