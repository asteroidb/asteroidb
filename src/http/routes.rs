use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use super::handlers::{
    AppState, certified_write, eventual_write, get_certification_status, get_certified,
    get_eventual,
};

/// Build the HTTP API router with all endpoints.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/eventual/write", post(eventual_write))
        .route("/api/eventual/{key}", get(get_eventual))
        .route("/api/certified/write", post(certified_write))
        .route("/api/certified/{key}", get(get_certified))
        .route("/api/status/{key}", get(get_certification_status))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::certified::CertifiedApi;
    use crate::api::eventual::EventualApi;
    use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
    use crate::http::types::{
        CertifiedReadResponse, CertifiedWriteResponse, CrdtValueJson, EventualReadResponse,
        StatusResponse, WriteResponse,
    };
    use crate::types::{CertificationStatus, KeyRange, NodeId};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        let node_id = NodeId("test-node".into());

        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: String::new(),
            },
            authority_nodes: vec![
                NodeId("auth-1".into()),
                NodeId("auth-2".into()),
                NodeId("auth-3".into()),
            ],
        });

        Arc::new(AppState {
            eventual: Mutex::new(EventualApi::new(node_id.clone())),
            certified: Mutex::new(CertifiedApi::new(node_id, ns)),
        })
    }

    async fn body_string(body: Body) -> String {
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    // ---------------------------------------------------------------
    // Eventual write + read round-trip
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn eventual_counter_inc_and_read() {
        let state = test_state();
        let app = router(state);

        // Increment counter
        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"type":"counter_inc","key":"hits"}"#))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let write_resp: WriteResponse = serde_json::from_str(&body).unwrap();
        assert!(write_resp.ok);

        // Read back
        let req = Request::builder()
            .uri("/api/eventual/hits")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let read_resp: EventualReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(read_resp.key, "hits");
        assert_eq!(read_resp.value, Some(CrdtValueJson::Counter { value: 1 }));
    }

    #[tokio::test]
    async fn eventual_counter_dec() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"type":"counter_dec","key":"balance"}"#))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let req = Request::builder()
            .uri("/api/eventual/balance")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let read_resp: EventualReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(read_resp.value, Some(CrdtValueJson::Counter { value: -1 }));
    }

    #[tokio::test]
    async fn eventual_set_add_and_read() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"type":"set_add","key":"users","element":"alice"}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let req = Request::builder()
            .uri("/api/eventual/users")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let read_resp: EventualReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(
            read_resp.value,
            Some(CrdtValueJson::Set {
                elements: vec!["alice".into()]
            })
        );
    }

    #[tokio::test]
    async fn eventual_register_set_and_read() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"type":"register_set","key":"greeting","value":"hello"}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let req = Request::builder()
            .uri("/api/eventual/greeting")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let read_resp: EventualReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(
            read_resp.value,
            Some(CrdtValueJson::Register {
                value: Some("hello".into()),
            })
        );
    }

    #[tokio::test]
    async fn eventual_map_set_and_read() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"type":"map_set","key":"config","map_key":"name","map_value":"AsteroidDB"}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let req = Request::builder()
            .uri("/api/eventual/config")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let read_resp: EventualReadResponse = serde_json::from_str(&body).unwrap();
        match &read_resp.value {
            Some(CrdtValueJson::Map { entries }) => {
                assert_eq!(entries.get("name"), Some(&"AsteroidDB".to_string()));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn eventual_read_nonexistent_key() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .uri("/api/eventual/missing")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let read_resp: EventualReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(read_resp.key, "missing");
        assert!(read_resp.value.is_none());
    }

    // ---------------------------------------------------------------
    // Error handling
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn eventual_type_mismatch_returns_conflict() {
        let state = test_state();
        let app = router(state);

        // First, create a counter
        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"type":"counter_inc","key":"k"}"#))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();

        // Then try to add to it as a set → type mismatch
        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"type":"set_add","key":"k","element":"x"}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        let body = body_string(resp.into_body()).await;
        assert!(body.contains("TYPE_MISMATCH"));
    }

    #[tokio::test]
    async fn eventual_set_remove_key_not_found() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"type":"set_remove","key":"missing","element":"x"}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = body_string(resp.into_body()).await;
        assert!(body.contains("KEY_NOT_FOUND"));
    }

    // ---------------------------------------------------------------
    // Certified write + read
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn certified_write_pending() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key":"sensor","value":{"type":"counter","value":5},"on_timeout":"pending"}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let write_resp: CertifiedWriteResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(write_resp.status, CertificationStatus::Pending);
    }

    #[tokio::test]
    async fn certified_write_on_timeout_error() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key":"sensor","value":{"type":"counter","value":1},"on_timeout":"error"}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);

        let body = body_string(resp.into_body()).await;
        assert!(body.contains("TIMEOUT"));
    }

    #[tokio::test]
    async fn certified_read_returns_status() {
        let state = test_state();
        let app = router(state);

        // Write a certified value
        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key":"data","value":{"type":"register","value":"hello"}}"#,
            ))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();

        // Read it back
        let req = Request::builder()
            .uri("/api/certified/data")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let read_resp: CertifiedReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(read_resp.key, "data");
        assert!(read_resp.value.is_some());
        assert_eq!(read_resp.status, CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // Status endpoint
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn status_endpoint_returns_pending() {
        let state = test_state();
        let app = router(state);

        // Write first
        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key":"k1","value":{"type":"counter","value":1}}"#,
            ))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();

        // Check status
        let req = Request::builder()
            .uri("/api/status/k1")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let status_resp: StatusResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(status_resp.key, "k1");
        assert_eq!(status_resp.status, CertificationStatus::Pending);
    }

    #[tokio::test]
    async fn status_endpoint_nonexistent_returns_pending() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .uri("/api/status/nonexistent")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let status_resp: StatusResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(status_resp.status, CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // Invalid JSON returns 422
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn invalid_json_returns_unprocessable() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/eventual/write")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"not":"valid"}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ---------------------------------------------------------------
    // Multiple operations on same key
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn multiple_counter_increments() {
        let state = test_state();
        let app = router(state);

        for _ in 0..3 {
            let req = Request::builder()
                .method("POST")
                .uri("/api/eventual/write")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"type":"counter_inc","key":"count"}"#))
                .unwrap();
            app.clone().oneshot(req).await.unwrap();
        }

        let req = Request::builder()
            .uri("/api/eventual/count")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let read_resp: EventualReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(read_resp.value, Some(CrdtValueJson::Counter { value: 3 }));
    }

    // ---------------------------------------------------------------
    // Certified write with set value
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn certified_write_set_value() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key":"tags","value":{"type":"set","elements":["a","b"]}}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let req = Request::builder()
            .uri("/api/certified/tags")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let read_resp: CertifiedReadResponse = serde_json::from_str(&body).unwrap();
        assert!(read_resp.value.is_some());
    }
}
