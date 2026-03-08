use std::sync::Arc;

use axum::Router;
use axum::routing::{delete, get, post, put};

use super::handlers::{
    AppState, certified_write, eventual_write, get_authority_definition, get_certification_status,
    get_certified, get_eventual, get_internal_frontiers, get_metrics, get_policy, get_slo,
    get_topology, get_version_history, internal_announce, internal_delta_sync, internal_join,
    internal_keys, internal_leave, internal_ping, internal_sync, list_authorities, list_policies,
    post_internal_frontiers, remove_policy, set_authority_definition, set_placement_policy,
    verify_proof,
};

/// Build the HTTP API router with all endpoints.
///
/// When `AppState::internal_token` is `Some`, the `/api/internal/*`
/// routes and control-plane mutation routes (`PUT`, `DELETE`) are
/// protected by Bearer token authentication. When `None`, all routes
/// are open (backwards-compatible).
pub fn router(state: Arc<AppState>) -> Router {
    // Internal routes sub-router. Conditionally wrapped with auth middleware.
    let internal_routes = Router::new()
        .route(
            "/api/internal/frontiers",
            post(post_internal_frontiers).get(get_internal_frontiers),
        )
        .route("/api/internal/sync", post(internal_sync))
        .route("/api/internal/sync/delta", post(internal_delta_sync))
        .route("/api/internal/keys", get(internal_keys))
        .route("/api/internal/join", post(internal_join))
        .route("/api/internal/leave", post(internal_leave))
        .route("/api/internal/announce", post(internal_announce))
        .route("/api/internal/ping", post(internal_ping));

    // Control-plane mutation routes sub-router (PUT / DELETE).
    // These require internal token auth like the internal routes.
    let cp_mutation_routes = Router::new()
        .route(
            "/api/control-plane/authorities",
            put(set_authority_definition),
        )
        .route("/api/control-plane/policies", put(set_placement_policy))
        .route(
            "/api/control-plane/policies/{prefix}",
            delete(remove_policy),
        );

    let (internal_routes, cp_mutation_routes) = if let Some(ref token) = state.internal_token {
        let token1 = token.clone();
        let token2 = token.clone();
        let internal_routes = internal_routes.layer(axum::middleware::from_fn(move |req, next| {
            let t = token1.clone();
            super::auth::require_bearer_token(req, next, t)
        }));
        let cp_mutation_routes =
            cp_mutation_routes.layer(axum::middleware::from_fn(move |req, next| {
                let t = token2.clone();
                super::auth::require_bearer_token(req, next, t)
            }));
        (internal_routes, cp_mutation_routes)
    } else {
        (internal_routes, cp_mutation_routes)
    };

    Router::new()
        .route("/api/eventual/write", post(eventual_write))
        .route("/api/eventual/{*key}", get(get_eventual))
        .route("/api/certified/write", post(certified_write))
        .route("/api/certified/verify", post(verify_proof))
        .route("/api/certified/{*key}", get(get_certified))
        .route("/api/status/{*key}", get(get_certification_status))
        .merge(internal_routes)
        .merge(cp_mutation_routes)
        // Control-plane read-only endpoints (public)
        .route("/api/control-plane/authorities", get(list_authorities))
        .route(
            "/api/control-plane/authorities/{prefix}",
            get(get_authority_definition),
        )
        .route("/api/control-plane/policies", get(list_policies))
        .route("/api/control-plane/policies/{prefix}", get(get_policy))
        .route("/api/control-plane/versions", get(get_version_history))
        .route("/api/topology", get(get_topology))
        .route("/api/metrics", get(get_metrics))
        .route("/api/slo", get(get_slo))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::certified::CertifiedApi;
    use crate::api::eventual::EventualApi;
    use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
    use crate::http::types::{
        AuthorityDefinitionResponse, CertifiedReadResponse, CertifiedWriteResponse, CrdtValueJson,
        EventualReadResponse, PlacementPolicyResponse, StatusResponse, VersionHistoryResponse,
        WriteResponse,
    };
    use crate::ops::metrics::RuntimeMetrics;
    use crate::placement::PlacementPolicy;
    use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::RwLock;
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        test_state_with_token(None)
    }

    fn test_state_with_token(token: Option<String>) -> Arc<AppState> {
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
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(
            PolicyVersion(1),
            KeyRange {
                prefix: String::new(),
            },
            3,
        ));

        let namespace = Arc::new(RwLock::new(ns));

        Arc::new(AppState {
            eventual: Arc::new(Mutex::new(EventualApi::new(node_id.clone()))),
            certified: Arc::new(Mutex::new(CertifiedApi::new(
                node_id,
                Arc::clone(&namespace),
            ))),
            namespace,
            metrics: Arc::new(RuntimeMetrics::default()),
            peers: None,
            peer_persist_path: None,
            consensus: Arc::new(Mutex::new(
                crate::control_plane::consensus::ControlPlaneConsensus::new(vec![
                    NodeId("auth-1".into()),
                    NodeId("auth-2".into()),
                    NodeId("auth-3".into()),
                ]),
            )),
            internal_token: token,
            self_node_id: None,
            self_addr: None,
            latency_model: None,
            cluster_nodes: None,
            slo_tracker: Arc::new(crate::ops::slo::SloTracker::new()),
            keyset_registry: Some(Arc::new(RwLock::new(
                crate::authority::certificate::KeysetRegistry::new(),
            ))),
            epoch_config: crate::authority::certificate::EpochConfig::default(),
            current_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
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

    // ---------------------------------------------------------------
    // Control-plane: Authority definitions
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_list_authorities() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .uri("/api/control-plane/authorities")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let defs: Vec<AuthorityDefinitionResponse> = serde_json::from_str(&body).unwrap();
        // test_state creates one catch-all authority definition
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].key_range_prefix, "");
        assert_eq!(defs[0].authority_nodes.len(), 3);
    }

    #[tokio::test]
    async fn control_plane_set_and_get_authority() {
        let state = test_state();
        let app = router(state);

        // Set a new authority definition (majority: 2 of 3 approvals)
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/authorities")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"user/","authority_nodes":["auth-u1","auth-u2"],"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let def: AuthorityDefinitionResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(def.key_range_prefix, "user/");
        assert_eq!(def.authority_nodes, vec!["auth-u1", "auth-u2"]);

        // Get it back
        let req = Request::builder()
            .uri("/api/control-plane/authorities/user%2F")
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let def: AuthorityDefinitionResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(def.key_range_prefix, "user/");
        assert_eq!(def.authority_nodes.len(), 2);

        // List should now have 2 definitions
        let req = Request::builder()
            .uri("/api/control-plane/authorities")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let defs: Vec<AuthorityDefinitionResponse> = serde_json::from_str(&body).unwrap();
        assert_eq!(defs.len(), 2);
    }

    #[tokio::test]
    async fn control_plane_get_nonexistent_authority() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .uri("/api/control-plane/authorities/missing%2F")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ---------------------------------------------------------------
    // Control-plane: Placement policies
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_set_and_get_policy() {
        let state = test_state();
        let app = router(state);

        // Set a placement policy (majority: 2 of 3 approvals)
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"user/","replica_count":3,"required_tags":["dc:tokyo"],"certified":true,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let policy: PlacementPolicyResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(policy.key_range_prefix, "user/");
        assert_eq!(policy.replica_count, 3);
        assert!(policy.certified);
        assert_eq!(policy.required_tags, vec!["dc:tokyo"]);

        // Get it back
        let req = Request::builder()
            .uri("/api/control-plane/policies/user%2F")
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let policy: PlacementPolicyResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(policy.key_range_prefix, "user/");
        assert_eq!(policy.replica_count, 3);
    }

    #[tokio::test]
    async fn control_plane_list_policies_has_default() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .uri("/api/control-plane/policies")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let policies: Vec<PlacementPolicyResponse> = serde_json::from_str(&body).unwrap();
        assert_eq!(policies.len(), 1, "expected default placement policy");
    }

    #[tokio::test]
    async fn control_plane_remove_policy() {
        let state = test_state();
        let app = router(state);

        // First set a policy (with majority approvals)
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"data/","replica_count":5,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();

        // Remove it (with majority approvals)
        let req = Request::builder()
            .method("DELETE")
            .uri("/api/control-plane/policies/data%2F")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"approvals":["auth-1","auth-2"]}"#))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let removed: PlacementPolicyResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(removed.key_range_prefix, "data/");

        // Should be gone now
        let req = Request::builder()
            .uri("/api/control-plane/policies/data%2F")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn control_plane_remove_nonexistent_policy() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("DELETE")
            .uri("/api/control-plane/policies/missing%2F")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"approvals":["auth-1","auth-2"]}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ---------------------------------------------------------------
    // Control-plane: Version history
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_version_history() {
        let state = test_state();
        let app = router(state);

        // Initial version history (namespace had 1 authority set + 1 placement policy -> version 3)
        let req = Request::builder()
            .uri("/api/control-plane/versions")
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let versions: VersionHistoryResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(versions.current_version, 3);
        assert_eq!(versions.history, vec![1, 2, 3]);

        // Set a policy -> version should increment
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"test/","replica_count":1,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();

        // Check version history again
        let req = Request::builder()
            .uri("/api/control-plane/versions")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let versions: VersionHistoryResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(versions.current_version, 4);
        assert_eq!(versions.history, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn control_plane_update_policy_increments_version() {
        let state = test_state();
        let app = router(state);

        // Set policy twice -> version should increment each time
        for i in 0..2 {
            let req = Request::builder()
                .method("PUT")
                .uri("/api/control-plane/policies")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"key_range_prefix":"data/","replica_count":{},"approvals":["auth-1","auth-2"]}}"#,
                    i + 1
                )))
                .unwrap();
            app.clone().oneshot(req).await.unwrap();
        }

        let req = Request::builder()
            .uri("/api/control-plane/versions")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let versions: VersionHistoryResponse = serde_json::from_str(&body).unwrap();
        // initial(1) + auth_def(2) + default_policy(3) + policy_set(4) + policy_set(5)
        assert_eq!(versions.current_version, 5);
        assert_eq!(versions.history.len(), 5);
    }

    // ---------------------------------------------------------------
    // Control-plane: Consensus enforcement (FR-009)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_authority_without_majority_returns_403() {
        let state = test_state();
        let app = router(state);

        // Only 1 approval (need 2 of 3 for majority)
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/authorities")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"denied/","authority_nodes":["a1"],"approvals":["auth-1"]}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let body = body_string(resp.into_body()).await;
        let err: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(err["error_code"], "POLICY_DENIED");

        // Verify it was not applied
        let req = Request::builder()
            .uri("/api/control-plane/authorities/denied%2F")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn control_plane_policy_without_majority_returns_403() {
        let state = test_state();
        let app = router(state);

        // Empty approvals (need 2 of 3 for majority)
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"denied/","replica_count":3,"approvals":[]}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let body = body_string(resp.into_body()).await;
        let err: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(err["error_code"], "POLICY_DENIED");

        // Verify it was not applied
        let req = Request::builder()
            .uri("/api/control-plane/policies/denied%2F")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn control_plane_authority_with_majority_succeeds() {
        let state = test_state();
        let app = router(state);

        // 2 of 3 approvals = majority
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/authorities")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"ok/","authority_nodes":["a1","a2"],"approvals":["auth-1","auth-3"]}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify it was applied
        let req = Request::builder()
            .uri("/api/control-plane/authorities/ok%2F")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let def: AuthorityDefinitionResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(def.key_range_prefix, "ok/");
        assert_eq!(def.authority_nodes, vec!["a1", "a2"]);
    }

    #[tokio::test]
    async fn control_plane_policy_with_majority_succeeds() {
        let state = test_state();
        let app = router(state);

        // All 3 approvals
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"ok/","replica_count":5,"approvals":["auth-1","auth-2","auth-3"]}"#,
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify it was applied
        let req = Request::builder()
            .uri("/api/control-plane/policies/ok%2F")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let policy: PlacementPolicyResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(policy.key_range_prefix, "ok/");
        assert_eq!(policy.replica_count, 5);
    }

    #[tokio::test]
    async fn control_plane_non_authority_approvals_rejected() {
        let state = test_state();
        let app = router(state);

        // 2 approvals but from non-authority nodes
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"bad/","replica_count":3,"approvals":["unknown-1","unknown-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ---------------------------------------------------------------
    // Metrics endpoint
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn metrics_endpoint_returns_valid_json() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .uri("/api/metrics")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();

        // Verify all expected fields are present.
        assert!(json.get("pending_count").is_some());
        assert!(json.get("certified_total").is_some());
        assert!(json.get("certification_latency_mean_us").is_some());
        assert!(json.get("frontier_skew_ms").is_some());
        assert!(json.get("sync_failure_rate").is_some());
        assert!(json.get("sync_attempt_total").is_some());
        assert!(json.get("sync_failure_total").is_some());
        assert!(json.get("peer_sync").is_some());
        assert!(json.get("certification_latency_window").is_some());

        // Default values should be zero.
        assert_eq!(json["pending_count"], 0);
        assert_eq!(json["certified_total"], 0);
        assert_eq!(json["frontier_skew_ms"], 0);
        assert_eq!(json["sync_attempt_total"], 0);
        assert_eq!(json["sync_failure_total"], 0);

        // New fields should have default/empty values.
        assert!(json["peer_sync"].as_object().unwrap().is_empty());
        assert_eq!(json["certification_latency_window"]["sample_count"], 0);
    }

    #[tokio::test]
    async fn metrics_endpoint_reflects_updated_values() {
        use std::sync::atomic::Ordering;

        let state = test_state();
        state.metrics.pending_count.store(5, Ordering::Relaxed);
        state.metrics.certified_total.store(10, Ordering::Relaxed);
        state
            .metrics
            .certification_latency_sum_us
            .store(2000, Ordering::Relaxed);
        state
            .metrics
            .certification_latency_count
            .store(4, Ordering::Relaxed);
        state.metrics.frontier_skew_ms.store(42, Ordering::Relaxed);
        state
            .metrics
            .sync_attempt_total
            .store(20, Ordering::Relaxed);
        state.metrics.sync_failure_total.store(3, Ordering::Relaxed);

        let app = router(state);

        let req = Request::builder()
            .uri("/api/metrics")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(json["pending_count"], 5);
        assert_eq!(json["certified_total"], 10);
        assert_eq!(json["frontier_skew_ms"], 42);
        assert_eq!(json["sync_attempt_total"], 20);
        assert_eq!(json["sync_failure_total"], 3);
        // Mean latency: 2000 / 4 = 500.0
        assert!((json["certification_latency_mean_us"].as_f64().unwrap() - 500.0).abs() < 0.01);
        // Failure rate: 3 / 20 = 0.15
        assert!((json["sync_failure_rate"].as_f64().unwrap() - 0.15).abs() < 0.01);
    }

    #[tokio::test]
    async fn metrics_endpoint_includes_peer_sync_and_cert_window() {
        use std::time::Duration;

        let state = test_state();

        // Record per-peer sync metrics.
        state
            .metrics
            .record_peer_sync_success("node-a", Duration::from_millis(10));
        state
            .metrics
            .record_peer_sync_success("node-a", Duration::from_millis(20));
        state.metrics.record_peer_sync_failure("node-b");

        // Record certification latency window samples.
        state
            .metrics
            .record_certification_latency(Duration::from_millis(50));

        let app = router(state);

        let req = Request::builder()
            .uri("/api/metrics")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();

        // Verify per-peer sync stats.
        let peer_sync = json["peer_sync"].as_object().unwrap();
        assert_eq!(peer_sync.len(), 2);

        let node_a = &peer_sync["node-a"];
        assert_eq!(node_a["success_count"], 2);
        assert_eq!(node_a["failure_count"], 0);
        // Mean of 10ms and 20ms = 15ms = 15000us
        assert!((node_a["mean_latency_us"].as_f64().unwrap() - 15000.0).abs() < 1.0);

        let node_b = &peer_sync["node-b"];
        assert_eq!(node_b["success_count"], 0);
        assert_eq!(node_b["failure_count"], 1);

        // Verify certification latency window.
        let cert_window = &json["certification_latency_window"];
        assert_eq!(cert_window["sample_count"], 1);
        assert!((cert_window["mean_us"].as_f64().unwrap() - 50000.0).abs() < 1.0);
    }

    // ---------------------------------------------------------------
    // Proof bundle in certified read (pending -> no proof)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn certified_read_pending_has_no_proof() {
        let state = test_state();
        let app = router(state);

        // Write a certified value (stays pending since no frontiers).
        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/write")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key":"k1","value":{"type":"counter","value":1}}"#,
            ))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();

        // Read it back -- should be pending with no proof.
        let req = Request::builder()
            .uri("/api/certified/k1")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let read_resp: CertifiedReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(read_resp.status, CertificationStatus::Pending);
        assert!(
            read_resp.proof.is_none(),
            "proof should be None for pending"
        );
    }

    // ---------------------------------------------------------------
    // Proof bundle in certified read (certified -> has proof)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn certified_read_certified_has_proof() {
        use crate::authority::ack_frontier::AckFrontier;
        use crate::hlc::HlcTimestamp;
        use crate::types::PolicyVersion;

        let state = test_state();

        // Write a certified value and advance frontiers to certify it.
        {
            let mut api = state.certified.lock().await;
            let val = {
                use crate::crdt::pn_counter::PnCounter;
                use crate::store::kv::CrdtValue;
                let mut c = PnCounter::new();
                c.increment(&NodeId("writer".into()));
                CrdtValue::Counter(c)
            };
            api.certified_write("k2".into(), val, crate::api::certified::OnTimeout::Pending)
                .unwrap();

            let write_ts = api.pending_writes()[0].timestamp.physical;

            // Advance 2 of 3 authorities.
            api.update_frontier(AckFrontier {
                authority_id: NodeId("auth-1".into()),
                frontier_hlc: HlcTimestamp {
                    physical: write_ts + 100,
                    logical: 0,
                    node_id: "auth-1".into(),
                },
                key_range: KeyRange {
                    prefix: String::new(),
                },
                policy_version: PolicyVersion(1),
                digest_hash: "h1".into(),
            });
            api.update_frontier(AckFrontier {
                authority_id: NodeId("auth-2".into()),
                frontier_hlc: HlcTimestamp {
                    physical: write_ts + 200,
                    logical: 0,
                    node_id: "auth-2".into(),
                },
                key_range: KeyRange {
                    prefix: String::new(),
                },
                policy_version: PolicyVersion(1),
                digest_hash: "h2".into(),
            });
            api.process_certifications();
        }

        let app = router(state);

        let req = Request::builder()
            .uri("/api/certified/k2")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let read_resp: CertifiedReadResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(read_resp.status, CertificationStatus::Certified);
        assert!(
            read_resp.proof.is_some(),
            "proof should be present when certified"
        );

        let proof = read_resp.proof.unwrap();
        assert_eq!(proof.total_authorities, 3);
        assert_eq!(proof.contributing_authorities.len(), 2);
        assert!(
            proof
                .contributing_authorities
                .contains(&"auth-1".to_string())
        );
        assert!(
            proof
                .contributing_authorities
                .contains(&"auth-2".to_string())
        );
    }

    // ---------------------------------------------------------------
    // Verify proof endpoint
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn verify_proof_valid() {
        use crate::authority::certificate::{
            KeysetVersion, create_certificate_message, sign_message,
        };
        use crate::http::types::VerifyProofResponse;
        use crate::types::PolicyVersion;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        // Build a real certificate with valid Ed25519 signatures.
        let kr = KeyRange {
            prefix: "user/".into(),
        };
        let hlc = crate::hlc::HlcTimestamp {
            physical: 1000,
            logical: 0,
            node_id: "auth-1".into(),
        };
        let pv = PolicyVersion(1);
        let message = create_certificate_message(&kr, &hlc, &pv);

        let auth_ids = ["auth-1", "auth-2", "auth-3"];
        let mut sigs_json = Vec::new();
        let mut registry_keys = Vec::new();
        for auth_id in &auth_ids {
            let sk = SigningKey::generate(&mut OsRng);
            let vk = sk.verifying_key();
            let sig = sign_message(&sk, &message);
            let pk_hex: String = vk.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
            let sig_hex: String = sig.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
            registry_keys.push((NodeId(auth_id.to_string()), vk));
            sigs_json.push(serde_json::json!({
                "authority_id": auth_id,
                "public_key": pk_hex,
                "signature": sig_hex,
                "keyset_version": 1
            }));
        }

        let body_json = serde_json::json!({
            "key_range_prefix": "user/",
            "frontier": {"physical": 1000, "logical": 0, "node_id": "auth-1"},
            "policy_version": 1,
            "contributing_authorities": auth_ids,
            "total_authorities": 5,
            "certificate": {
                "keyset_version": 1,
                "signatures": sigs_json
            }
        });

        // Build state with a keyset registry containing the test keys.
        let state = test_state();
        {
            let registry_lock = state.keyset_registry.as_ref().unwrap();
            let mut registry = registry_lock.write().unwrap();
            registry
                .register_keyset(KeysetVersion(1), 0, registry_keys)
                .unwrap();
        }
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/verify")
            .header("content-type", "application/json")
            .body(Body::from(body_json.to_string()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let result: VerifyProofResponse = serde_json::from_str(&body).unwrap();
        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.contributing_count, 3);
        assert_eq!(result.required_count, 3); // 5/2+1 = 3
    }

    #[tokio::test]
    async fn verify_proof_without_certificate_rejected() {
        use crate::http::types::VerifyProofResponse;

        let state = test_state();
        let app = router(state);

        // A proof without a certificate must be rejected even when
        // enough contributing authorities are listed.
        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/verify")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{
                    "key_range_prefix": "user/",
                    "frontier": {"physical": 1000, "logical": 0, "node_id": "auth-1"},
                    "policy_version": 1,
                    "contributing_authorities": ["auth-1", "auth-2", "auth-3"],
                    "total_authorities": 5
                }"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let result: VerifyProofResponse = serde_json::from_str(&body).unwrap();
        assert!(!result.valid);
        assert!(result.has_majority);
        assert_eq!(result.contributing_count, 3);
        assert_eq!(result.required_count, 3);
    }

    #[tokio::test]
    async fn verify_proof_insufficient_authorities() {
        use crate::http::types::VerifyProofResponse;

        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/api/certified/verify")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{
                    "key_range_prefix": "user/",
                    "frontier": {"physical": 1000, "logical": 0, "node_id": "auth-1"},
                    "policy_version": 1,
                    "contributing_authorities": ["auth-1"],
                    "total_authorities": 5
                }"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let result: VerifyProofResponse = serde_json::from_str(&body).unwrap();
        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.contributing_count, 1);
        assert_eq!(result.required_count, 3);
    }

    // ---------------------------------------------------------------
    // Internal frontier push: accepted count reflects actual changes (#105)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn post_internal_frontiers_accepted_counts_actual_changes() {
        use crate::network::frontier_sync::FrontierPushResponse;

        let state = test_state();
        let app = router(state);

        let frontier_json = r#"{
            "frontiers": [
                {
                    "authority_id": "auth-1",
                    "frontier_hlc": {"physical": 100, "logical": 0, "node_id": "auth-1"},
                    "key_range": {"prefix": ""},
                    "policy_version": 1,
                    "digest_hash": "h1"
                }
            ]
        }"#;

        // First push: frontier is new, accepted should be 1.
        let req = Request::builder()
            .method("POST")
            .uri("/api/internal/frontiers")
            .header("content-type", "application/json")
            .body(Body::from(frontier_json))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let result: FrontierPushResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(result.accepted, 1, "new frontier should be accepted");

        // Second push: same frontier (stale/duplicate), accepted should be 0.
        let req = Request::builder()
            .method("POST")
            .uri("/api/internal/frontiers")
            .header("content-type", "application/json")
            .body(Body::from(frontier_json))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let result: FrontierPushResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(
            result.accepted, 0,
            "duplicate frontier should not be accepted"
        );

        // Third push: newer frontier, accepted should be 1.
        let newer_json = r#"{
            "frontiers": [
                {
                    "authority_id": "auth-1",
                    "frontier_hlc": {"physical": 200, "logical": 0, "node_id": "auth-1"},
                    "key_range": {"prefix": ""},
                    "policy_version": 1,
                    "digest_hash": "h2"
                }
            ]
        }"#;

        let req = Request::builder()
            .method("POST")
            .uri("/api/internal/frontiers")
            .header("content-type", "application/json")
            .body(Body::from(newer_json))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_string(resp.into_body()).await;
        let result: FrontierPushResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(result.accepted, 1, "newer frontier should be accepted");
    }

    // ---------------------------------------------------------------
    // Internal API auth tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn internal_route_without_token_config_allows_all() {
        // No internal_token configured -> all requests pass without auth.
        let state = test_state_with_token(None);
        let app = router(state);

        let req = Request::builder()
            .uri("/api/internal/keys")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn internal_route_rejects_missing_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .uri("/api/internal/keys")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn internal_route_rejects_wrong_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .uri("/api/internal/keys")
            .header("authorization", "Bearer wrong-secret")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn internal_route_accepts_correct_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .uri("/api/internal/keys")
            .header("authorization", "Bearer test-secret")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn public_route_unaffected_by_internal_token() {
        // Even with internal_token configured, public routes remain open.
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .uri("/api/eventual/nonexistent")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ---------------------------------------------------------------
    // Control-plane mutation auth tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn cp_put_authorities_rejects_missing_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/authorities")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"x/","authority_nodes":["a"],"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cp_put_authorities_rejects_wrong_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/authorities")
            .header("content-type", "application/json")
            .header("authorization", "Bearer wrong-secret")
            .body(Body::from(
                r#"{"key_range_prefix":"x/","authority_nodes":["a"],"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cp_put_authorities_accepts_correct_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/authorities")
            .header("content-type", "application/json")
            .header("authorization", "Bearer test-secret")
            .body(Body::from(
                r#"{"key_range_prefix":"x/","authority_nodes":["a"],"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cp_put_policies_rejects_missing_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"x/","replica_count":3,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cp_put_policies_rejects_wrong_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .header("authorization", "Bearer wrong-secret")
            .body(Body::from(
                r#"{"key_range_prefix":"x/","replica_count":3,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cp_put_policies_accepts_correct_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .header("authorization", "Bearer test-secret")
            .body(Body::from(
                r#"{"key_range_prefix":"x/","replica_count":3,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cp_delete_policy_rejects_missing_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("DELETE")
            .uri("/api/control-plane/policies/x%2F")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"approvals":["auth-1","auth-2"]}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cp_delete_policy_rejects_wrong_token() {
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        let req = Request::builder()
            .method("DELETE")
            .uri("/api/control-plane/policies/x%2F")
            .header("content-type", "application/json")
            .header("authorization", "Bearer wrong-secret")
            .body(Body::from(r#"{"approvals":["auth-1","auth-2"]}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cp_read_routes_unaffected_by_internal_token() {
        // GET control-plane routes should remain open even with auth configured.
        let state = test_state_with_token(Some("test-secret".into()));
        let app = router(state);

        // GET authorities (list)
        let req = Request::builder()
            .uri("/api/control-plane/authorities")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // GET policies (list)
        let req = Request::builder()
            .uri("/api/control-plane/policies")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // GET versions
        let req = Request::builder()
            .uri("/api/control-plane/versions")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cp_mutation_without_token_config_allows_all() {
        // No internal_token configured -> all mutation requests pass without auth.
        let state = test_state_with_token(None);
        let app = router(state);

        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"x/","replica_count":3,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ---------------------------------------------------------------
    // Control-plane: DELETE quorum enforcement
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn control_plane_remove_policy_without_majority_returns_403() {
        let state = test_state();
        let app = router(state);

        // First set a policy
        let req = Request::builder()
            .method("PUT")
            .uri("/api/control-plane/policies")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"key_range_prefix":"data/","replica_count":5,"approvals":["auth-1","auth-2"]}"#,
            ))
            .unwrap();
        app.clone().oneshot(req).await.unwrap();

        // Try to remove with insufficient approvals (only 1 of 3)
        let req = Request::builder()
            .method("DELETE")
            .uri("/api/control-plane/policies/data%2F")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"approvals":["auth-1"]}"#))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Policy should still exist
        let req = Request::builder()
            .uri("/api/control-plane/policies/data%2F")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
