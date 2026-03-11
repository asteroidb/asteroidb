#[cfg(feature = "server")]
use std::sync::Arc;

#[cfg(feature = "server")]
use axum::Router;
#[cfg(feature = "server")]
use axum::routing::{delete, get, post, put};

#[cfg(feature = "server")]
use crate::handlers;
#[cfg(feature = "server")]
use crate::proxy::AsteroidProxy;

/// Build the full API router.
#[cfg(feature = "server")]
pub fn api_router(proxy: Arc<AsteroidProxy>) -> Router {
    Router::new()
        // Task CRUD
        .route("/bff/api/tasks", post(handlers::create_task))
        .route("/bff/api/tasks", get(handlers::list_tasks))
        .route("/bff/api/tasks/{task_id}", delete(handlers::delete_task))
        // Task mutations
        .route(
            "/bff/api/tasks/{task_id}/vote",
            post(handlers::vote_task),
        )
        .route(
            "/bff/api/tasks/{task_id}/tags",
            post(handlers::update_tags),
        )
        .route(
            "/bff/api/tasks/{task_id}/metadata",
            put(handlers::update_metadata),
        )
        .route(
            "/bff/api/tasks/{task_id}/status",
            put(handlers::update_status),
        )
        // Certification
        .route(
            "/bff/api/tasks/{task_id}/cert",
            get(handlers::get_cert_status),
        )
        .route(
            "/bff/api/tasks/{task_id}/verify",
            post(handlers::verify_task_proof),
        )
        // Cluster passthrough
        .route("/bff/api/cluster/metrics", get(handlers::get_metrics))
        .route("/bff/api/cluster/slo", get(handlers::get_slo))
        .route("/bff/api/cluster/topology", get(handlers::get_topology))
        .route("/bff/api/cluster/health", get(handlers::get_health))
        .with_state(proxy)
}
