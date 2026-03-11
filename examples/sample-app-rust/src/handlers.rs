#[cfg(feature = "server")]
use std::sync::Arc;

#[cfg(feature = "server")]
use axum::Json;
#[cfg(feature = "server")]
use axum::extract::{Path, State};

#[cfg(feature = "server")]
use crate::error::AppError;
#[cfg(feature = "server")]
use crate::proxy::AsteroidProxy;
#[cfg(feature = "server")]
use crate::shared::types::*;

#[cfg(feature = "server")]
type AppState = Arc<AsteroidProxy>;

// ---------------------------------------------------------------
// Task CRUD
// ---------------------------------------------------------------

/// Create a new task: writes metadata, status, and adds to the task index.
#[cfg(feature = "server")]
pub async fn create_task(
    State(proxy): State<AppState>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<Json<CreateTaskResponse>, AppError> {
    let task_id = uuid::Uuid::new_v4().to_string();

    // Write title to OR-Map
    proxy
        .eventual_write(serde_json::json!({
            "type": "map_set",
            "key": format!("task/{task_id}/meta"),
            "map_key": "title",
            "map_value": req.title,
        }))
        .await?;

    // Write description to OR-Map
    proxy
        .eventual_write(serde_json::json!({
            "type": "map_set",
            "key": format!("task/{task_id}/meta"),
            "map_key": "description",
            "map_value": req.description,
        }))
        .await?;

    // Set initial status to "todo" via LWW-Register
    proxy
        .eventual_write(serde_json::json!({
            "type": "register_set",
            "key": format!("task/{task_id}/status"),
            "value": "todo",
        }))
        .await?;

    // Add task ID to the board index (OR-Set)
    proxy
        .eventual_write(serde_json::json!({
            "type": "set_add",
            "key": "board/task_index",
            "element": task_id.clone(),
        }))
        .await?;

    Ok(Json(CreateTaskResponse { task_id }))
}

/// List all tasks by reading the task index and assembling each task.
#[cfg(feature = "server")]
pub async fn list_tasks(
    State(proxy): State<AppState>,
) -> Result<Json<Vec<Task>>, AppError> {
    // Read the task index OR-Set
    let index_resp = proxy.eventual_read("board/task_index").await?;

    let task_ids: Vec<String> = index_resp
        .get("value")
        .and_then(|v| v.get("elements"))
        .and_then(|e| serde_json::from_value(e.clone()).ok())
        .unwrap_or_default();

    let mut tasks = Vec::new();
    for task_id in &task_ids {
        if let Ok(task) = assemble_task(&proxy, task_id).await {
            tasks.push(task);
        }
    }

    Ok(Json(tasks))
}

/// Delete a task by removing it from the index.
#[cfg(feature = "server")]
pub async fn delete_task(
    State(proxy): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    proxy
        .eventual_write(serde_json::json!({
            "type": "set_remove",
            "key": "board/task_index",
            "element": task_id,
        }))
        .await?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------
// Task mutations
// ---------------------------------------------------------------

/// Vote on a task (PN-Counter increment/decrement).
#[cfg(feature = "server")]
pub async fn vote_task(
    State(proxy): State<AppState>,
    Path(task_id): Path<String>,
    Json(req): Json<VoteRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let op = match req.direction {
        VoteDirection::Up => "counter_inc",
        VoteDirection::Down => "counter_dec",
    };

    proxy
        .eventual_write(serde_json::json!({
            "type": op,
            "key": format!("task/{task_id}/votes"),
        }))
        .await?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Update tags on a task (OR-Set add/remove).
#[cfg(feature = "server")]
pub async fn update_tags(
    State(proxy): State<AppState>,
    Path(task_id): Path<String>,
    Json(req): Json<TagUpdateRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let (op, body) = match req.action {
        TagAction::Add => (
            "set_add",
            serde_json::json!({
                "type": "set_add",
                "key": format!("task/{task_id}/tags"),
                "element": req.tag,
            }),
        ),
        TagAction::Remove => (
            "set_remove",
            serde_json::json!({
                "type": "set_remove",
                "key": format!("task/{task_id}/tags"),
                "element": req.tag,
            }),
        ),
    };
    let _ = op; // used in the json body via string literal

    proxy.eventual_write(body).await?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Update task metadata (OR-Map set for title/description).
#[cfg(feature = "server")]
pub async fn update_metadata(
    State(proxy): State<AppState>,
    Path(task_id): Path<String>,
    Json(req): Json<MetadataUpdateRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    if let Some(title) = req.title {
        proxy
            .eventual_write(serde_json::json!({
                "type": "map_set",
                "key": format!("task/{task_id}/meta"),
                "map_key": "title",
                "map_value": title,
            }))
            .await?;
    }

    if let Some(description) = req.description {
        proxy
            .eventual_write(serde_json::json!({
                "type": "map_set",
                "key": format!("task/{task_id}/meta"),
                "map_key": "description",
                "map_value": description,
            }))
            .await?;
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Update task status. Uses certified write when moving to "done".
#[cfg(feature = "server")]
pub async fn update_status(
    State(proxy): State<AppState>,
    Path(task_id): Path<String>,
    Json(req): Json<StatusUpdateRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let key = format!("task/{task_id}/status");
    let status_str = req.status.to_string();

    if req.status == TaskStatus::Done {
        // Certified write for completing a task
        let resp = proxy
            .certified_write(serde_json::json!({
                "key": key,
                "value": {
                    "type": "register",
                    "value": status_str,
                },
            }))
            .await?;
        Ok(Json(resp))
    } else {
        // Eventual write for other status changes
        proxy
            .eventual_write(serde_json::json!({
                "type": "register_set",
                "key": key,
                "value": status_str,
            }))
            .await?;
        Ok(Json(serde_json::json!({ "ok": true })))
    }
}

// ---------------------------------------------------------------
// Certification
// ---------------------------------------------------------------

/// Get the certification status for a task's status key.
#[cfg(feature = "server")]
pub async fn get_cert_status(
    State(proxy): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let key = format!("task/{task_id}/status");
    let resp = proxy.get_status(&key).await?;
    Ok(Json(resp))
}

/// Verify the proof bundle for a task's certified status.
#[cfg(feature = "server")]
pub async fn verify_task_proof(
    State(proxy): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let key = format!("task/{task_id}/status");

    // First, get the certified read (which includes the proof bundle)
    let cert_resp = proxy.certified_read(&key).await?;

    // Extract the proof fields and forward them to the verify endpoint
    let proof = cert_resp.get("proof");
    if let Some(proof) = proof {
        let verify_body = serde_json::json!({
            "key_range_prefix": proof.get("key_range_prefix").and_then(|v| v.as_str()).unwrap_or(""),
            "frontier": proof.get("frontier"),
            "policy_version": proof.get("policy_version").and_then(|v| v.as_u64()).unwrap_or(0),
            "contributing_authorities": proof.get("contributing_authorities"),
            "total_authorities": proof.get("total_authorities").and_then(|v| v.as_u64()).unwrap_or(0),
            "certificate": proof.get("certificate"),
        });

        let result = proxy.verify_proof(verify_body).await?;
        Ok(Json(result))
    } else {
        Ok(Json(serde_json::json!({
            "valid": false,
            "has_majority": false,
            "contributing_count": 0,
            "required_count": 0,
            "error": "no proof bundle available",
        })))
    }
}

// ---------------------------------------------------------------
// Cluster passthrough
// ---------------------------------------------------------------

#[cfg(feature = "server")]
pub async fn get_metrics(
    State(proxy): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    let resp = proxy.get_metrics().await?;
    Ok(Json(resp))
}

#[cfg(feature = "server")]
pub async fn get_slo(
    State(proxy): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    let resp = proxy.get_slo().await?;
    Ok(Json(resp))
}

#[cfg(feature = "server")]
pub async fn get_topology(
    State(proxy): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    let resp = proxy.get_topology().await?;
    Ok(Json(resp))
}

#[cfg(feature = "server")]
pub async fn get_health(
    State(proxy): State<AppState>,
) -> Json<Vec<NodeHealth>> {
    let results = proxy.health_check_all().await;
    Json(results)
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

/// Assemble a full Task from its constituent CRDT keys.
#[cfg(feature = "server")]
async fn assemble_task(proxy: &AsteroidProxy, task_id: &str) -> Result<Task, AppError> {
    // Read metadata (OR-Map)
    let meta_resp = proxy
        .eventual_read(&format!("task/{task_id}/meta"))
        .await?;
    let entries = meta_resp
        .get("value")
        .and_then(|v| v.get("entries"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let title = entries
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let description = entries
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Read status (LWW-Register)
    let status_resp = proxy
        .eventual_read(&format!("task/{task_id}/status"))
        .await?;
    let status_str = status_resp
        .get("value")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
        .unwrap_or("todo");
    let status: TaskStatus = status_str.parse().unwrap_or(TaskStatus::Todo);

    // Read votes (PN-Counter)
    let votes_resp = proxy
        .eventual_read(&format!("task/{task_id}/votes"))
        .await?;
    let votes = votes_resp
        .get("value")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // Read tags (OR-Set)
    let tags_resp = proxy
        .eventual_read(&format!("task/{task_id}/tags"))
        .await?;
    let tags: Vec<String> = tags_resp
        .get("value")
        .and_then(|v| v.get("elements"))
        .and_then(|e| serde_json::from_value(e.clone()).ok())
        .unwrap_or_default();

    // If status is "done", fetch certification info
    let certification = if status == TaskStatus::Done {
        let cert_resp = proxy
            .get_status(&format!("task/{task_id}/status"))
            .await
            .ok();

        cert_resp.map(|resp| {
            let cert_status = resp
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("Pending")
                .to_string();

            // Try to get proof from certified read
            let proof = None; // Proof is fetched on demand via the proof viewer

            CertInfo {
                status: cert_status,
                proof,
            }
        })
    } else {
        None
    };

    Ok(Task {
        id: task_id.to_string(),
        title,
        description,
        status,
        votes,
        tags,
        certification,
    })
}
