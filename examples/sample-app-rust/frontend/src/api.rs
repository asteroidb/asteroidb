use gloo_net::http::Request;
use asteroidb_sample_app::shared::types::*;

const BASE: &str = "/bff/api";

// ---------------------------------------------------------------
// Task API
// ---------------------------------------------------------------

pub async fn fetch_tasks() -> Result<Vec<Task>, String> {
    let resp = Request::get(&format!("{BASE}/tasks"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

pub async fn create_task(title: &str, description: &str) -> Result<CreateTaskResponse, String> {
    let body = CreateTaskRequest {
        title: title.to_string(),
        description: description.to_string(),
    };
    let resp = Request::post(&format!("{BASE}/tasks"))
        .json(&body)
        .map_err(|e| e.to_string())?
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

pub async fn delete_task(task_id: &str) -> Result<(), String> {
    Request::delete(&format!("{BASE}/tasks/{task_id}"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn vote_task(task_id: &str, up: bool) -> Result<(), String> {
    let body = VoteRequest {
        direction: if up {
            VoteDirection::Up
        } else {
            VoteDirection::Down
        },
    };
    Request::post(&format!("{BASE}/tasks/{task_id}/vote"))
        .json(&body)
        .map_err(|e| e.to_string())?
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn update_tags(task_id: &str, action: TagAction, tag: &str) -> Result<(), String> {
    let body = TagUpdateRequest {
        action,
        tag: tag.to_string(),
    };
    Request::post(&format!("{BASE}/tasks/{task_id}/tags"))
        .json(&body)
        .map_err(|e| e.to_string())?
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn update_metadata(
    task_id: &str,
    title: Option<&str>,
    description: Option<&str>,
) -> Result<(), String> {
    let body = MetadataUpdateRequest {
        title: title.map(|s| s.to_string()),
        description: description.map(|s| s.to_string()),
    };
    Request::put(&format!("{BASE}/tasks/{task_id}/metadata"))
        .json(&body)
        .map_err(|e| e.to_string())?
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn update_status(task_id: &str, status: TaskStatus) -> Result<serde_json::Value, String> {
    let body = StatusUpdateRequest { status };
    let resp = Request::put(&format!("{BASE}/tasks/{task_id}/status"))
        .json(&body)
        .map_err(|e| e.to_string())?
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

pub async fn get_cert_status(task_id: &str) -> Result<serde_json::Value, String> {
    let resp = Request::get(&format!("{BASE}/tasks/{task_id}/cert"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

pub async fn verify_proof(task_id: &str) -> Result<VerifyResult, String> {
    let resp = Request::post(&format!("{BASE}/tasks/{task_id}/verify"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

// ---------------------------------------------------------------
// Cluster API
// ---------------------------------------------------------------

pub async fn fetch_metrics() -> Result<String, String> {
    let resp = Request::get(&format!("{BASE}/cluster/metrics"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.text().await.map_err(|e| e.to_string())
}

pub async fn fetch_slo() -> Result<String, String> {
    let resp = Request::get(&format!("{BASE}/cluster/slo"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.text().await.map_err(|e| e.to_string())
}

pub async fn fetch_topology() -> Result<String, String> {
    let resp = Request::get(&format!("{BASE}/cluster/topology"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.text().await.map_err(|e| e.to_string())
}

pub async fn fetch_health() -> Result<String, String> {
    let resp = Request::get(&format!("{BASE}/cluster/health"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.text().await.map_err(|e| e.to_string())
}
