use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------
// Task model
// ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub title: String,
    pub description: String,
    pub status: TaskStatus,
    pub votes: i64,
    pub tags: Vec<String>,
    pub certification: Option<CertInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Todo,
    Doing,
    Done,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Todo => write!(f, "todo"),
            TaskStatus::Doing => write!(f, "doing"),
            TaskStatus::Done => write!(f, "done"),
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "todo" => Ok(TaskStatus::Todo),
            "doing" => Ok(TaskStatus::Doing),
            "done" => Ok(TaskStatus::Done),
            other => Err(format!("unknown status: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertInfo {
    pub status: String,
    pub proof: Option<ProofBundle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofBundle {
    pub key_range_prefix: String,
    pub frontier: FrontierInfo,
    pub policy_version: u64,
    pub contributing_authorities: Vec<String>,
    pub total_authorities: usize,
    pub has_certificate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierInfo {
    pub physical: u64,
    pub logical: u32,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResult {
    pub valid: bool,
    pub has_majority: bool,
    pub contributing_count: usize,
    pub required_count: usize,
}

// ---------------------------------------------------------------
// BFF request types
// ---------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTaskRequest {
    pub title: String,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTaskResponse {
    pub task_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoteRequest {
    pub direction: VoteDirection,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum VoteDirection {
    Up,
    Down,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TagUpdateRequest {
    pub action: TagAction,
    pub tag: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TagAction {
    Add,
    Remove,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusUpdateRequest {
    pub status: TaskStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataUpdateRequest {
    pub title: Option<String>,
    pub description: Option<String>,
}

// ---------------------------------------------------------------
// Cluster types (passthrough from AsteroidDB)
// ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub address: String,
    pub healthy: bool,
}

/// Metrics snapshot passthrough (subset of AsteroidDB MetricsSnapshot).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsInfo {
    pub pending_count: u64,
    pub certified_total: u64,
    pub certification_latency_mean_us: f64,
    pub frontier_skew_ms: u64,
    pub sync_failure_rate: f64,
    pub write_ops_total: u64,
    pub peer_sync: HashMap<String, PeerSyncInfo>,
    pub certification_latency_window: CertLatencyWindowInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSyncInfo {
    pub mean_latency_us: f64,
    pub p99_latency_us: f64,
    pub success_count: u64,
    pub failure_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertLatencyWindowInfo {
    pub sample_count: u64,
    pub mean_us: f64,
    pub p99_us: f64,
}

/// SLO snapshot passthrough.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloInfo {
    pub budgets: HashMap<String, SloBudgetInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloBudgetInfo {
    pub target: SloTargetInfo,
    pub total_requests: u64,
    pub violations: u64,
    pub budget_remaining: f64,
    pub is_warning: bool,
    pub is_critical: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloTargetInfo {
    pub name: String,
    pub kind: String,
    pub target_value: f64,
    pub target_percentage: f64,
    pub window_secs: u64,
}

/// Topology snapshot passthrough.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyInfo {
    pub regions: Vec<RegionInfo>,
    pub total_nodes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionInfo {
    pub name: String,
    pub node_count: usize,
    pub node_ids: Vec<String>,
    pub inter_region_latency_ms: HashMap<String, f64>,
}
