#[cfg(feature = "server")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "server")]
use serde_json::Value;

#[cfg(feature = "server")]
use crate::error::AppError;

/// Proxy to AsteroidDB nodes with round-robin load balancing.
#[cfg(feature = "server")]
pub struct AsteroidProxy {
    client: reqwest::Client,
    nodes: Vec<String>,
    index: AtomicUsize,
}

#[cfg(feature = "server")]
impl AsteroidProxy {
    pub fn new(nodes: Vec<String>) -> Self {
        assert!(!nodes.is_empty(), "at least one AsteroidDB node required");
        Self {
            client: reqwest::Client::new(),
            nodes,
            index: AtomicUsize::new(0),
        }
    }

    /// Get the next node URL via round-robin.
    fn next_node(&self) -> &str {
        let idx = self.index.fetch_add(1, Ordering::Relaxed) % self.nodes.len();
        &self.nodes[idx]
    }

    /// POST an eventual write to AsteroidDB.
    pub async fn eventual_write(&self, body: Value) -> Result<Value, AppError> {
        let url = format!("{}/api/eventual/write", self.next_node());
        let resp = self.client.post(&url).json(&body).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// GET an eventual read from AsteroidDB.
    pub async fn eventual_read(&self, key: &str) -> Result<Value, AppError> {
        let url = format!(
            "{}/api/eventual/{}",
            self.next_node(),
            urlencoded(key)
        );
        let resp = self.client.get(&url).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// POST a certified write to AsteroidDB.
    pub async fn certified_write(&self, body: Value) -> Result<Value, AppError> {
        let url = format!("{}/api/certified/write", self.next_node());
        let resp = self.client.post(&url).json(&body).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// GET a certified read from AsteroidDB.
    pub async fn certified_read(&self, key: &str) -> Result<Value, AppError> {
        let url = format!(
            "{}/api/certified/{}",
            self.next_node(),
            urlencoded(key)
        );
        let resp = self.client.get(&url).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// GET certification status for a key.
    pub async fn get_status(&self, key: &str) -> Result<Value, AppError> {
        let url = format!(
            "{}/api/status/{}",
            self.next_node(),
            urlencoded(key)
        );
        let resp = self.client.get(&url).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// POST to verify a proof bundle.
    pub async fn verify_proof(&self, body: Value) -> Result<Value, AppError> {
        let url = format!("{}/api/certified/verify", self.next_node());
        let resp = self.client.post(&url).json(&body).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// GET metrics from AsteroidDB.
    pub async fn get_metrics(&self) -> Result<Value, AppError> {
        let url = format!("{}/api/metrics", self.next_node());
        let resp = self.client.get(&url).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// GET SLO data from AsteroidDB.
    pub async fn get_slo(&self) -> Result<Value, AppError> {
        let url = format!("{}/api/slo", self.next_node());
        let resp = self.client.get(&url).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// GET topology from AsteroidDB.
    pub async fn get_topology(&self) -> Result<Value, AppError> {
        let url = format!("{}/api/topology", self.next_node());
        let resp = self.client.get(&url).send().await?;
        let json = resp.json().await?;
        Ok(json)
    }

    /// Health check all configured nodes.
    pub async fn health_check_all(&self) -> Vec<crate::shared::types::NodeHealth> {
        let mut results = Vec::new();
        for node in &self.nodes {
            let url = format!("{node}/healthz");
            let healthy = self.client.get(&url).send().await.is_ok();
            results.push(crate::shared::types::NodeHealth {
                address: node.clone(),
                healthy,
            });
        }
        results
    }
}

/// Simple percent-encoding for URL path segments.
#[cfg(feature = "server")]
fn urlencoded(s: &str) -> String {
    s.replace('%', "%25")
        .replace('/', "%2F")
        .replace(' ', "%20")
        .replace('#', "%23")
        .replace('?', "%3F")
}
