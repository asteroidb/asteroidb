//! AsteroidDB CLI tool for operational management.
//!
//! Provides commands for status checks, key-value operations, metrics
//! inspection, and SLO budget monitoring.

use clap::{Parser, Subcommand};

/// AsteroidDB command-line interface.
#[derive(Parser)]
#[command(name = "asteroidb-cli", about = "AsteroidDB operational CLI")]
struct Cli {
    /// Host address of the AsteroidDB node (host:port).
    #[arg(long, env = "ASTEROIDB_HOST", default_value = "127.0.0.1:3000")]
    host: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show node status (metrics summary).
    Status,
    /// Get a value by key from the eventual store.
    Get {
        /// The key to retrieve.
        key: String,
    },
    /// Put a value into the eventual store (register type).
    Put {
        /// The key to write.
        key: String,
        /// The value to write.
        value: String,
    },
    /// Show detailed runtime metrics.
    Metrics,
    /// Show SLO budget status.
    Slo,
}

fn base_url(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    }
}

fn main() {
    let cli = Cli::parse();
    let base = base_url(&cli.host);
    let client = reqwest::blocking::Client::new();

    match cli.command {
        Commands::Status => cmd_status(&client, &base),
        Commands::Get { key } => cmd_get(&client, &base, &key),
        Commands::Put { key, value } => cmd_put(&client, &base, &key, &value),
        Commands::Metrics => cmd_metrics(&client, &base),
        Commands::Slo => cmd_slo(&client, &base),
    }
}

fn cmd_status(client: &reqwest::blocking::Client, base: &str) {
    let url = format!("{base}/api/metrics");
    match client.get(&url).send() {
        Ok(resp) => {
            if !resp.status().is_success() {
                eprintln!("Error: HTTP {}", resp.status());
                std::process::exit(1);
            }
            let body: serde_json::Value = resp.json().unwrap_or_default();
            println!("=== AsteroidDB Node Status ===");
            println!(
                "{:<35} {}",
                "Pending certifications:",
                body.get("pending_count")
                    .unwrap_or(&serde_json::Value::Null)
            );
            println!(
                "{:<35} {}",
                "Certified total:",
                body.get("certified_total")
                    .unwrap_or(&serde_json::Value::Null)
            );
            println!(
                "{:<35} {:.2} us",
                "Cert latency mean:",
                body.get("certification_latency_mean_us")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0)
            );
            println!(
                "{:<35} {} ms",
                "Frontier skew:",
                body.get("frontier_skew_ms")
                    .unwrap_or(&serde_json::Value::Null)
            );
            println!(
                "{:<35} {:.4}",
                "Sync failure rate:",
                body.get("sync_failure_rate")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0)
            );
            println!(
                "{:<35} {}",
                "Sync attempts:",
                body.get("sync_attempt_total")
                    .unwrap_or(&serde_json::Value::Null)
            );
        }
        Err(e) => {
            eprintln!("Error connecting to {url}: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_get(client: &reqwest::blocking::Client, base: &str, key: &str) {
    let url = format!("{base}/api/eventual/{key}");
    match client.get(&url).send() {
        Ok(resp) => {
            if !resp.status().is_success() {
                eprintln!("Error: HTTP {}", resp.status());
                std::process::exit(1);
            }
            let body: serde_json::Value = resp.json().unwrap_or_default();
            println!(
                "{}",
                serde_json::to_string_pretty(&body).unwrap_or_default()
            );
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_put(client: &reqwest::blocking::Client, base: &str, key: &str, value: &str) {
    let url = format!("{base}/api/eventual/write");
    let body = put_body(key, value);

    match client.post(&url).json(&body).send() {
        Ok(resp) => {
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().unwrap_or_default();
                eprintln!("Error: HTTP {status}: {text}");
                std::process::exit(1);
            }
            println!("OK");
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_metrics(client: &reqwest::blocking::Client, base: &str) {
    let url = format!("{base}/api/metrics");
    match client.get(&url).send() {
        Ok(resp) => {
            if !resp.status().is_success() {
                eprintln!("Error: HTTP {}", resp.status());
                std::process::exit(1);
            }
            let body: serde_json::Value = resp.json().unwrap_or_default();
            println!(
                "{}",
                serde_json::to_string_pretty(&body).unwrap_or_default()
            );
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

/// Build the JSON body for the `put` command.
///
/// Produces the internally-tagged format expected by `EventualWriteRequest`:
/// `{"type":"register_set","key":"...","value":"..."}`.
fn put_body(key: &str, value: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "register_set",
        "key": key,
        "value": value,
    })
}

fn cmd_slo(client: &reqwest::blocking::Client, base: &str) {
    let url = format!("{base}/api/slo");
    match client.get(&url).send() {
        Ok(resp) => {
            if !resp.status().is_success() {
                eprintln!("Error: HTTP {}", resp.status());
                std::process::exit(1);
            }
            let body: serde_json::Value = resp.json().unwrap_or_default();

            println!("=== SLO Budget Status ===\n");

            if let Some(budgets) = body.get("budgets").and_then(|b| b.as_object()) {
                // Sort keys for deterministic output.
                let mut keys: Vec<&String> = budgets.keys().collect();
                keys.sort();

                println!(
                    "{:<35} {:>10} {:>10} {:>12} {:>8}",
                    "SLO", "Total", "Violations", "Remaining%", "Status"
                );
                println!("{}", "-".repeat(80));

                for key in keys {
                    let budget = &budgets[key];
                    let total = budget
                        .get("total_requests")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let violations = budget
                        .get("violations")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let remaining = budget
                        .get("budget_remaining")
                        .and_then(|v| v.as_f64())
                        .unwrap_or_else(|| {
                            // Fallback: compute success rate if server omits budget_remaining.
                            if total == 0 {
                                100.0
                            } else {
                                (1.0 - violations as f64 / total as f64) * 100.0
                            }
                        });
                    let status = if remaining < 20.0 {
                        "CRITICAL"
                    } else if remaining < 50.0 {
                        "WARNING"
                    } else {
                        "OK"
                    };
                    println!(
                        "{:<35} {:>10} {:>10} {:>11.1}% {:>8}",
                        key, total, violations, remaining, status
                    );
                }
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&body).unwrap_or_default()
                );
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_body_produces_tagged_json() {
        let body = put_body("mykey", "myvalue");
        let obj = body.as_object().expect("should be a JSON object");
        assert_eq!(
            obj.get("type").and_then(|v| v.as_str()),
            Some("register_set")
        );
        assert_eq!(obj.get("key").and_then(|v| v.as_str()), Some("mykey"));
        assert_eq!(obj.get("value").and_then(|v| v.as_str()), Some("myvalue"));
        assert_eq!(obj.len(), 3, "should have exactly 3 fields");
    }

    #[test]
    fn put_body_roundtrips_through_eventual_write_request() {
        // Verify the JSON produced by put_body can be deserialized as EventualWriteRequest.
        let body = put_body("sensor-1", "42.5");
        let json_str = serde_json::to_string(&body).unwrap();
        let req: asteroidb_poc::http::types::EventualWriteRequest =
            serde_json::from_str(&json_str).unwrap();
        match req {
            asteroidb_poc::http::types::EventualWriteRequest::RegisterSet { key, value } => {
                assert_eq!(key, "sensor-1");
                assert_eq!(value, "42.5");
            }
            other => panic!("expected RegisterSet, got {other:?}"),
        }
    }

    #[test]
    fn base_url_adds_scheme() {
        assert_eq!(base_url("127.0.0.1:3000"), "http://127.0.0.1:3000");
        assert_eq!(base_url("http://localhost:3000"), "http://localhost:3000");
        assert_eq!(base_url("https://example.com"), "https://example.com");
    }
}
