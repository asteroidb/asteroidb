//! AsteroidDB CLI tool for operational management.
//!
//! Provides commands for status checks, key-value operations, metrics
//! inspection, and SLO budget monitoring.

use clap::{Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};

use asteroidb_poc::authority::creation_policy::CreationPolicyManifest;
use asteroidb_poc::authority::writer_auth::{SignedBoundOperation, SignedCreation};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::network::operation_transport::OperationPacket;
use asteroidb_poc::operation::{
    BoundOperation, CandidateKvAction, CertificationMode, CrdtSubject, Declaration, EventDot,
    OperationLeaf,
};
use asteroidb_poc::types::NodeId;
use ed25519_dalek::SigningKey;

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
        /// Session token from a previous `put` or `get` (read-your-writes /
        /// monotonic reads). The read fails with HTTP 412 when this replica
        /// has not caught up to the token yet.
        #[arg(long)]
        session_token: Option<String>,
        /// Start a session without a precondition: ask the server to return
        /// a session token with the response (equivalent to an empty
        /// --session-token).
        #[arg(long)]
        session: bool,
        /// Maximum time (ms, server-capped at 5000) to wait for the replica
        /// to catch up before the server answers 412.
        #[arg(long)]
        wait_ms: Option<u64>,
    },
    /// Put a value into the eventual store (register type).
    Put {
        /// The key to write.
        key: String,
        /// The value to write.
        value: String,
    },
    /// Submit a signed operation packet and return its local storage receipt.
    OperationSubmit { packet: PathBuf },
    /// Sign a new key creation and write an OperationPacket JSON file.
    OperationSignCreate {
        namespace_id: String,
        writer: String,
        counter: u64,
        key: String,
        #[arg(long)]
        value_file: PathBuf,
        #[arg(long)]
        key_file: PathBuf,
        /// The same creation policy manifest pinned by the target nodes.
        #[arg(long)]
        creation_policy_file: PathBuf,
        #[arg(long, value_enum)]
        mode: OperationModeArg,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        timestamp_ms: Option<u64>,
        #[arg(long, default_value_t = 0)]
        logical: u32,
    },
    /// Sign a value update using the immutable declaration in a Create packet.
    OperationSignAdd {
        creation_packet: PathBuf,
        writer: String,
        counter: u64,
        #[arg(long)]
        value_file: PathBuf,
        #[arg(long)]
        key_file: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        timestamp_ms: Option<u64>,
        #[arg(long, default_value_t = 0)]
        logical: u32,
    },
    /// Sign a permanent key deletion using a Create packet.
    OperationSignDelete {
        creation_packet: PathBuf,
        writer: String,
        counter: u64,
        #[arg(long)]
        key_file: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Submit a signed packet and wait for its operation certificate.
    OperationSubmitCertified {
        packet: PathBuf,
        /// Maximum certificate wait in milliseconds (server maximum: 60000).
        #[arg(long)]
        wait_ms: Option<u64>,
    },
    /// Read the local projection of all held operations for a namespace/key.
    OperationRead { namespace_id: String, key: String },
    /// Read the local projection of certified operations with its proof basis.
    OperationCertifiedRead { namespace_id: String, key: String },
    /// Read the local or certified operation view through the unified endpoint.
    OperationView {
        namespace_id: String,
        key: String,
        #[arg(long, value_enum)]
        kind: OperationViewKind,
    },
    /// Inspect one operation ID (namespace, writer, counter).
    OperationStatus {
        namespace_id: String,
        writer: String,
        counter: u64,
    },
    /// Show detailed runtime metrics.
    Metrics,
    /// Show SLO budget status.
    Slo,
}

#[derive(Clone, Copy, ValueEnum)]
enum OperationViewKind {
    Local,
    Certified,
}

#[derive(Clone, Copy, ValueEnum)]
enum OperationModeArg {
    Enabled,
    Disabled,
}

impl From<OperationModeArg> for CertificationMode {
    fn from(value: OperationModeArg) -> Self {
        match value {
            OperationModeArg::Enabled => Self::Enabled,
            OperationModeArg::Disabled => Self::Disabled,
        }
    }
}

impl OperationViewKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Certified => "certified",
        }
    }
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
        Commands::Get {
            key,
            session_token,
            session,
            wait_ms,
        } => cmd_get(&client, &base, &key, session_token, session, wait_ms),
        Commands::Put { key, value } => cmd_put(&client, &base, &key, &value),
        Commands::OperationSubmit { packet } => {
            cmd_operation_submit(&client, &base, &packet, false, None)
        }
        Commands::OperationSignCreate {
            namespace_id,
            writer,
            counter,
            key,
            value_file,
            key_file,
            creation_policy_file,
            mode,
            output,
            timestamp_ms,
            logical,
        } => {
            let result = sign_create_packet(
                &namespace_id,
                &writer,
                counter,
                &key,
                &value_file,
                &key_file,
                &creation_policy_file,
                mode.into(),
                timestamp_ms,
                logical,
            )
            .and_then(|packet| write_signed_packet(&output, &packet));
            finish_signing(result);
        }
        Commands::OperationSignAdd {
            creation_packet,
            writer,
            counter,
            value_file,
            key_file,
            output,
            timestamp_ms,
            logical,
        } => {
            let result = sign_followup_packet(
                &creation_packet,
                &writer,
                counter,
                Some(&value_file),
                &key_file,
                timestamp_ms,
                logical,
            )
            .and_then(|packet| write_signed_packet(&output, &packet));
            finish_signing(result);
        }
        Commands::OperationSignDelete {
            creation_packet,
            writer,
            counter,
            key_file,
            output,
        } => {
            let result =
                sign_followup_packet(&creation_packet, &writer, counter, None, &key_file, None, 0)
                    .and_then(|packet| write_signed_packet(&output, &packet));
            finish_signing(result);
        }
        Commands::OperationSubmitCertified { packet, wait_ms } => {
            cmd_operation_submit(&client, &base, &packet, true, wait_ms)
        }
        Commands::OperationRead { namespace_id, key } => {
            cmd_operation_get(&client, &base, &["read", &namespace_id], Some(&key))
        }
        Commands::OperationCertifiedRead { namespace_id, key } => cmd_operation_get(
            &client,
            &base,
            &["certified-read", &namespace_id],
            Some(&key),
        ),
        Commands::OperationView {
            namespace_id,
            key,
            kind,
        } => {
            let url = operation_view_url(&base, &namespace_id, &key, kind).unwrap_or_else(|e| {
                eprintln!("Error: {e}");
                std::process::exit(1);
            });
            print_operation_response(client.get(url).send());
        }
        Commands::OperationStatus {
            namespace_id,
            writer,
            counter,
        } => cmd_operation_get(
            &client,
            &base,
            &["status", &namespace_id, &writer, &counter.to_string()],
            None,
        ),
        Commands::Metrics => cmd_metrics(&client, &base),
        Commands::Slo => cmd_slo(&client, &base),
    }
}

/// Build a public operation URL, encoding identity segments while retaining
/// the key's slash-separated path for the server's wildcard route.
fn operation_url(base: &str, segments: &[&str], key: Option<&str>) -> Result<reqwest::Url, String> {
    if segments
        .iter()
        .any(|part| part.is_empty() || *part == "." || *part == ".." || part.contains('/'))
    {
        return Err("operation path identity must be a nonempty single segment".into());
    }
    if key
        .is_some_and(|key| key.is_empty() || key.split('/').any(|part| part == "." || part == ".."))
    {
        return Err("operation key must be nonempty and contain no dot path segments".into());
    }
    let mut url = reqwest::Url::parse(base).map_err(|e| e.to_string())?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err("host URL must not contain a query or fragment".into());
    }
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| "host URL cannot be a base".to_string())?;
        path.pop_if_empty();
        path.extend(["api", "operations"]);
        path.extend(segments.iter().copied());
        if let Some(key) = key {
            path.extend(key.split('/'));
        }
    }
    Ok(url)
}

fn operation_view_url(
    base: &str,
    namespace_id: &str,
    key: &str,
    kind: OperationViewKind,
) -> Result<reqwest::Url, String> {
    let mut url = operation_url(base, &["view", namespace_id], Some(key))?;
    url.query_pairs_mut().append_pair("kind", kind.as_str());
    Ok(url)
}

fn signed_packet_from_file(path: &Path) -> Result<OperationPacket, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| format!("{}: invalid OperationPacket: {e}", path.display()))
}

fn signing_key_from_file(path: &Path) -> Result<SigningKey, String> {
    let raw =
        std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let bytes = hex::decode(raw.trim()).map_err(|_| {
        format!(
            "{}: expected a 32-byte Ed25519 secret key in hex",
            path.display()
        )
    })?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
        format!(
            "{}: expected a 32-byte Ed25519 secret key in hex",
            path.display()
        )
    })?;
    Ok(SigningKey::from_bytes(&bytes))
}

// The CLI command passes these named fields directly to the signing helper.
#[allow(clippy::too_many_arguments)]
fn sign_create_packet(
    namespace_id: &str,
    writer: &str,
    counter: u64,
    key: &str,
    value_file: &Path,
    key_file: &Path,
    creation_policy_file: &Path,
    mode: CertificationMode,
    timestamp_ms: Option<u64>,
    logical: u32,
) -> Result<OperationPacket, String> {
    if namespace_id.is_empty() || writer.is_empty() || key.is_empty() {
        return Err("namespace, writer, and key must be nonempty".into());
    }
    let signing_key = signing_key_from_file(key_file)?;
    let value_bytes =
        std::fs::read(value_file).map_err(|error| format!("{}: {error}", value_file.display()))?;
    let subject = CrdtSubject {
        namespace_id: namespace_id.into(),
        event_dot: EventDot {
            writer: NodeId(writer.into()),
            counter,
        },
    };
    let bound = BoundOperation {
        operation: OperationLeaf {
            subject: subject.clone(),
            action: CandidateKvAction::Create {
                key: key.into(),
                value_bytes,
                timestamp: HlcTimestamp {
                    physical: timestamp_ms.unwrap_or_else(asteroidb_poc::hlc::wall_clock_ms),
                    logical,
                    node_id: writer.into(),
                },
            },
        },
        declaration: Declaration {
            creation_id: subject,
            key: key.into(),
            mode,
        },
    };
    let policy_json = std::fs::read_to_string(creation_policy_file)
        .map_err(|error| format!("{}: {error}", creation_policy_file.display()))?;
    let policy = CreationPolicyManifest::from_json(&policy_json)
        .map_err(|error| format!("{}: {error}", creation_policy_file.display()))?;
    policy
        .validate_declaration(&bound.declaration)
        .map_err(|error| error.to_string())?;
    let creation = SignedCreation::sign_with_policy_digest(&bound, &signing_key, policy.digest())
        .map_err(|error| error.to_string())?;
    let signed = SignedBoundOperation::sign(bound, &signing_key, creation)
        .map_err(|error| error.to_string())?;
    Ok(OperationPacket { signed })
}

fn sign_followup_packet(
    creation_packet: &Path,
    writer: &str,
    counter: u64,
    value_file: Option<&Path>,
    key_file: &Path,
    timestamp_ms: Option<u64>,
    logical: u32,
) -> Result<OperationPacket, String> {
    if writer.is_empty() {
        return Err("writer must be nonempty".into());
    }
    let created = signed_packet_from_file(creation_packet)?;
    let creation = created.signed.creation;
    if !creation.matches_create(&created.signed.bound)
        || !matches!(
            created.signed.bound.operation.action,
            CandidateKvAction::Create { .. }
        )
    {
        return Err("creation packet must contain its exact signed Create".into());
    }
    let declaration = creation.declaration().clone();
    if declaration.creation_id.event_dot.writer.0 == writer
        && declaration.creation_id.event_dot.counter == counter
    {
        return Err("follow-up operation ID must differ from the Create ID".into());
    }
    let action = match value_file {
        Some(path) => CandidateKvAction::Add {
            key: declaration.key.clone(),
            value_bytes: std::fs::read(path)
                .map_err(|error| format!("{}: {error}", path.display()))?,
            timestamp: HlcTimestamp {
                physical: timestamp_ms.unwrap_or_else(asteroidb_poc::hlc::wall_clock_ms),
                logical,
                node_id: writer.into(),
            },
        },
        None => CandidateKvAction::Delete {
            key: declaration.key.clone(),
        },
    };
    let bound = BoundOperation {
        operation: OperationLeaf {
            subject: CrdtSubject {
                namespace_id: declaration.creation_id.namespace_id.clone(),
                event_dot: EventDot {
                    writer: NodeId(writer.into()),
                    counter,
                },
            },
            action,
        },
        declaration,
    };
    let signed = SignedBoundOperation::sign(bound, &signing_key_from_file(key_file)?, creation)
        .map_err(|error| error.to_string())?;
    Ok(OperationPacket { signed })
}

fn write_signed_packet(path: &Path, packet: &OperationPacket) -> Result<(), String> {
    let encoded = serde_json::to_vec_pretty(packet).map_err(|error| error.to_string())?;
    std::fs::write(path, encoded).map_err(|error| format!("{}: {error}", path.display()))
}

fn finish_signing(result: Result<(), String>) {
    if let Err(error) = result {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn print_operation_response(response: Result<reqwest::blocking::Response, reqwest::Error>) {
    match response {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().unwrap_or_default();
            if !status.is_success() {
                eprintln!("Error: HTTP {status}: {body}");
                std::process::exit(1);
            }
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => println!("{}", serde_json::to_string_pretty(&json).unwrap()),
                Err(_) => {
                    eprintln!("Error: operation endpoint returned invalid JSON");
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_operation_get(
    client: &reqwest::blocking::Client,
    base: &str,
    segments: &[&str],
    key: Option<&str>,
) {
    let url = operation_url(base, segments, key).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        std::process::exit(1);
    });
    print_operation_response(client.get(url).send());
}

fn cmd_operation_submit(
    client: &reqwest::blocking::Client,
    base: &str,
    packet_path: &Path,
    certified: bool,
    wait_ms: Option<u64>,
) {
    let packet = signed_packet_from_file(packet_path).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        std::process::exit(1);
    });
    let endpoint = if certified {
        "submit-certified"
    } else {
        "submit"
    };
    let mut url = operation_url(base, &[endpoint], None).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        std::process::exit(1);
    });
    if let Some(ms) = wait_ms {
        url.query_pairs_mut()
            .append_pair("wait_ms", &ms.to_string());
    }
    print_operation_response(client.post(url).json(&packet).send());
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

fn cmd_get(
    client: &reqwest::blocking::Client,
    base: &str,
    key: &str,
    session_token: Option<String>,
    session: bool,
    wait_ms: Option<u64>,
) {
    let url = format!("{base}/api/eventual/{key}");
    // --session is shorthand for an empty session token: no precondition,
    // but the server issues an observed-position token with the response.
    let token = session_token.or_else(|| session.then(String::new));
    let mut query: Vec<(&str, String)> = Vec::new();
    if let Some(token) = token {
        query.push(("session_token", token));
    }
    if let Some(ms) = wait_ms {
        query.push(("wait_ms", ms.to_string()));
    }
    match client.get(&url).query(&query).send() {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().unwrap_or_default();
                eprintln!("Error: HTTP {status}: {text}");
                if status.as_u16() == 412 {
                    eprintln!(
                        "hint: replica not caught up; retry with the same token or add --wait-ms"
                    );
                }
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
            let body: serde_json::Value = resp.json().unwrap_or_default();
            println!("OK");
            // Print the session token on its own line so existing scripts
            // that grep for "OK" keep working.
            if let Some(token) = body.get("session_token").and_then(|v| v.as_str()) {
                println!("session_token: {token}");
            }
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
    use asteroidb_poc::authority::writer_auth::WriterRegistry;
    use std::collections::BTreeSet;

    #[test]
    fn signed_create_add_and_delete_verify_with_writer_registry() {
        let dir = tempfile::tempdir().unwrap();
        let creator_key = SigningKey::from_bytes(&[7; 32]);
        let updater_key = SigningKey::from_bytes(&[8; 32]);
        let creator_key_file = dir.path().join("creator.key");
        let updater_key_file = dir.path().join("updater.key");
        let first_value = dir.path().join("first.bin");
        let second_value = dir.path().join("second.bin");
        let create_file = dir.path().join("create.json");
        let policy_file = dir.path().join("creation-policy.json");
        std::fs::write(&creator_key_file, hex::encode(creator_key.to_bytes())).unwrap();
        std::fs::write(&updater_key_file, hex::encode(updater_key.to_bytes())).unwrap();
        std::fs::write(&first_value, [0, 255, 1]).unwrap();
        std::fs::write(&second_value, [2, 3]).unwrap();
        std::fs::write(
            &policy_file,
            r#"{"version":1,"ranges":[{"namespace_id":"orders","key_prefix":"","default_mode":"Enabled","override_writers":[]}]}"#,
        )
        .unwrap();

        let create = sign_create_packet(
            "orders",
            "creator",
            1,
            "items/1",
            &first_value,
            &creator_key_file,
            &policy_file,
            CertificationMode::Enabled,
            Some(123),
            2,
        )
        .unwrap();
        assert!(
            sign_create_packet(
                "orders",
                "creator",
                1,
                "items/1",
                &first_value,
                &creator_key_file,
                &policy_file,
                CertificationMode::Disabled,
                Some(123),
                2,
            )
            .is_err()
        );
        write_signed_packet(&create_file, &create).unwrap();
        let add = sign_followup_packet(
            &create_file,
            "updater",
            2,
            Some(&second_value),
            &updater_key_file,
            Some(124),
            3,
        )
        .unwrap();
        let delete =
            sign_followup_packet(&create_file, "updater", 3, None, &updater_key_file, None, 0)
                .unwrap();

        let mut registry = WriterRegistry::new();
        let grants = BTreeSet::from(["orders".to_string()]);
        registry
            .register(
                NodeId("creator".into()),
                creator_key.verifying_key(),
                grants.clone(),
            )
            .unwrap();
        registry
            .register(
                NodeId("updater".into()),
                updater_key.verifying_key(),
                grants,
            )
            .unwrap();
        for packet in [&create, &add, &delete] {
            packet.signed.verify(&registry).unwrap();
            assert_eq!(
                packet.signed.bound.declaration,
                create.signed.bound.declaration
            );
        }
        for packet in [&add, &delete] {
            let json = serde_json::to_value(packet).unwrap();
            assert_eq!(json["signed"]["creation"]["version"], 3);
            assert!(json["signed"]["creation"].get("bound").is_none());
            assert!(
                json["signed"]["creation"]
                    .get("create_commitment")
                    .is_some()
            );
            assert_eq!(
                packet.signed.creation.creation_policy_digest,
                create.signed.creation.creation_policy_digest
            );
        }
        assert_eq!(signed_packet_from_file(&create_file).unwrap(), create);
        assert!(
            matches!(add.signed.bound.operation.action, CandidateKvAction::Add { ref value_bytes, .. } if value_bytes == &[2, 3])
        );
        assert!(matches!(
            delete.signed.bound.operation.action,
            CandidateKvAction::Delete { .. }
        ));
        assert!(
            sign_followup_packet(&create_file, "creator", 1, None, &creator_key_file, None, 0)
                .is_err()
        );

        let bad_file = dir.path().join("bad.json");
        write_signed_packet(&bad_file, &add).unwrap();
        assert!(
            sign_followup_packet(&bad_file, "updater", 4, None, &updater_key_file, None, 0)
                .is_err()
        );
        let mut inconsistent = create.clone();
        inconsistent.signed.bound.declaration.key = "other".into();
        write_signed_packet(&bad_file, &inconsistent).unwrap();
        assert!(
            sign_followup_packet(&bad_file, "updater", 4, None, &updater_key_file, None, 0)
                .is_err()
        );
    }

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

    #[test]
    fn operation_commands_parse_separately_from_legacy_commands() {
        let create_args = [
            "asteroidb-cli",
            "operation-sign-create",
            "orders",
            "creator",
            "1",
            "items/1",
            "--value-file",
            "value.bin",
            "--key-file",
            "creator.key",
            "--mode",
            "enabled",
            "--output",
            "create.json",
        ];
        assert!(Cli::try_parse_from(create_args).is_err());
        let parsed = Cli::try_parse_from(
            create_args
                .into_iter()
                .chain(["--creation-policy-file", "policy.json"]),
        )
        .unwrap();
        assert!(matches!(
            parsed.command,
            Commands::OperationSignCreate { creation_policy_file, .. }
                if creation_policy_file == Path::new("policy.json")
        ));
        let parsed = Cli::try_parse_from([
            "asteroidb-cli",
            "operation-submit-certified",
            "packet.json",
            "--wait-ms",
            "1500",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Commands::OperationSubmitCertified {
                wait_ms: Some(1500),
                ..
            }
        ));
        let parsed = Cli::try_parse_from([
            "asteroidb-cli",
            "operation-status",
            "namespace",
            "writer",
            "42",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Commands::OperationStatus { counter: 42, .. }
        ));
        let parsed = Cli::try_parse_from([
            "asteroidb-cli",
            "operation-view",
            "namespace",
            "orders/item",
            "--kind",
            "certified",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Commands::OperationView {
                kind: OperationViewKind::Certified,
                ..
            }
        ));
        assert!(
            Cli::try_parse_from([
                "asteroidb-cli",
                "operation-view",
                "namespace",
                "orders/item",
            ])
            .is_err()
        );
        let parsed = Cli::try_parse_from(["asteroidb-cli", "put", "key", "value"]).unwrap();
        assert!(matches!(parsed.command, Commands::Put { .. }));
    }

    #[test]
    fn operation_urls_use_public_namespace_routes_and_encode_segments() {
        assert_eq!(
            operation_url(
                "http://localhost:3000",
                &["read", "space ns"],
                Some("a/b c")
            )
            .unwrap()
            .as_str(),
            "http://localhost:3000/api/operations/read/space%20ns/a/b%20c"
        );
        assert_eq!(
            operation_view_url(
                "http://localhost:3000",
                "space ns",
                "orders/item",
                OperationViewKind::Certified,
            )
            .unwrap()
            .as_str(),
            "http://localhost:3000/api/operations/view/space%20ns/orders/item?kind=certified"
        );
        assert_eq!(
            operation_url(
                "http://localhost:3000",
                &["certified-read", "ns"],
                Some("orders/item")
            )
            .unwrap()
            .path(),
            "/api/operations/certified-read/ns/orders/item"
        );
        assert_eq!(
            operation_url(
                "http://localhost:3000",
                &["status", "ns", "writer", "7"],
                None
            )
            .unwrap()
            .path(),
            "/api/operations/status/ns/writer/7"
        );
        assert!(operation_url("http://localhost:3000", &["read", "ns"], Some("")).is_err());
        assert!(operation_url("http://localhost:3000", &["read", "ns"], Some("a/../b")).is_err());
        assert!(operation_url("http://localhost:3000", &["status", "../writer"], None).is_err());
    }

    #[test]
    fn signed_packet_file_is_sent_as_operation_packet_json() {
        use asteroidb_poc::authority::writer_auth::{SignedBoundOperation, SignedCreation};
        use asteroidb_poc::hlc::HlcTimestamp;
        use asteroidb_poc::operation::{
            BoundOperation, CandidateKvAction, CertificationMode, CrdtSubject, Declaration,
            EventDot, OperationLeaf,
        };
        use asteroidb_poc::types::NodeId;
        use ed25519_dalek::SigningKey;

        let writer = SigningKey::from_bytes(&[7; 32]);
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        let bound = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "orders/item".into(),
                    value_bytes: b"hello".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: "orders/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let creation = SignedCreation::sign_with_policy_digest(&bound, &writer, [0; 32]).unwrap();
        let packet = OperationPacket {
            signed: SignedBoundOperation::sign(bound, &writer, creation).unwrap(),
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("packet.json");
        std::fs::write(&path, serde_json::to_vec(&packet).unwrap()).unwrap();
        let loaded = signed_packet_from_file(&path).unwrap();
        assert_eq!(loaded, packet);

        let client = reqwest::blocking::Client::new();
        let mut url = operation_url("http://localhost:3000", &["submit-certified"], None).unwrap();
        url.query_pairs_mut().append_pair("wait_ms", "1500");
        let request = client.post(url).json(&loaded).build().unwrap();
        assert_eq!(request.url().path(), "/api/operations/submit-certified");
        assert_eq!(request.url().query(), Some("wait_ms=1500"));
        assert_eq!(
            serde_json::from_slice::<OperationPacket>(request.body().unwrap().as_bytes().unwrap())
                .unwrap(),
            packet
        );
        assert_eq!(
            operation_url("http://localhost:3000", &["submit"], None)
                .unwrap()
                .path(),
            "/api/operations/submit"
        );
        std::fs::write(&path, b"{}").unwrap();
        assert!(signed_packet_from_file(&path).is_err());
    }
}
