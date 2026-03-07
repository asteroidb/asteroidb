/// Fishing Catch Management Demo — AsteroidDB PoC
///
/// Demonstrates CRDT convergence and Authority certification through
/// an interactive fishing boat scenario with a Web UI.
///
/// Run: `cargo run --example fishing_demo`
/// Then open: http://localhost:8080
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

const HTML: &str = include_str!("../demo/fishing/index.html");

const FISH_TYPES: &[&str] = &["maguro", "ika", "saba", "tai"];

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

struct DemoState {
    boat_a: EventualApi,
    boat_b: EventualApi,
    port_eventual: EventualApi,
    port_certified: CertifiedApi,
    partitioned: [bool; 2], // [boat_a, boat_b]
    current_step: usize,
    log: Vec<LogEntry>,
    certified_catches: HashMap<String, i64>,
    cert_status: String,
}

#[derive(Clone, Serialize)]
struct LogEntry {
    message: String,
    class: String,
}

type SharedState = Arc<Mutex<DemoState>>;

impl DemoState {
    fn new() -> Self {
        let node_a = NodeId("boat-a".into());
        let node_b = NodeId("boat-b".into());
        let node_port = NodeId("port".into());
        let auth_1 = NodeId("auth-1".into());
        let auth_2 = NodeId("auth-2".into());
        let auth_3 = NodeId("auth-3".into());

        let mut namespace = SystemNamespace::new();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: String::new(),
            },
            authority_nodes: vec![auth_1, auth_2, auth_3],
            auto_generated: false,
        });
        let ns = Arc::new(std::sync::RwLock::new(namespace));

        Self {
            boat_a: EventualApi::new(node_a),
            boat_b: EventualApi::new(node_b),
            port_eventual: EventualApi::new(node_port.clone()),
            port_certified: CertifiedApi::new(node_port, ns.clone()),
            partitioned: [false, false],
            current_step: 0,
            log: Vec::new(),
            certified_catches: HashMap::new(),
            cert_status: "none".into(),
        }
    }

    fn get_catches(&self, api: &EventualApi) -> HashMap<String, i64> {
        let mut catches = HashMap::new();
        for fish in FISH_TYPES {
            let key = format!("catch/{fish}");
            let val = match api.get_eventual(&key) {
                Some(CrdtValue::Counter(c)) => c.value(),
                _ => 0,
            };
            catches.insert(fish.to_string(), val);
        }
        catches
    }

    fn sync_all(&mut self) {
        // Collect values from all three nodes first (avoid borrow issues)
        let vals_a: Vec<(String, CrdtValue)> = FISH_TYPES
            .iter()
            .filter_map(|f| {
                let key = format!("catch/{f}");
                self.boat_a.get_eventual(&key).map(|v| (key, v.clone()))
            })
            .collect();
        let vals_b: Vec<(String, CrdtValue)> = FISH_TYPES
            .iter()
            .filter_map(|f| {
                let key = format!("catch/{f}");
                self.boat_b.get_eventual(&key).map(|v| (key, v.clone()))
            })
            .collect();
        let vals_p: Vec<(String, CrdtValue)> = FISH_TYPES
            .iter()
            .filter_map(|f| {
                let key = format!("catch/{f}");
                self.port_eventual
                    .get_eventual(&key)
                    .map(|v| (key, v.clone()))
            })
            .collect();

        // Merge into each node
        for (key, val) in &vals_b {
            let _ = self.boat_a.merge_remote(key.clone(), val);
        }
        for (key, val) in &vals_p {
            let _ = self.boat_a.merge_remote(key.clone(), val);
        }

        for (key, val) in &vals_a {
            let _ = self.boat_b.merge_remote(key.clone(), val);
        }
        for (key, val) in &vals_p {
            let _ = self.boat_b.merge_remote(key.clone(), val);
        }

        for (key, val) in &vals_a {
            let _ = self.port_eventual.merge_remote(key.clone(), val);
        }
        for (key, val) in &vals_b {
            let _ = self.port_eventual.merge_remote(key.clone(), val);
        }
    }

    /// Sync only connected nodes (port ↔ non-partitioned boats)
    fn sync_connected(&mut self) {
        let vals_a: Option<Vec<(String, CrdtValue)>> = if !self.partitioned[0] {
            Some(
                FISH_TYPES
                    .iter()
                    .filter_map(|f| {
                        let key = format!("catch/{f}");
                        self.boat_a.get_eventual(&key).map(|v| (key, v.clone()))
                    })
                    .collect(),
            )
        } else {
            None
        };
        let vals_b: Option<Vec<(String, CrdtValue)>> = if !self.partitioned[1] {
            Some(
                FISH_TYPES
                    .iter()
                    .filter_map(|f| {
                        let key = format!("catch/{f}");
                        self.boat_b.get_eventual(&key).map(|v| (key, v.clone()))
                    })
                    .collect(),
            )
        } else {
            None
        };
        let vals_p: Vec<(String, CrdtValue)> = FISH_TYPES
            .iter()
            .filter_map(|f| {
                let key = format!("catch/{f}");
                self.port_eventual
                    .get_eventual(&key)
                    .map(|v| (key, v.clone()))
            })
            .collect();

        // Merge port data into connected boats
        if let Some(ref va) = vals_a {
            for (key, val) in &vals_p {
                let _ = self.boat_a.merge_remote(key.clone(), val);
            }
            for (key, val) in va {
                let _ = self.port_eventual.merge_remote(key.clone(), val);
            }
        }
        if let Some(ref vb) = vals_b {
            for (key, val) in &vals_p {
                let _ = self.boat_b.merge_remote(key.clone(), val);
            }
            for (key, val) in vb {
                let _ = self.port_eventual.merge_remote(key.clone(), val);
            }
        }
        // Sync between connected boats
        if let (Some(va), Some(vb)) = (&vals_a, &vals_b) {
            for (key, val) in va {
                let _ = self.boat_b.merge_remote(key.clone(), val);
            }
            for (key, val) in vb {
                let _ = self.boat_a.merge_remote(key.clone(), val);
            }
        }
    }

    fn add_log(&mut self, message: &str, class: &str) {
        self.log.push(LogEntry {
            message: message.into(),
            class: class.into(),
        });
    }
}

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StateResponse {
    boat_a: BoatState,
    boat_b: BoatState,
    port: PortState,
}

#[derive(Serialize)]
struct BoatState {
    catches: HashMap<String, i64>,
    connected: bool,
    can_record: bool,
}

#[derive(Serialize)]
struct PortState {
    catches: HashMap<String, i64>,
    certified_catches: HashMap<String, i64>,
    certification_status: String,
}

#[derive(Deserialize)]
struct CatchRequest {
    boat: String,
    fish: String,
}

#[derive(Serialize)]
struct ScenarioResponse {
    ok: bool,
    log: Vec<LogEntry>,
    explanation: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn index_handler() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], HTML)
}

async fn state_handler(State(state): State<SharedState>) -> Json<StateResponse> {
    let s = state.lock().unwrap();
    Json(StateResponse {
        boat_a: BoatState {
            catches: s.get_catches(&s.boat_a),
            connected: !s.partitioned[0],
            can_record: s.current_step >= 1,
        },
        boat_b: BoatState {
            catches: s.get_catches(&s.boat_b),
            connected: !s.partitioned[1],
            can_record: s.current_step >= 1,
        },
        port: PortState {
            catches: s.get_catches(&s.port_eventual),
            certified_catches: s.certified_catches.clone(),
            certification_status: s.cert_status.clone(),
        },
    })
}

async fn catch_handler(
    State(state): State<SharedState>,
    Json(req): Json<CatchRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut s = state.lock().unwrap();
    let key = format!("catch/{}", req.fish);

    let result = match req.boat.as_str() {
        "a" => s.boat_a.eventual_counter_inc(&key),
        "b" => s.boat_b.eventual_counter_inc(&key),
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    match result {
        Ok(()) => {
            // Auto-sync if connected
            if !s.partitioned[0] && !s.partitioned[1] {
                s.sync_connected();
            } else if req.boat == "a" && !s.partitioned[0] {
                // sync boat_a with port
                let val = s.boat_a.get_eventual(&key).cloned();
                if let Some(v) = val {
                    let _ = s.port_eventual.merge_remote(key.clone(), &v);
                }
            } else if req.boat == "b" && !s.partitioned[1] {
                let val = s.boat_b.get_eventual(&key).cloned();
                if let Some(v) = val {
                    let _ = s.port_eventual.merge_remote(key.clone(), &v);
                }
            }
            Ok(Json(serde_json::json!({"ok": true})))
        }
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn scenario_handler(
    State(state): State<SharedState>,
    axum::extract::Path(step): axum::extract::Path<usize>,
) -> Json<ScenarioResponse> {
    let mut s = state.lock().unwrap();
    let mut logs = Vec::new();
    let explanation: String;

    match step {
        // Step 0: Reset
        0 => {
            *s = DemoState::new();
            logs.push(LogEntry {
                message: "システム初期化完了。3ノード構成: 第一丸, 第二丸, 港(漁協)".into(),
                class: "".into(),
            });
            explanation = "\
                <strong>初期状態:</strong> 漁船2隻と港の3ノードが接続されています。<br>\
                各ノードは独立したCRDTストアを持ち、接続中はリアルタイムに同期します。<br>\
                AsteroidDBの<strong>Eventual Consistency</strong>モードで動作中です。\
            "
            .into();
        }

        // Step 1: Pre-departure catches
        1 => {
            s.current_step = 1;
            // Simulate some initial catches at port
            for _ in 0..3 {
                let _ = s.boat_a.eventual_counter_inc("catch/maguro");
            }
            for _ in 0..2 {
                let _ = s.boat_b.eventual_counter_inc("catch/ika");
            }
            let _ = s.boat_a.eventual_counter_inc("catch/saba");

            // Sync all (everyone is connected)
            s.sync_all();

            logs.push(LogEntry {
                message: "第一丸: マグロ×3, サバ×1 を記録".into(),
                class: "".into(),
            });
            logs.push(LogEntry {
                message: "第二丸: イカ×2 を記録".into(),
                class: "".into(),
            });
            logs.push(LogEntry {
                message: "全ノード同期完了 (CRDT merge)".into(),
                class: "sync".into(),
            });
            s.add_log("出港前の漁獲を記録・同期完了", "sync");

            explanation = "\
                <strong>出港前:</strong> 港で各船が漁獲を記録しました。<br>\
                全ノードが接続中のため、<strong>CRDTマージ</strong>により即座にデータが収束しています。<br>\
                PN-Counter CRDTは各ノードの増分を独立に追跡するため、マージ後の値は全ノードで一致します。\
            "
            .into();
        }

        // Step 2: Go to sea (partition)
        2 => {
            s.current_step = 2;
            s.partitioned = [true, true];

            logs.push(LogEntry {
                message: "第一丸: 出航 → 通信圏外".into(),
                class: "partition".into(),
            });
            logs.push(LogEntry {
                message: "第二丸: 出航 → 通信圏外".into(),
                class: "partition".into(),
            });
            logs.push(LogEntry {
                message: "ネットワーク分断発生 (漁船 ↔ 港 間の通信が途絶)".into(),
                class: "partition".into(),
            });
            s.add_log("両船が出航、通信圏外に", "partition");

            explanation = "\
                <strong>出航 (ネットワーク分断):</strong> 両漁船が沖へ出て通信圏外になりました。<br>\
                これは分散システムにおける<strong>ネットワークパーティション</strong>と同じ状態です。<br>\
                AsteroidDBはCRDTにより、分断中も各ノードで<strong>書き込みを継続</strong>できます (AP特性)。<br>\
                各船の＋ボタンで漁獲を自由に記録してみてください。\
            "
            .into();
        }

        // Step 3: Fishing at sea (writes during partition)
        3 => {
            s.current_step = 3;
            // Simulate catches during partition
            for _ in 0..5 {
                let _ = s.boat_a.eventual_counter_inc("catch/maguro");
            }
            for _ in 0..3 {
                let _ = s.boat_a.eventual_counter_inc("catch/tai");
            }
            for _ in 0..4 {
                let _ = s.boat_b.eventual_counter_inc("catch/saba");
            }
            for _ in 0..6 {
                let _ = s.boat_b.eventual_counter_inc("catch/ika");
            }
            let _ = s.boat_b.eventual_counter_inc("catch/maguro");
            let _ = s.boat_b.eventual_counter_inc("catch/maguro");

            logs.push(LogEntry {
                message: "第一丸 (洋上): マグロ×5, タイ×3 を記録".into(),
                class: "".into(),
            });
            logs.push(LogEntry {
                message: "第二丸 (洋上): サバ×4, イカ×6, マグロ×2 を記録".into(),
                class: "".into(),
            });
            logs.push(LogEntry {
                message: "⚠ 港のデータは古いまま (分断中のためデータは不整合)".into(),
                class: "partition".into(),
            });
            s.add_log("洋上で操業中、各船が独立に記録", "");

            explanation = "\
                <strong>洋上で操業:</strong> 各船が独立に漁獲を記録しています。<br>\
                港のデータは出港前のまま ― これが<strong>分断中のデータ不整合</strong>です。<br>\
                従来のRDBMSでは書き込み不可になりますが、CRDTは<strong>各ノードでローカル書き込みを許容</strong>します。<br>\
                帰港時にマージすれば正しい合計値に収束します。\
            "
            .into();
        }

        // Step 4: Return to port (recovery + sync)
        4 => {
            s.current_step = 4;
            s.partitioned = [false, false];

            // Sync all nodes — CRDT merge convergence
            s.sync_all();

            logs.push(LogEntry {
                message: "第一丸: 帰港 → 通信回復".into(),
                class: "sync".into(),
            });
            logs.push(LogEntry {
                message: "第二丸: 帰港 → 通信回復".into(),
                class: "sync".into(),
            });
            logs.push(LogEntry {
                message: "CRDT anti-entropy sync 実行 → 全ノード収束完了".into(),
                class: "sync".into(),
            });

            // Verify convergence
            let catches_a = s.get_catches(&s.boat_a);
            let catches_p = s.get_catches(&s.port_eventual);
            let converged = catches_a == catches_p;
            logs.push(LogEntry {
                message: format!(
                    "収束検証: {}",
                    if converged {
                        "全ノードのデータが一致 ✓"
                    } else {
                        "不一致あり ✗"
                    }
                ),
                class: "sync".into(),
            });
            s.add_log("帰港・CRDT収束完了", "sync");

            explanation = "\
                <strong>帰港 (パーティション回復):</strong> 全ノードのデータがCRDTマージにより<strong>自動収束</strong>しました。<br>\
                PN-Counter CRDTの特性により:<br>\
                • 各船の増分は<strong>ノード単位で独立管理</strong>され、競合しません<br>\
                • マージは<strong>可換・結合・冪等</strong> — 順序やタイミングに依存しません<br>\
                • 最終的な値は全ノードで<strong>数学的に正しい合計値</strong>になります\
            "
            .into();
        }

        // Step 5: Certify catches
        5 => {
            s.current_step = 5;
            s.cert_status = "Pending".into();

            // Create certified writes for each fish type
            let port_id = NodeId("port".into());
            for fish in FISH_TYPES {
                let key = format!("catch/{fish}");
                let count = match s.port_eventual.get_eventual(&key) {
                    Some(CrdtValue::Counter(c)) => c.value(),
                    _ => 0,
                };
                let counter = PnCounter::from_value(&port_id, count);
                let _ = s.port_certified.certified_write(
                    key,
                    CrdtValue::Counter(counter),
                    OnTimeout::Pending,
                );
            }

            logs.push(LogEntry {
                message: "漁協: 水揚げ確定申請 (Certified write) を発行".into(),
                class: "certify".into(),
            });
            logs.push(LogEntry {
                message: "Authority ノードへ過半数合意を要求中...".into(),
                class: "certify".into(),
            });

            // Simulate authority ack frontiers (majority: 2 of 3)
            let pending = s.port_certified.pending_writes();
            let write_ts = if !pending.is_empty() {
                pending[0].timestamp.physical
            } else {
                1_000_000
            };

            // First authority ack — not yet majority
            s.port_certified.update_frontier(AckFrontier {
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
                digest_hash: format!("auth1-{write_ts}"),
            });
            s.port_certified.process_certifications();

            logs.push(LogEntry {
                message: "Authority auth-1: ACK (1/3 — 過半数未達)".into(),
                class: "certify".into(),
            });

            // Second authority ack — majority reached!
            s.port_certified.update_frontier(AckFrontier {
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
                digest_hash: format!("auth2-{write_ts}"),
            });
            s.port_certified.process_certifications();

            // Check certification status
            let status = s
                .port_certified
                .get_certification_status("catch/maguro");

            if status == CertificationStatus::Certified {
                s.cert_status = "Certified".into();
                // Record certified values
                for fish in FISH_TYPES {
                    let key = format!("catch/{fish}");
                    let count = match s.port_eventual.get_eventual(&key) {
                        Some(CrdtValue::Counter(c)) => c.value(),
                        _ => 0,
                    };
                    s.certified_catches.insert(fish.to_string(), count);
                }
            }

            logs.push(LogEntry {
                message: format!(
                    "Authority auth-2: ACK (2/3 — 過半数達成!) → ステータス: {status:?}"
                ),
                class: "certify".into(),
            });
            logs.push(LogEntry {
                message: "水揚げデータが確定 (Certified) されました".into(),
                class: "certify".into(),
            });
            s.add_log("水揚げ確定完了 (Authority 過半数合意)", "certify");

            explanation = "\
                <strong>水揚げ確定 (Authority Certification):</strong><br>\
                漁協が<strong>Certified write</strong>を発行し、Authority ノード群の過半数合意で確定しました。<br><br>\
                • Authority 3ノード中 <strong>2ノードがACK</strong> → 過半数達成 → <strong>Certified</strong><br>\
                • 確定済みデータは<strong>Proof Bundle</strong>(Ed25519署名)付きで検証可能<br>\
                • Eventual (速い書き込み) + Certified (信頼できる確定) の<strong>ハイブリッド整合性</strong>モデル<br><br>\
                これにより、沖合での高速記録と、水揚げ時の信頼性保証を両立しています。\
            "
            .into();
        }

        _ => {
            return Json(ScenarioResponse {
                ok: false,
                log: vec![],
                explanation: "不正なステップ".into(),
            });
        }
    }

    for l in &logs {
        s.log.push(l.clone());
    }

    Json(ScenarioResponse {
        ok: true,
        log: logs,
        explanation,
    })
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let state: SharedState = Arc::new(Mutex::new(DemoState::new()));

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/demo/state", get(state_handler))
        .route("/demo/catch", post(catch_handler))
        .route("/demo/scenario/{step}", post(scenario_handler))
        .with_state(state);

    let addr = "0.0.0.0:8080";
    println!("=================================================");
    println!("  漁獲高管理システム - AsteroidDB Demo");
    println!("  http://localhost:8080");
    println!("=================================================");
    println!();
    println!("ブラウザで上記URLを開いてください。");
    println!("Ctrl-C で停止します。");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
