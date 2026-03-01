# AsteroidDB Getting Started Guide

本ドキュメントでは、AsteroidDB を新規環境でビルド・実行・検証するための手順を説明します。

## 1. セットアップ

### 前提条件

| 項目 | 要件 |
|------|------|
| **OS** | macOS / Linux (Windows は WSL2 経由を推奨) |
| **Rust toolchain** | Edition 2024 対応 (rustc 1.85.0 以降) |
| **Cargo** | Rust toolchain に付属 |
| **Git** | 2.x 以降 |

Rust がインストールされていない場合:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### リポジトリの取得

```bash
git clone <repository-url>
cd asteroidb
```

### ビルド

```bash
# デバッグビルド
cargo build

# リリースビルド (最適化あり)
cargo build --release
```

### Lint / フォーマット確認

CI gate と同等のチェック:

```bash
cargo fmt --all -- --check                         # フォーマット確認
cargo clippy --all-targets --all-features -- -D warnings  # lint
cargo test                                        # 全テスト実行
```

## 2. アーキテクチャ概要

AsteroidDB は 3 つのプレーンで構成される分散 KVS です。

```
┌─────────────────────────────────────────────────────┐
│                    Client API                        │
│  get_eventual / get_certified / certified_write      │
│  eventual_write / crdt.<type>.<op>                   │
├──────────────┬──────────────────────┬───────────────┤
│  Data Plane  │  Authority Plane     │ Control Plane │
│  CRDT Store  │  Majority Consensus  │ System NS     │
│  Replication │  ack_frontier (HLC)  │ Tag Policies  │
│  Compaction  │  majority_certificate│ Keyset Mgmt   │
├──────────────┴──────────────────────┴───────────────┤
│              Node Layer (store / subscribe / both)   │
│              Tag-based Placement (no fixed hierarchy)│
└─────────────────────────────────────────────────────┘
```

### Data Plane

CRDT (Conflict-free Replicated Data Types) を用いてデータの可用性と整合性を両立します。MVP では以下の CRDT 型をサポートします:

- **PN-Counter**: 加算・減算可能なカウンタ
- **OR-Set**: 観測削除可能な集合 (add-wins セマンティクス)
- **OR-Map**: 観測削除可能な連想配列 (LWW 値)
- **LWW-Register**: Last-Writer-Wins レジスタ

データは CRDT マージにより、ネットワーク分断後も自動的に整合収束します。

### Authority Plane

Authority ノード群の過半数合意 (majority consensus) により、Eventual なデータに対して Certified (確定) 状態を付与します。

- `ack_frontier`: 各 Authority が取り込んだ更新の HLC 到達境界
- `majority_certificate`: Ed25519 個別署名集約 (将来 BLS Threshold 拡張予定)

認証ステータスは以下の 4 状態を取ります: `pending` | `certified` | `rejected` | `timeout`

### Control Plane

`system namespace` により配置ポリシーと Authority 定義をデータベース自身で管理します。

- タグベースのノード配置 (固定階層なし)
- レプリカ数、必須タグ/禁止タグ、分断時挙動の制御
- 配置ポリシーの更新は control-plane Authority の合意で確定

## 3. API 利用の最小シナリオ

### ノードの起動

```bash
cargo run
```

起動すると、HTTP サーバーが `127.0.0.1:3000` で起動し、3 つの Authority ノード (`auth-1`, `auth-2`, `auth-3`) を含むデフォルト構成でバックグラウンド処理を開始します。バインドアドレスは環境変数 `ASTEROIDB_BIND_ADDR` で、ノード ID は `ASTEROIDB_NODE_ID` で変更可能です。`Ctrl-C` で停止します。

### HTTP API エンドポイント一覧

| メソッド | パス | 説明 |
|---------|------|------|
| `POST` | `/api/eventual/write` | Eventual write (CRDT 操作) |
| `GET` | `/api/eventual/{key}` | Eventual read |
| `POST` | `/api/certified/write` | Certified write |
| `POST` | `/api/certified/verify` | Proof bundle の検証 |
| `GET` | `/api/certified/{key}` | Certified read (ステータス付き) |
| `GET` | `/api/status/{key}` | 認証ステータス確認 |
| `GET` | `/api/metrics` | ランタイムメトリクス取得 |
| `GET/PUT` | `/api/control-plane/authorities` | Authority 定義の一覧/更新 |
| `GET` | `/api/control-plane/authorities/{prefix}` | Authority 定義の取得 |
| `GET/PUT` | `/api/control-plane/policies` | 配置ポリシーの一覧/更新 |
| `GET/DELETE` | `/api/control-plane/policies/{prefix}` | 配置ポリシーの取得/削除 |
| `GET` | `/api/control-plane/versions` | ポリシーバージョン履歴 |

> **URL エンコーディングに関する注意**: `{key}` は URL の単一パスセグメントとしてマッチします。キーにスラッシュ (`/`) などの特殊文字を含む場合は、URL エンコードが必要です。例: キー `sensor/temp` は `sensor%2Ftemp` と記述してください。エンコードしない場合、ルーティングが正しく行われず 404 エラーになります。

### 3.1 Eventual Read/Write

Eventual モードではローカル受理後に伝播し、CRDT マージで最終的に収束します。

**Counter のインクリメント:**

```bash
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"hits"}'
# => {"ok":true}
```

**Counter の読み取り:**

```bash
curl http://localhost:3000/api/eventual/hits
# => {"key":"hits","value":{"type":"counter","value":1}}
```

**Set への要素追加:**

```bash
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"users","element":"alice"}'
# => {"ok":true}
```

**Map への値設定:**

```bash
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"map_set","key":"config","map_key":"name","map_value":"AsteroidDB"}'
# => {"ok":true}
```

**LWW-Register への値設定:**

```bash
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"greeting","value":"hello"}'
# => {"ok":true}
```

Eventual write のリクエストボディは `type` フィールドで操作種別を指定します:

| type | 必須フィールド | 説明 |
|------|-------------|------|
| `counter_inc` | `key` | カウンタ加算 |
| `counter_dec` | `key` | カウンタ減算 |
| `set_add` | `key`, `element` | Set への追加 |
| `set_remove` | `key`, `element` | Set からの削除 |
| `map_set` | `key`, `map_key`, `map_value` | Map への設定 |
| `map_delete` | `key`, `map_key` | Map からの削除 |
| `register_set` | `key`, `value` | Register への設定 |

### 3.2 Certified Write / Status 確認

Certified write は Authority ノード群の過半数合意で確定する書き込みです。
`on_timeout` パラメータで、タイムアウト時の振る舞いを制御できます。

**Certified write (on_timeout=pending):**

```bash
curl -X POST http://localhost:3000/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{
    "key": "sensor/temp",
    "value": {"type": "counter", "value": 42},
    "on_timeout": "pending"
  }'
# => {"status":"pending"}
```

**Certified write (on_timeout=error):**

```bash
curl -X POST http://localhost:3000/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{
    "key": "sensor/temp",
    "value": {"type": "register", "value": "25.3C"},
    "on_timeout": "error"
  }'
# Authority 合意がタイムアウトした場合:
# => 504 {"error_code":"TIMEOUT","message":"timeout"}
```

**認証ステータスの確認:**

```bash
curl http://localhost:3000/api/status/sensor%2Ftemp
# => {"key":"sensor/temp","status":"pending"}
```

**Certified read (値 + ステータス + frontier):**

```bash
curl http://localhost:3000/api/certified/sensor%2Ftemp
# => {
#      "key": "sensor/temp",
#      "value": {"type": "counter", "value": 42},
#      "status": "pending",
#      "frontier": {"physical": 1700000000000, "logical": 0, "node_id": "auth-1"}
#    }
```

2 ステップ運用パターン:

1. `eventual_write` でまず書き込み
2. `get_certification_status` でポーリングして確定を待つ

### 3.3 System Namespace でポリシー設定

System Namespace は HTTP API または Rust API で設定できます。

HTTP API 例:

```bash
# Authority 定義を更新
curl -X PUT http://localhost:3000/api/control-plane/authorities \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix":"sensor/",
    "authority_nodes":["auth-1","auth-2","auth-3"]
  }'

# 配置ポリシーを更新
curl -X PUT http://localhost:3000/api/control-plane/policies \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix":"sensor/",
    "replica_count":3,
    "required_tags":["region:us-east"],
    "forbidden_tags":[],
    "allow_local_write_on_partition":true,
    "certified":true
  }'
```

Rust API 例:

```rust
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};

// System Namespace の作成
let mut ns = SystemNamespace::new();

// Authority 定義: "sensor/" プレフィックスのキーに対する Authority ノード群
ns.set_authority_definition(AuthorityDefinition {
    key_range: KeyRange { prefix: "sensor/".into() },
    authority_nodes: vec![
        NodeId("auth-1".into()),
        NodeId("auth-2".into()),
        NodeId("auth-3".into()),
    ],
});

// 配置ポリシー: レプリカ数3、認証対象
let policy = PlacementPolicy::new(
    PolicyVersion(1),
    KeyRange { prefix: "sensor/".into() },
    3,  // replica_count
)
.with_certified(true)
.with_required_tags(vec!["region:us-east".into()])
.with_local_write_on_partition(true);

ns.set_placement_policy(policy);

// ポリシーの確認
let p = ns.get_placement_policy("sensor/").unwrap();
println!("Replica count: {}", p.replica_count);

// キーに対する Authority の解決 (最長プレフィックスマッチ)
let auth = ns.get_authorities_for_key("sensor/temp").unwrap();
println!("Authority nodes: {:?}", auth.authority_nodes);
```

Control-plane ポリシー更新の合意:

```rust
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;

// Control-plane consensus で配置ポリシーの更新を提案
let mut consensus = ControlPlaneConsensus::new(ns);
let new_policy = PlacementPolicy::new(
    PolicyVersion(2),
    KeyRange { prefix: "sensor/".into() },
    5,  // レプリカ数を 5 に変更
);

// Authority ノード群の過半数承認を集めて適用
let result = consensus.propose_policy_update(
    new_policy,
    vec![
        NodeId("auth-1".into()),
        NodeId("auth-2".into()),
    ],
);
// majority (2/3) に達していれば Ok(())
```

## 4. テスト実行

### テストの種類

AsteroidDB のテストは以下のカテゴリに分かれています:

| カテゴリ | 実行方法 | 説明 |
|---------|---------|------|
| **ユニットテスト** | `cargo test --lib` | 各モジュール内の `#[cfg(test)] mod tests` |
| **統合テスト** | `cargo test --test integration` | `tests/integration/` 配下 |
| **分断耐性テスト** | `cargo test --test partition_tolerance` | `tests/partition_tolerance.rs` |
| **Store/CRDT/HLC テスト** | `cargo test --test store_crdt_hlc` | `tests/store_crdt_hlc.rs` |

### テスト実行コマンド

```bash
# 全テスト実行
cargo test

# ライブラリテストのみ (ユニットテスト)
cargo test --lib

# 特定モジュールのテスト
cargo test crdt::pn_counter      # PN-Counter
cargo test crdt::or_set          # OR-Set
cargo test crdt::or_map          # OR-Map
cargo test crdt::lww_register    # LWW-Register
cargo test authority             # Authority (ack_frontier + certificate)
cargo test control_plane         # Control Plane
cargo test placement             # 配置ポリシー
cargo test http                  # HTTP API
cargo test runtime               # ノードランナー
cargo test store                 # KVS ストレージ

# テスト一覧の表示
cargo test -- --list

# 特定テストの実行 (名前の部分一致)
cargo test eventual_counter_inc
```

### テストカバレッジの概要

テストは以下のモジュールを網羅しています。  
最新の件数は `cargo test -- --list` で確認してください。

- **CRDT 実装** (`src/crdt/`): マージの可換性・結合性・冪等性、収束性
- **HLC** (`src/hlc.rs`): 単調性、因果順序
- **Authority** (`src/authority/`): ack_frontier の追跡、majority_certificate の署名検証、重複排除
- **Certified API** (`src/api/certified.rs`): 認証フロー、retention policy、eviction
- **Eventual API** (`src/api/eventual.rs`): CRDT 操作、型チェック、マージ
- **HTTP API** (`src/http/`): リクエスト/レスポンス変換、エンドポイントテスト
- **配置ポリシー** (`src/placement/`): タグフィルタリング、ノード選択
- **Control Plane** (`src/control_plane/`): System Namespace 永続化、合意プロトコル
- **ノードランナー** (`src/runtime/`): バックグラウンドタスク、グレースフルシャットダウン
- **Store** (`src/store/`): KVS 操作、スナップショット永続化
- **統合テスト**: Authority 認証フロー、配置連携、CRDT 収束、クォーラム安全性
- **分断耐性テスト**: ネットワーク分断後の収束、Certified write の挙動

## 5. Docker Compose による 3 ノードクラスタ

Docker Compose を使って、ローカル環境で 3 ノードのクラスタを起動できます。

### 前提条件

| 項目 | 要件 |
|------|------|
| **Docker** | 20.10 以降 |
| **Docker Compose** | V2 (docker compose コマンド) |

### クラスタの起動

```bash
# 補助スクリプトで起動 (ビルド + バックグラウンド起動)
./scripts/cluster-up.sh

# または直接 docker compose を実行
docker compose up -d --build
```

起動後、各ノードは以下のポートで HTTP API を公開します:

| ノード | ホスト側ポート | コンテナ内ポート |
|--------|--------------|----------------|
| node-1 | `localhost:3001` | `0.0.0.0:3000` |
| node-2 | `localhost:3002` | `0.0.0.0:3000` |
| node-3 | `localhost:3003` | `0.0.0.0:3000` |

### ヘルスチェック

```bash
./scripts/cluster-status.sh
```

出力例:

```
AsteroidDB Cluster Status
=========================

  node-1 (localhost:3001): UP
  node-2 (localhost:3002): UP
  node-3 (localhost:3003): UP

All nodes are healthy.
```

### クラスタへの操作

各ノードに対して個別に HTTP API を呼び出せます:

```bash
# node-1 に書き込み
curl -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"hits"}'

# node-2 から読み取り
curl http://localhost:3002/api/eventual/hits

# node-3 に書き込み
curl -X POST http://localhost:3003/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"users","element":"alice"}'
```

> **注**: `docker-compose.yml` のデフォルト構成では peer bootstrap を行わないため、ノード間レプリケーションは有効化されません。各ノードは独立した HTTP API サーバとして動作します。

### クラスタの停止

```bash
./scripts/cluster-down.sh

# または直接
docker compose down
```

### ログの確認

```bash
# 全ノードのログを表示
docker compose logs

# 特定ノードのログをフォロー
docker compose logs -f node-1
```

### 設定ファイル

各ノードの設定例は `configs/` ディレクトリに格納されています:

- `configs/node-1.json` - node-1 の NodeConfig
- `configs/node-2.json` - node-2 の NodeConfig
- `configs/node-3.json` - node-3 の NodeConfig

これらは `NodeConfig::load()` で読み込み可能な JSON 形式です。将来的にノード起動時に設定ファイルを指定する機能が追加される予定です。

## 6. デモシナリオ

### ノード起動とバックグラウンド処理

```bash
cargo run
```

デフォルト構成ではノード `node-1` が以下のバックグラウンド処理を実行します:

- **Certification processing** (1秒間隔): pending write を再評価
- **Cleanup** (5秒間隔): 期限切れ pending write の除去
- **Compaction check** (10秒間隔): チェックポイント作成の判定
- **Frontier report** (1秒間隔): Authority ノードの場合のみ frontier を報告
- **Anti-entropy sync** (2秒間隔): SyncClient が設定され、かつ peers がある場合のみ実行

`Ctrl-C` で終了できます:

```
AsteroidDB starting...
HTTP server listening on 127.0.0.1:3000
Node run loop started. Press Ctrl-C to stop.
^C
Shutting down...
AsteroidDB stopped.
```

### Partition Tolerance のデモ

分断耐性の動作確認はテストスイートで検証できます:

```bash
# 分断耐性テストの実行
cargo test --test partition_tolerance -- --nocapture

# 主要なシナリオ:
# - 分断中の eventual write 継続
# - 分断回復後の CRDT マージ収束
# - Authority majority 喪失時の certified write 挙動
# - 非対称負荷での収束
```

### demo_partition_recovery

`demo_partition_recovery` は実装済みです。

```bash
cargo run --example demo_partition_recovery
```

このデモは、3ノードの分断と復旧、CRDT 収束、Certified 状態遷移を一連で確認できます。

## 7. トラブルシューティング

### ビルドエラー

| エラー | 原因 | 対処法 |
|--------|------|--------|
| `edition 2024 is not supported` | Rust toolchain が古い | `rustup update stable` で最新版に更新 |
| `failed to resolve: use of undeclared crate` | 依存クレートが見つからない | `cargo clean && cargo build` で再ビルド |
| `linker 'cc' not found` | C コンパイラが未インストール | macOS: `xcode-select --install` / Linux: `apt install build-essential` |

### テストエラー

| エラー | 原因 | 対処法 |
|--------|------|--------|
| `test ... timed out` | 非同期テストのデッドロック | 環境依存の可能性あり。`cargo test -- --test-threads=1` で再試行 |
| `address already in use` | ポート競合 | 他プロセスがポートを使用中。プロセスを終了して再試行 |

### API エラー

| HTTP ステータス | エラーコード | 原因 | 対処法 |
|----------------|-------------|------|--------|
| 400 | `INVALID_ARGUMENT` | リクエストパラメータ不正 | リクエストボディの JSON 形式を確認 |
| 400 | `INVALID_OP` | CRDT 型に対して無効な操作 | 操作対象の型を確認 |
| 404 | `KEY_NOT_FOUND` | 対象キーが存在しない | `set_remove` / `map_delete` は事前にキーが存在する必要あり |
| 409 | `TYPE_MISMATCH` | 既存キーの CRDT 型と操作型が不一致 | 同一キーには同じ CRDT 型の操作のみ可能 |
| 409 | `STALE_VERSION` | 古い文脈での更新 | 最新の状態を取得して再試行 |
| 403 | `POLICY_DENIED` | ポリシー違反 | Authority 定義が存在するか確認 |
| 504 | `TIMEOUT` | Authority 合意タイムアウト | `on_timeout=pending` に変更してポーリングパターンを使用 |
| 500 | `INTERNAL` | 内部エラー | ログを確認して Issue を報告 |
| 422 | (Axum) | 不正な JSON リクエスト | JSON の構造が期待されるスキーマと一致しているか確認 |

### よくある問題

**Q: `cargo clippy --all-targets --all-features -- -D warnings` で警告が出る**

A: コードの修正が必要です。clippy の指摘に従って修正してください。CI gate はこのチェックを通過する必要があります。

**Q: テストが不安定 (flaky) な場合**

A: 一部の非同期テスト (`runtime::node_runner` など) はタイミングに依存します。再実行で解決する場合がありますが、繰り返し失敗する場合は Issue を作成してください。

**Q: `certified_write` が常に `pending` になる**

A: Authority ノードの ack_frontier が書き込みの HLC タイムスタンプに到達していない状態です。単一ノード構成では Authority フロンティアの自動進行がないため、テストでは `update_frontier` API で明示的にフロンティアを進める必要があります。
