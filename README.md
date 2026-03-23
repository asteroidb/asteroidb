# AsteroidDB

整合性レベルの異なるワークロードを単一クラスタで統合運用する分散キーバリューストア。
マルチリージョンデータセンターから高遅延の衛星コンステレーションまで対応する設計です。

## 主要機能

- **デュアル整合性モデル** -- 操作ごとに可用性優先の *Eventual* 書き込み（CRDT ベース）と
  Authority 確認済みの *Certified* 書き込みを選択可能。
- **CRDT ネイティブストレージ** -- PN-Counter、OR-Set、OR-Map、LWW-Register を搭載し、
  ネットワーク分断後も自動的にコンフリクトフリーなマージを実行。
- **BLS threshold signatures** -- BLS12-381 aggregate signatures による majority certificate
  （Ed25519 フォールバック付き）。
- **タグベース配置** -- 固定のトポロジー階層なし。任意のノードタグ、必須/禁止制約、
  レイテンシ考慮ランキングでレプリカ配置を制御。
- **SLO モニタリング** -- certification レイテンシ、sync 失敗率、frontier スキューの
  error budget トラッキングを内蔵。
- **Control Plane** -- system namespace に配置ポリシーと Authority 定義を格納し、
  quorum consensus で更新。

## アーキテクチャ

```
                         +-----------+
                         |  Client   |
                         +-----+-----+
                               |  HTTP API
              +----------------+----------------+
              |                |                |
        +-----v-----+   +-----v------+   +-----v--------+
        | Data Plane |   | Authority  |   | Control      |
        |            |   | Plane      |   | Plane        |
        | CRDT Store |   | Majority   |   | System NS    |
        | Delta Sync |   | Consensus  |   | Tag Policies |
        | Compaction |   | ack_frontier|  | Keyset Mgmt  |
        +-----+------+  | Certificate|   +-----+--------+
              |          +-----+------+         |
              +----------------+---------+------+
                               |
                    +----------v-----------+
                    |     Node Layer       |
                    | store / subscribe /  |
                    | both                 |
                    | Tag-based Placement  |
                    +----------------------+
```

**Data Plane** -- CRDT の読み書き、ピア間の anti-entropy delta sync、ログ圧縮を担当。
書き込みはローカルで受理され、非同期で伝播します。

**Authority Plane** -- キー範囲ごとの Authority ノード群。過半数が更新を確認すると
（HLC ベースの `ack_frontier` で追跡）、`majority_certificate` が発行されます。
クライアントは暗号学的証明付きの Certified read を要求可能です。

**Control Plane** -- `system namespace` で配置ポリシーと Authority 定義を管理。
変更には control-plane Authority ノード群の quorum consensus が必要です。

## クイックスタート

### 前提条件

- Rust toolchain (edition 2024, 1.85+)
- Docker & Docker Compose（マルチノードクラスタ用）

### ビルド

```bash
cargo build --release
```

### 単一ノードの実行

```bash
cargo run
# 127.0.0.1:3000 でリッスン開始
```

環境変数:

| 変数 | デフォルト値 | 説明 |
|------|------------|------|
| `ASTEROIDB_BIND_ADDR` | `127.0.0.1:3000` | HTTP リッスンアドレス |
| `ASTEROIDB_NODE_ID` | `node-1` | 一意なノード識別子 |
| `ASTEROIDB_ADVERTISE_ADDR` | bind と同じ | ピアに公開するアドレス |
| `ASTEROIDB_INTERNAL_TOKEN` | *(なし)* | ノード間認証用 Bearer トークン |
| `ASTEROIDB_DATA_DIR` | `./data` | 永続化ディレクトリ |
| `ASTEROIDB_CONFIG` | *(なし)* | JSON 設定ファイルのパス |
| `ASTEROIDB_BLS_SEED` | *(なし)* | 16 進エンコードされた 32 バイト BLS 鍵シード |
| `ASTEROIDB_AUTHORITY_NODES` | `auth-1,auth-2,auth-3` | Authority ノード ID のカンマ区切りリスト |

### Docker Compose で 3 ノードクラスタを実行

```bash
# 起動
docker compose up -d --build

# ヘルスチェック
scripts/cluster-status.sh

# 停止
docker compose down
```

各ノードは `localhost:3001`、`localhost:3002`、`localhost:3003` で公開されます。

### インタラクティブデモの実行

```bash
scripts/demo.sh
```

## API の使用例

### Eventual write (LWW Register)

```bash
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"register_set","key":"sensor-1","value":"23.5"}'
```

### Eventual read

```bash
curl -s http://localhost:3001/api/eventual/sensor-1 | jq .
# {"key":"sensor-1","value":{"type":"register","value":"23.5"}}
```

### CRDT counter 操作

```bash
# カウンタのインクリメント
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"counter_inc","key":"page-views"}'

# カウンタの読み取り
curl -s http://localhost:3001/api/eventual/page-views | jq .
# {"key":"page-views","value":{"type":"counter","value":1}}
```

### OR-Set 操作

```bash
# Set に要素を追加
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"set_add","key":"tags","element":"important"}'

# 要素を削除
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"set_remove","key":"tags","element":"important"}'
```

### Certified write

```bash
curl -s -X POST http://localhost:3001/api/certified/write \
  -H 'Content-Type: application/json' \
  -d '{
    "key": "balance",
    "value": {"type":"register","value":"1000"},
    "on_timeout": "pending"
  }'
```

### Certified read（証明付き）

```bash
curl -s http://localhost:3001/api/certified/balance | jq .
# 値 + 認証ステータス + 暗号学的証明バンドルを返却
```

### 認証ステータスの確認

```bash
curl -s http://localhost:3001/api/status/balance | jq .
# {"key":"balance","status":"certified"}
```

### SLO budget

```bash
curl -s http://localhost:3001/api/slo | jq .
```

### メトリクス

```bash
curl -s http://localhost:3001/api/metrics | jq .
```

## CLI

`asteroidb-cli` バイナリは運用コマンドを提供します:

```bash
# CLI のビルド
cargo build --release --bin asteroidb-cli

# ノードステータスの概要
asteroidb-cli status

# キーの読み取り
asteroidb-cli get sensor-1

# Register 値の書き込み
asteroidb-cli put sensor-1 "23.5"

# 詳細メトリクス
asteroidb-cli metrics

# SLO error budget
asteroidb-cli slo
```

`--host` または `ASTEROIDB_HOST` で対象ノードを指定:

```bash
asteroidb-cli --host 127.0.0.1:3002 status
```

## 開発

### ビルドとテスト

```bash
cargo build                    # デバッグビルド
cargo build --release          # リリースビルド
cargo test                     # 全テスト
cargo test --lib               # ライブラリユニットテストのみ
cargo test <module>            # 特定モジュール
```

### Lint とフォーマット

```bash
cargo fmt --check              # フォーマット確認
cargo fmt                      # 自動フォーマット
cargo clippy -- -D warnings    # Lint (CI gate)
```

### CI gate（マージ前に通過必須）

```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

### ネットワークシミュレーション

```bash
# 軽量 netem シナリオ (tc / NET_ADMIN が必要)
scripts/test-netem-light.sh
```

## プロジェクト構成

```
src/
  lib.rs                  # ライブラリルート
  main.rs                 # バイナリエントリポイント (HTTP サーバー + NodeRunner)
  bin/cli.rs              # asteroidb-cli バイナリ
  crdt/                   # CRDT 実装
    pn_counter.rs         #   PN-Counter
    or_set.rs             #   OR-Set
    or_map.rs             #   OR-Map + LWW-Register
    lww_register.rs       #   LWW-Register
  store/                  # バージョン管理付き KV ストレージ + 永続化
  authority/              # 合意・証明書管理
    ack_frontier.rs       #   HLC ベースの frontier 追跡
    certificate.rs        #   Ed25519 / BLS デュアルモード証明書
    bls.rs                #   BLS12-381 threshold signatures
  placement/              # タグベースレプリカ配置
    policy.rs             #   配置ポリシー
    latency.rs            #   スライディングウィンドウ RTT モデル
    topology.rs           #   リージョン対応トポロジービュー
    rebalance.rs          #   リバランス計画の算出
  control_plane/          # System namespace と quorum consensus
  network/                # ピア管理と delta sync
    membership.rs         #   Fan-out join/leave プロトコル
    sync.rs               #   Anti-entropy delta sync（backoff 付き）
  ops/                    # 運用ツーリング
    metrics.rs            #   ランタイムメトリクス収集
    slo.rs                #   SLO フレームワークと error budget
  compaction/             # ログ圧縮エンジン
    engine.rs             #   適応型チューニング付き圧縮
    tuner.rs              #   書き込みレートトラッカー
  api/                    # クライアント API ロジック
    certified.rs          #   Certified read/write
    eventual.rs           #   Eventual read/write
    status.rs             #   認証ステータス
  http/                   # HTTP API レイヤ (Axum)
    routes.rs             #   ルート定義
    handlers.rs           #   リクエストハンドラ
    types.rs              #   リクエスト/レスポンス型
    auth.rs               #   Bearer トークンミドルウェア
  hlc.rs                  # Hybrid Logical Clock
  node.rs                 # ノード定義
  error.rs                # 共通エラー型
  types.rs                # 共通型定義
  runtime/                # NodeRunner バックグラウンドループ
docs/                     # ドキュメント
configs/                  # Docker 用ノード別 JSON 設定
scripts/                  # クラスタ管理・テストスクリプト
tests/                    # 統合 / E2E テスト
```

## ドキュメント

| ドキュメント | 説明 |
|------------|------|
| [アーキテクチャ](docs/architecture.md) | コンポーネント設計、データフロー、シーケンス図 |
| [はじめに](docs/getting-started.md) | AsteroidDB のビルド・実行・検証 |
| [ベンチマーク](docs/benchmark.md) | パフォーマンスベンチマークとプロファイリング |
| [Netem テスト](docs/netem-testing.md) | ネットワークエミュレーションテストシナリオ |
| [セキュリティ](SECURITY.md) | 脅威モデル、信頼境界、暗号プリミティブ |
| [ビジョン](docs/vision.md) | プロジェクトの目標とスコープ |
| [要件定義](docs/requirements.md) | MVP 機能要件・非機能要件 |

## コントリビューション

コントリビューションを歓迎します！ プルリクエストの送信により
[コントリビューターライセンス契約](CLA.md) に同意したものとみなされます。
コミットには `git commit -s` でサインオフを付与してください。

## ライセンス

Apache License, Version 2.0 の下でライセンスされています。
詳細は [LICENSE](LICENSE) を参照してください。
