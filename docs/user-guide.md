# AsteroidDB ユーザーガイド

AsteroidDB は、Eventual（結果整合性）と Certified（確定整合性）の2つの整合性レベルを単一クラスタで提供する分散キーバリューストアです。本ガイドでは、インストールからクラスタ運用、基本操作、ユースケース別チュートリアルまでを解説します。

## 目次

- [1. インストール](#1-インストール)
  - [1.1 Docker を使用する方法（推奨）](#11-docker-を使用する方法推奨)
  - [1.2 ソースからビルドする方法](#12-ソースからビルドする方法)
- [2. クラスタセットアップ](#2-クラスタセットアップ)
  - [2.1 シングルノード構成](#21-シングルノード構成)
  - [2.2 3ノードクラスタ構成](#22-3ノードクラスタ構成)
  - [2.3 マルチリージョン構成](#23-マルチリージョン構成)
- [3. 基本操作ガイド](#3-基本操作ガイド)
  - [3.1 Eventual Read / Write](#31-eventual-read--write)
  - [3.2 Certified Read / Write](#32-certified-read--write)
  - [3.3 CLI を使った操作](#33-cli-を使った操作)
- [4. ユースケースチュートリアル](#4-ユースケースチュートリアル)
  - [4.1 カウンタ: ページビュー集計](#41-カウンタ-ページビュー集計)
  - [4.2 セット操作: タグ管理](#42-セット操作-タグ管理)
  - [4.3 Certified Write: 金融残高の管理](#43-certified-write-金融残高の管理)
  - [4.4 OR-Map: 設定値の管理](#44-or-map-設定値の管理)
- [5. タグベースの配置ポリシー設定](#5-タグベースの配置ポリシー設定)
  - [5.1 基本的な配置ポリシー](#51-基本的な配置ポリシー)
  - [5.2 Authority 定義の設定](#52-authority-定義の設定)
  - [5.3 マルチリージョンの配置ポリシー例](#53-マルチリージョンの配置ポリシー例)
- [6. 運用・監視](#6-運用監視)
  - [6.1 メトリクスの確認](#61-メトリクスの確認)
  - [6.2 SLO モニタリング](#62-slo-モニタリング)
  - [6.3 クラスタトポロジーの確認](#63-クラスタトポロジーの確認)
- [7. トラブルシューティング FAQ](#7-トラブルシューティング-faq)

---

## 1. インストール

### 1.1 Docker を使用する方法（推奨）

最も簡単に AsteroidDB を試す方法は、Docker Compose を使用することです。

**前提条件:**

| 項目 | 要件 |
|------|------|
| Docker | 20.10 以降 |
| Docker Compose | V2（`docker compose` コマンド） |

**手順:**

```bash
# リポジトリを取得
git clone <repository-url>
cd asteroidb

# 3ノードクラスタを起動
docker compose up -d --build
```

起動後、各ノードは以下のポートで HTTP API を公開します。

| ノード | URL |
|--------|-----|
| node-1 | `http://localhost:3001` |
| node-2 | `http://localhost:3002` |
| node-3 | `http://localhost:3003` |

動作確認:

```bash
# ヘルスチェック
curl -s http://localhost:3001/healthz
# => "ok"
```

### 1.2 ソースからビルドする方法

**前提条件:**

| 項目 | 要件 |
|------|------|
| OS | macOS / Linux（Windows は WSL2 経由を推奨） |
| Rust toolchain | Edition 2024 対応（rustc 1.85.0 以降） |
| Git | 2.x 以降 |

**手順:**

```bash
# Rust ツールチェインが未インストールの場合
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# リポジトリを取得
git clone <repository-url>
cd asteroidb

# リリースビルド
cargo build --release

# サーバーを起動
cargo run --release
# => HTTP server listening on 127.0.0.1:3000
```

CLI ツールもビルドできます:

```bash
cargo build --release --bin asteroidb-cli
# バイナリは target/release/asteroidb-cli に生成されます
```

---

## 2. クラスタセットアップ

### 2.1 シングルノード構成

開発やテストの目的で、1台のノードだけで AsteroidDB を起動できます。

```bash
cargo run --release
```

デフォルトでは以下の設定で起動します:

| 項目 | デフォルト値 |
|------|-------------|
| バインドアドレス | `127.0.0.1:3000` |
| ノード ID | `node-1` |
| データディレクトリ | `./data` |

環境変数で設定を変更できます:

```bash
ASTEROIDB_BIND_ADDR=0.0.0.0:8080 \
ASTEROIDB_NODE_ID=my-node \
ASTEROIDB_DATA_DIR=/var/lib/asteroidb \
cargo run --release
```

**環境変数一覧:**

| 環境変数 | デフォルト | 説明 |
|---------|---------|------|
| `ASTEROIDB_BIND_ADDR` | `127.0.0.1:3000` | HTTP のリッスンアドレス |
| `ASTEROIDB_NODE_ID` | `node-1` | ノードの一意な識別子 |
| `ASTEROIDB_ADVERTISE_ADDR` | バインドアドレスと同じ | ピアに公開するアドレス |
| `ASTEROIDB_INTERNAL_TOKEN` | （なし） | ノード間認証の Bearer トークン |
| `ASTEROIDB_DATA_DIR` | `./data` | データ永続化ディレクトリ |
| `ASTEROIDB_CONFIG` | （なし） | JSON 設定ファイルのパス |
| `ASTEROIDB_BLS_SEED` | （なし） | BLS 鍵の Hex エンコード 32 バイトシード |
| `ASTEROIDB_AUTHORITY_NODES` | `auth-1,auth-2,auth-3` | Authority ノード ID のカンマ区切りリスト |

> **注意:** シングルノード構成では Authority の過半数合意が成立しないため、Certified Write は `pending` 状態のままになります。Certified 機能を使用するには 3 ノード以上のクラスタが必要です。

### 2.2 3ノードクラスタ構成

本番環境を想定した基本的な構成です。Docker Compose を使う方法と、手動で起動する方法があります。

#### Docker Compose を使用する方法

```bash
# クラスタを起動
docker compose up -d --build

# ステータスを確認
scripts/cluster-status.sh

# 出力例:
# AsteroidDB Cluster Status
# =========================
#   node-1 (localhost:3001): UP
#   node-2 (localhost:3002): UP
#   node-3 (localhost:3003): UP
# All nodes are healthy.
```

#### 手動で起動する方法

各ノード用の設定ファイルを作成します。

**node-1 の設定ファイル（`configs/node-1.json`）:**

```json
{
  "node": { "id": "node-1", "mode": "Both", "tags": [] },
  "bind_addr": "0.0.0.0:3001",
  "peers": {
    "self_id": "node-1",
    "peers": {
      "node-2": { "node_id": "node-2", "addr": "127.0.0.1:3002" },
      "node-3": { "node_id": "node-3", "addr": "127.0.0.1:3003" }
    }
  }
}
```

**node-2 の設定ファイル（`configs/node-2.json`）:**

```json
{
  "node": { "id": "node-2", "mode": "Both", "tags": [] },
  "bind_addr": "0.0.0.0:3002",
  "peers": {
    "self_id": "node-2",
    "peers": {
      "node-1": { "node_id": "node-1", "addr": "127.0.0.1:3001" },
      "node-3": { "node_id": "node-3", "addr": "127.0.0.1:3003" }
    }
  }
}
```

**node-3 の設定ファイル（`configs/node-3.json`）:**

```json
{
  "node": { "id": "node-3", "mode": "Both", "tags": [] },
  "bind_addr": "0.0.0.0:3003",
  "peers": {
    "self_id": "node-3",
    "peers": {
      "node-1": { "node_id": "node-1", "addr": "127.0.0.1:3001" },
      "node-2": { "node_id": "node-2", "addr": "127.0.0.1:3002" }
    }
  }
}
```

それぞれ別のターミナルで起動します:

```bash
# ターミナル 1
ASTEROIDB_CONFIG=configs/node-1.json \
ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3 \
cargo run --release

# ターミナル 2
ASTEROIDB_CONFIG=configs/node-2.json \
ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3 \
cargo run --release

# ターミナル 3
ASTEROIDB_CONFIG=configs/node-3.json \
ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3 \
cargo run --release
```

### 2.3 マルチリージョン構成

異なるリージョンにノードを配置する場合、タグを使用してリージョン情報を設定します。

**東京リージョンのノード設定例:**

```json
{
  "node": {
    "id": "tokyo-1",
    "mode": "Both",
    "tags": ["region:ap-northeast-1", "az:ap-northeast-1a", "tier:primary"]
  },
  "bind_addr": "0.0.0.0:3000",
  "peers": {
    "self_id": "tokyo-1",
    "peers": {
      "tokyo-2": { "node_id": "tokyo-2", "addr": "10.1.0.2:3000" },
      "osaka-1": { "node_id": "osaka-1", "addr": "10.2.0.1:3000" },
      "virginia-1": { "node_id": "virginia-1", "addr": "10.3.0.1:3000" }
    }
  }
}
```

**大阪リージョンのノード設定例:**

```json
{
  "node": {
    "id": "osaka-1",
    "mode": "Both",
    "tags": ["region:ap-northeast-3", "az:ap-northeast-3a", "tier:primary"]
  },
  "bind_addr": "0.0.0.0:3000",
  "peers": {
    "self_id": "osaka-1",
    "peers": {
      "tokyo-1": { "node_id": "tokyo-1", "addr": "10.1.0.1:3000" },
      "tokyo-2": { "node_id": "tokyo-2", "addr": "10.1.0.2:3000" },
      "virginia-1": { "node_id": "virginia-1", "addr": "10.3.0.1:3000" }
    }
  }
}
```

**ノード間認証の設定:**

マルチリージョン構成ではノード間通信にトークン認証を有効にすることを推奨します。全ノードで同じ `ASTEROIDB_INTERNAL_TOKEN` を設定してください。

```bash
ASTEROIDB_INTERNAL_TOKEN=your-secure-token-here \
ASTEROIDB_CONFIG=configs/tokyo-1.json \
cargo run --release
```

---

## 3. 基本操作ガイド

AsteroidDB は HTTP API を通じてすべての操作を行います。以下では `curl` コマンドを使った操作例を示します。

### 3.1 Eventual Read / Write

Eventual モードは、可用性（Availability）を優先する操作です。書き込みはローカルノードに即座に受理され、バックグラウンドのデルタ同期によって他のノードに伝播します。CRDT（Conflict-free Replicated Data Types）のマージにより、ネットワーク分断後でも自動的に整合収束します。

#### 対応する CRDT 型と操作

| 操作種別 | `type` パラメータ | 必須フィールド | 説明 |
|---------|-----------------|-------------|------|
| カウンタ加算 | `counter_inc` | `key` | PN-Counter を +1 |
| カウンタ減算 | `counter_dec` | `key` | PN-Counter を -1 |
| セット追加 | `set_add` | `key`, `element` | OR-Set に要素を追加 |
| セット削除 | `set_remove` | `key`, `element` | OR-Set から要素を削除 |
| マップ設定 | `map_set` | `key`, `map_key`, `map_value` | OR-Map にキーバリューを設定 |
| マップ削除 | `map_delete` | `key`, `map_key` | OR-Map からキーを削除 |
| レジスタ設定 | `register_set` | `key`, `value` | LWW-Register に値を設定 |

#### 書き込み例

```bash
# カウンタをインクリメント
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"page-views"}'
# => {"ok":true}

# セットに要素を追加
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"active-users","element":"alice"}'
# => {"ok":true}

# レジスタに値を設定
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"sensor/temp","value":"23.5"}'
# => {"ok":true}
```

#### 読み取り例

```bash
# カウンタの値を取得
curl -s http://localhost:3001/api/eventual/page-views | jq .
# => {"key":"page-views","value":{"type":"counter","value":1}}

# セットの内容を取得
curl -s http://localhost:3001/api/eventual/active-users | jq .
# => {"key":"active-users","value":{"type":"set","elements":["alice"]}}

# レジスタの値を取得
curl -s http://localhost:3001/api/eventual/sensor/temp | jq .
# => {"key":"sensor/temp","value":{"type":"register","value":"23.5"}}
```

> **ポイント:** キーにスラッシュ（`/`）を含めることができます。`sensor/temp` は `/api/eventual/sensor/temp` でアクセスできます（URL エンコード不要）。

#### ノード間のレプリケーション確認

3ノードクラスタの場合、別のノードからも同じデータを読み取れます:

```bash
# node-1 に書き込み
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"greeting","value":"hello"}'

# 少し待ってから node-2 で読み取り（デルタ同期は2秒間隔で実行）
sleep 3
curl -s http://localhost:3002/api/eventual/greeting | jq .
# => {"key":"greeting","value":{"type":"register","value":"hello"}}
```

### 3.2 Certified Read / Write

Certified モードは、Authority ノード群の過半数合意を得ることで、データの確定状態を保証する操作です。金融データや重要な設定変更など、強い整合性が必要な場合に使用します。

#### Certified Write

```bash
curl -s -X POST http://localhost:3001/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{
    "key": "account/balance",
    "value": {"type": "register", "value": "1000"},
    "on_timeout": "pending"
  }' | jq .
# => {"status":"pending"}
```

`on_timeout` パラメータで、Authority の合意がタイムアウトした場合の挙動を制御します:

| `on_timeout` | 挙動 |
|-------------|------|
| `pending` | タイムアウト時に `pending` ステータスを返し、バックグラウンドで合意を継続 |
| `error` | タイムアウト時に HTTP 504 エラーを返す |

#### 認証ステータスの確認

```bash
curl -s http://localhost:3001/api/status/account/balance | jq .
# => {"key":"account/balance","status":"certified"}
```

ステータスは以下の4種類です:

| ステータス | 説明 |
|-----------|------|
| `pending` | Authority の合意待ち |
| `certified` | 過半数の Authority が承認済み（確定） |
| `rejected` | 合意に失敗 |
| `timeout` | 合意がタイムアウト |

#### Certified Read（証明付き読み取り）

```bash
curl -s http://localhost:3001/api/certified/account/balance | jq .
# => {
#      "key": "account/balance",
#      "value": {"type": "register", "value": "1000"},
#      "status": "certified",
#      "frontier": {"physical": 1700000000000, "logical": 0, "node_id": "node-1"}
#    }
```

Certified Read は値に加えて、認証ステータスと frontier（HLC タイムスタンプ）を返します。`certified` ステータスの場合、Authority の過半数がこの値を承認していることが暗号的に証明されています。

#### 証明の検証

```bash
curl -s -X POST http://localhost:3001/api/certified/verify \
  -H "Content-Type: application/json" \
  -d '{ "proof_bundle": <取得した証明データ> }'
```

#### 推奨パターン: 2ステップ運用

即座に確定が得られない場合は、以下の2ステップパターンを推奨します:

1. `eventual/write` で書き込み
2. `status/{key}` でポーリングして確定を待つ

```bash
# ステップ 1: 書き込み
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"important-data","value":"critical-value"}'

# ステップ 2: ポーリング
while true; do
  STATUS=$(curl -s http://localhost:3001/api/status/important-data | jq -r '.status')
  echo "Status: $STATUS"
  if [ "$STATUS" = "certified" ]; then
    echo "Data is certified!"
    break
  fi
  sleep 1
done
```

### 3.3 CLI を使った操作

`asteroidb-cli` を使用すると、コマンドラインから簡単に操作できます。

```bash
# ノードのステータスを確認
asteroidb-cli status

# キーの値を取得
asteroidb-cli get sensor/temp

# 値を書き込み（LWW-Register として）
asteroidb-cli put sensor/temp "25.0"

# メトリクスの詳細を表示
asteroidb-cli metrics

# SLO エラーバジェットを表示
asteroidb-cli slo
```

別のノードに接続する場合は `--host` オプションを使用します:

```bash
asteroidb-cli --host 127.0.0.1:3002 status
```

環境変数 `ASTEROIDB_HOST` でデフォルトの接続先を変更することもできます:

```bash
export ASTEROIDB_HOST=127.0.0.1:3002
asteroidb-cli status
```

---

## 4. ユースケースチュートリアル

### 4.1 カウンタ: ページビュー集計

複数のエッジノードからアクセスカウントを集計するシナリオです。PN-Counter を使用することで、ネットワーク分断中も各ノードで独立にカウントを続け、復旧後に自動マージされます。

```bash
# === ノード 1 でページビューを記録 ===
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"page/home/views"}'

curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"page/home/views"}'

# === ノード 2 でもページビューを記録 ===
curl -s -X POST http://localhost:3002/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"page/home/views"}'

# === 同期後に任意のノードで合計を確認 ===
sleep 3
curl -s http://localhost:3003/api/eventual/page/home/views | jq .
# => {"key":"page/home/views","value":{"type":"counter","value":3}}
```

カウンタは減算もサポートします（PN-Counter のマイナス側）:

```bash
# 「いいね」を取り消す
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_dec","key":"page/home/views"}'

sleep 3
curl -s http://localhost:3001/api/eventual/page/home/views | jq .
# => {"key":"page/home/views","value":{"type":"counter","value":2}}
```

### 4.2 セット操作: タグ管理

OR-Set（Observed-Remove Set）を使用してタグやラベルを管理するシナリオです。OR-Set は add-wins セマンティクスを持ち、並行して追加と削除が発生した場合は追加が優先されます。

```bash
# === 記事にタグを追加 ===
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"article/123/tags","element":"rust"}'

curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"article/123/tags","element":"database"}'

curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"article/123/tags","element":"distributed-systems"}'

# === タグ一覧を確認 ===
curl -s http://localhost:3001/api/eventual/article/123/tags | jq .
# => {"key":"article/123/tags","value":{"type":"set","elements":["database","distributed-systems","rust"]}}

# === タグを削除 ===
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_remove","key":"article/123/tags","element":"database"}'

# === 削除後のタグ一覧を確認 ===
curl -s http://localhost:3001/api/eventual/article/123/tags | jq .
# => {"key":"article/123/tags","value":{"type":"set","elements":["distributed-systems","rust"]}}
```

#### 並行操作時の挙動

2つのノードで同時にタグ操作を行った場合:

```bash
# ノード 1 で "important" を追加
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"article/123/tags","element":"important"}'

# 同時にノード 2 で "featured" を追加
curl -s -X POST http://localhost:3002/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"article/123/tags","element":"featured"}'

# 同期後、両方のタグが含まれる（コンフリクトなし）
sleep 3
curl -s http://localhost:3001/api/eventual/article/123/tags | jq .
# => elements には "important" と "featured" の両方が含まれる
```

### 4.3 Certified Write: 金融残高の管理

Certified Write を使用して、権威ある確定が必要なデータを管理するシナリオです。

```bash
# === 口座残高を設定（Certified Write） ===
curl -s -X POST http://localhost:3001/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{
    "key": "account/user-alice/balance",
    "value": {"type": "register", "value": "50000"},
    "on_timeout": "pending"
  }' | jq .
# => {"status":"pending"}

# === 認証ステータスを確認 ===
curl -s http://localhost:3001/api/status/account/user-alice/balance | jq .
# => {"key":"account/user-alice/balance","status":"certified"}

# === 確定済みの値を証明付きで読み取り ===
curl -s http://localhost:3001/api/certified/account/user-alice/balance | jq .
# => {
#      "key": "account/user-alice/balance",
#      "value": {"type": "register", "value": "50000"},
#      "status": "certified",
#      "frontier": {...}
#    }
```

**`on_timeout=error` を使用する場合:**

即座に確定が得られなければエラーとして扱いたい場合に使用します:

```bash
curl -s -X POST http://localhost:3001/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{
    "key": "transaction/tx-001",
    "value": {"type": "register", "value": "transfer:alice->bob:1000"},
    "on_timeout": "error"
  }' | jq .
# Authority の合意がタイムアウトした場合:
# => HTTP 504: {"error_code":"TIMEOUT","message":"timeout"}
```

### 4.4 OR-Map: 設定値の管理

OR-Map を使用して、ネストされたキーバリューデータを管理するシナリオです。

```bash
# === アプリケーション設定を保存 ===
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"map_set","key":"app/config","map_key":"theme","map_value":"dark"}'

curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"map_set","key":"app/config","map_key":"language","map_value":"ja"}'

curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"map_set","key":"app/config","map_key":"notifications","map_value":"enabled"}'

# === 設定を確認 ===
curl -s http://localhost:3001/api/eventual/app/config | jq .
# => {"key":"app/config","value":{"type":"map","entries":{"theme":"dark","language":"ja","notifications":"enabled"}}}

# === 特定の設定を更新 ===
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"map_set","key":"app/config","map_key":"theme","map_value":"light"}'

# === 不要な設定を削除 ===
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"map_delete","key":"app/config","map_key":"notifications"}'
```

---

## 5. タグベースの配置ポリシー設定

AsteroidDB は固定のトポロジー階層（Region > DC > Rack）を持たず、タグベースの柔軟な配置ポリシーを使用します。これにより、通常のマルチ DC 構成から衛星コンステレーションまで、同じ仕組みで配置を制御できます。

### 5.1 基本的な配置ポリシー

配置ポリシーの設定には、Control Plane API を使用します。**設定の変更には Authority ノードの過半数承認が必要です。**

```bash
# "sensor/" プレフィックスのキーに対する配置ポリシーを設定
curl -s -X PUT http://localhost:3001/api/control-plane/policies \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "sensor/",
    "replica_count": 3,
    "required_tags": ["tier:primary"],
    "certified": true,
    "approvals": ["node-1", "node-2"]
  }' | jq .
```

ポリシーの設定項目:

| フィールド | 型 | 説明 |
|-----------|------|------|
| `key_range_prefix` | string | 対象キーのプレフィックス |
| `replica_count` | number | 最小レプリカ数 |
| `required_tags` | string[] | ノードに必須のタグ |
| `forbidden_tags` | string[] | このタグを持つノードを除外 |
| `certified` | boolean | Certified 機能を有効にするか |
| `approvals` | string[] | ポリシー変更を承認する Authority ノード ID |

#### ポリシーの確認

```bash
# 全ポリシーの一覧
curl -s http://localhost:3001/api/control-plane/policies | jq .

# 特定プレフィックスのポリシーを取得
curl -s http://localhost:3001/api/control-plane/policies/sensor | jq .

# ポリシーのバージョン履歴
curl -s http://localhost:3001/api/control-plane/versions | jq .
```

### 5.2 Authority 定義の設定

特定のキー範囲に対して、Certified 機能の合意を行う Authority ノード群を定義します:

```bash
curl -s -X PUT http://localhost:3001/api/control-plane/authorities \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "account/",
    "authority_nodes": ["node-1", "node-2", "node-3"],
    "approvals": ["node-1", "node-2"]
  }' | jq .
```

#### Authority 定義の確認

```bash
# 全 Authority 定義の一覧
curl -s http://localhost:3001/api/control-plane/authorities | jq .

# 特定プレフィックスの Authority 定義を取得
curl -s http://localhost:3001/api/control-plane/authorities/account | jq .
```

### 5.3 マルチリージョンの配置ポリシー例

**例 1: 東京リージョン限定のデータ**

日本国内のデータ保護規制に対応する場合:

```bash
curl -s -X PUT http://localhost:3001/api/control-plane/policies \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "jp-data/",
    "replica_count": 3,
    "required_tags": ["region:ap-northeast-1"],
    "forbidden_tags": ["decommissioning"],
    "certified": true,
    "approvals": ["node-1", "node-2"]
  }' | jq .
```

**例 2: 低遅延が必要なテレメトリデータ**

リージョンを問わず、最も近いノードにデータを配置する場合:

```bash
curl -s -X PUT http://localhost:3001/api/control-plane/policies \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "telemetry/",
    "replica_count": 2,
    "required_tags": ["tier:primary"],
    "certified": false,
    "approvals": ["node-1", "node-2"]
  }' | jq .
```

**例 3: 廃止予定ノードの除外**

メンテナンス対象ノードを配置から除外する場合:

```bash
curl -s -X PUT http://localhost:3001/api/control-plane/policies \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "critical/",
    "replica_count": 3,
    "required_tags": ["tier:primary"],
    "forbidden_tags": ["decommissioning", "maintenance"],
    "certified": true,
    "approvals": ["node-1", "node-2"]
  }' | jq .
```

---

## 6. 運用・監視

### 6.1 メトリクスの確認

ノードのランタイムメトリクスを取得します:

```bash
curl -s http://localhost:3001/api/metrics | jq .
```

主要なメトリクス:

| メトリクス | 説明 |
|-----------|------|
| `pending_count` | 認証待ちの書き込み数 |
| `certified_total` | 認証完了した書き込みの累計 |
| `certification_latency_mean_us` | 認証の平均レイテンシ（マイクロ秒） |
| `frontier_skew_ms` | Frontier のスキュー（ミリ秒） |
| `sync_failure_rate` | 同期の失敗率 |
| `sync_attempt_total` | 同期試行の累計 |

CLI を使う場合:

```bash
# サマリー表示
asteroidb-cli status

# 詳細メトリクス
asteroidb-cli metrics
```

### 6.2 SLO モニタリング

AsteroidDB は以下の SLO（Service Level Objective）を内蔵で追跡します:

- **認証レイテンシ** -- Certified Write の処理時間
- **同期失敗率** -- ノード間同期の失敗率
- **Frontier スキュー** -- Authority ノード間の Frontier 差

```bash
curl -s http://localhost:3001/api/slo | jq .
```

CLI を使う場合:

```bash
asteroidb-cli slo
# 出力例:
# === SLO Budget Status ===
#
# SLO                                     Total Violations Remaining%   Status
# --------------------------------------------------------------------------------
# certification_latency                      42          0      100.0%       OK
# frontier_skew                              42          0      100.0%       OK
# sync_failure_rate                          15          0      100.0%       OK
```

### 6.3 クラスタトポロジーの確認

クラスタ内のノード構成とリージョン情報を確認します:

```bash
curl -s http://localhost:3001/api/topology | jq .
```

---

## 7. トラブルシューティング FAQ

### インストール・ビルド関連

**Q: `edition 2024 is not supported` というエラーが出る**

A: Rust ツールチェインが古い可能性があります。以下のコマンドで最新版に更新してください:

```bash
rustup update stable
rustc --version  # 1.85.0 以降であることを確認
```

**Q: `linker 'cc' not found` というエラーが出る**

A: C コンパイラが必要です。お使いの OS に応じてインストールしてください:

```bash
# macOS
xcode-select --install

# Ubuntu / Debian
sudo apt install build-essential

# Fedora / RHEL
sudo dnf install gcc
```

**Q: Docker ビルドが失敗する**

A: Docker デーモンが起動しているか確認してください。また、十分なディスク容量（Rust のビルドには数 GB が必要）があることを確認してください:

```bash
docker info        # Docker の状態確認
docker system df   # ディスク使用量の確認
docker system prune -f  # 不要なイメージの削除
```

### クラスタ・ネットワーク関連

**Q: ノード間でデータが同期されない**

A: 以下を確認してください:

1. 設定ファイル（`ASTEROIDB_CONFIG`）でピア情報が正しく設定されているか
2. ノード間のネットワーク疎通性があるか
3. `ASTEROIDB_INTERNAL_TOKEN` が全ノードで同一か（設定している場合）

```bash
# ノードが起動しているか確認
curl -s http://localhost:3001/healthz

# メトリクスで同期状況を確認
curl -s http://localhost:3001/api/metrics | jq '{sync_attempt_total, sync_failure_rate}'
```

デルタ同期は 2 秒間隔で実行されるため、書き込み直後にデータが他ノードに反映されていない場合は数秒待ってから再確認してください。

**Q: `address already in use` というエラーが出る**

A: 指定したポートが既に別のプロセスで使用されています:

```bash
# ポートを使用しているプロセスを確認（Linux）
ss -tlnp | grep 3000

# ポートを使用しているプロセスを確認（macOS）
lsof -i :3000

# 該当プロセスを停止するか、別のポートで起動する
ASTEROIDB_BIND_ADDR=127.0.0.1:3010 cargo run --release
```

### API 関連

**Q: `certified_write` が常に `pending` になる**

A: 以下の原因が考えられます:

1. **シングルノード構成** -- Certified 機能は Authority の過半数合意が必要です。3 ノード以上のクラスタを構成してください。
2. **Authority ノードが定義されていない** -- 対象キー範囲に Authority 定義があることを確認してください:
   ```bash
   curl -s http://localhost:3001/api/control-plane/authorities | jq .
   ```
3. **Frontier が進行していない** -- Authority ノード間のフロンティア同期が正常に動作しているか、メトリクスを確認してください。

**Q: `TYPE_MISMATCH` エラーが返される**

A: 同一キーに対して異なる CRDT 型の操作を行おうとしています。例えば、`counter_inc` で作成したキーに `set_add` を実行することはできません:

```bash
# キーの現在の型を確認
curl -s http://localhost:3001/api/eventual/my-key | jq '.value.type'
```

キーごとに CRDT 型は固定されるため、別の型が必要な場合は別のキーを使用してください。

**Q: `POLICY_DENIED` エラーが返される**

A: 配置ポリシーや Authority 定義の更新には、過半数の Authority ノードの承認（`approvals`）が必要です。3 ノード構成の場合、最低 2 ノードの承認が必要です:

```bash
# 正しい例（2/3 の承認）
curl -s -X PUT http://localhost:3001/api/control-plane/policies \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "data/",
    "replica_count": 3,
    "approvals": ["node-1", "node-2"]
  }'

# エラーになる例（1/3 の承認では不足）
curl -s -X PUT http://localhost:3001/api/control-plane/policies \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "data/",
    "replica_count": 3,
    "approvals": ["node-1"]
  }'
# => 403 {"error_code":"POLICY_DENIED","message":"insufficient approvals for policy update"}
```

**Q: `KEY_NOT_FOUND` エラーが返される**

A: 存在しないキーに対して `set_remove` や `map_delete` を実行した場合に発生します。先にキーが作成されていることを確認してください:

```bash
# まず値が存在するか確認
curl -s http://localhost:3001/api/eventual/my-key | jq .
```

### パフォーマンス関連

**Q: 書き込みレイテンシが高い**

A: Eventual Write はローカル受理のため通常は低レイテンシですが、高い場合は以下を確認してください:

1. コンパクション間隔が適切か -- デフォルトでは 30 秒または 10,000 操作ごと
2. ディスク I/O がボトルネックでないか
3. SLO モニタリングで異常がないか

```bash
asteroidb-cli slo
```

**Q: テストが不安定（flaky）に見える**

A: 一部の非同期テストはタイミングに依存します。以下を試してください:

```bash
# シングルスレッドでテストを再実行
cargo test -- --test-threads=1
```

繰り返し失敗する場合は、GitHub Issue を作成してください。

---

## HTTP API エンドポイント一覧

参照用のエンドポイント早見表です。

### クライアント API

| メソッド | パス | 説明 |
|---------|------|------|
| `POST` | `/api/eventual/write` | Eventual write（CRDT 操作） |
| `GET` | `/api/eventual/{*key}` | Eventual read |
| `POST` | `/api/certified/write` | Certified write |
| `POST` | `/api/certified/verify` | 証明の検証 |
| `GET` | `/api/certified/{*key}` | Certified read（ステータス・証明付き） |
| `GET` | `/api/status/{*key}` | 認証ステータスの確認 |

### Control Plane API

| メソッド | パス | 説明 |
|---------|------|------|
| `PUT` | `/api/control-plane/authorities` | Authority 定義の設定 |
| `GET` | `/api/control-plane/authorities` | Authority 定義の一覧 |
| `GET` | `/api/control-plane/authorities/{prefix}` | Authority 定義の取得 |
| `PUT` | `/api/control-plane/policies` | 配置ポリシーの設定 |
| `GET` | `/api/control-plane/policies` | 配置ポリシーの一覧 |
| `GET/DELETE` | `/api/control-plane/policies/{prefix}` | 配置ポリシーの取得・削除 |
| `GET` | `/api/control-plane/versions` | ポリシーバージョン履歴 |

### 運用 API

| メソッド | パス | 説明 |
|---------|------|------|
| `GET` | `/api/metrics` | メトリクスの取得 |
| `GET` | `/api/slo` | SLO ステータスの取得 |
| `GET` | `/api/topology` | クラスタトポロジーの取得 |
| `GET` | `/healthz` | ヘルスチェック |
