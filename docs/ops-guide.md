# AsteroidDB 運用ガイド

本ドキュメントは AsteroidDB クラスタの運用・監視・トラブルシューティングに関する包括的なガイドです。
既存の Runbook（`docs/runbook/`）を補完し、監視設定・アラート基準・容量計画・パフォーマンスチューニングをカバーします。

---

## 目次

1. [デプロイメント構成](#1-デプロイメント構成)
2. [環境変数リファレンス](#2-環境変数リファレンス)
3. [監視・アラート設定](#3-監視アラート設定)
4. [SLO メトリクスの解釈とアラート基準](#4-slo-メトリクスの解釈とアラート基準)
5. [バックアップ・リストア手順](#5-バックアップリストア手順)
6. [ログ設定とログレベル](#6-ログ設定とログレベル)
7. [パフォーマンスチューニング](#7-パフォーマンスチューニング)
8. [ネットワーク設定](#8-ネットワーク設定)
9. [ノード追加・削除手順](#9-ノード追加削除手順)
10. [配置ポリシー変更手順](#10-配置ポリシー変更手順)
11. [容量計画](#11-容量計画)
12. [トラブルシューティング](#12-トラブルシューティング)
13. [障害復旧手順](#13-障害復旧手順)

---

## 1. デプロイメント構成

### 1.1 Docker Compose（推奨：開発・テスト環境）

プロジェクト付属の `docker-compose.yml` を使って 3 ノードクラスタを起動できます。

```bash
# 内部認証トークンを設定（省略可、省略時は認証なし）
export ASTEROIDB_INTERNAL_TOKEN="my-secret-token"

# クラスタ起動
docker compose up -d

# ポートマッピング
#   node-1: localhost:3001 -> container:3000
#   node-2: localhost:3002 -> container:3000
#   node-3: localhost:3003 -> container:3000
```

各ノードの設定ファイルは `configs/node-{1,2,3}.json` に配置されています。
設定ファイルの構造:

```json
{
  "node": {
    "id": "node-1",
    "mode": "Both",
    "tags": []
  },
  "bind_addr": "0.0.0.0:3000",
  "peers": {
    "self_id": "node-1",
    "peers": {
      "node-2": {
        "node_id": "node-2",
        "addr": "asteroidb-node-2:3000"
      },
      "node-3": {
        "node_id": "node-3",
        "addr": "asteroidb-node-3:3000"
      }
    }
  }
}
```

ノードモードは以下の 3 種類:
- `Store`: データストアのみ（レプリケーション対象）
- `Subscribe`: 読み取り専用レプリカ
- `Both`: ストア + Authority 合意に参加（デフォルト）

### 1.2 Docker 単体デプロイ

```bash
# イメージビルド
docker build -t asteroidb:latest .

# シングルノード起動
docker run -d \
  --name asteroidb-node-1 \
  -p 3000:3000 \
  -e ASTEROIDB_NODE_ID=node-1 \
  -e ASTEROIDB_BIND_ADDR=0.0.0.0:3000 \
  -e ASTEROIDB_ADVERTISE_ADDR=10.0.1.1:3000 \
  -e ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3 \
  -e ASTEROIDB_DATA_DIR=/data \
  -v /var/lib/asteroidb/node-1:/data \
  asteroidb:latest
```

### 1.3 systemd デプロイ（本番推奨）

```ini
# /etc/systemd/system/asteroidb.service
[Unit]
Description=AsteroidDB Node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=asteroidb
Group=asteroidb
ExecStart=/usr/local/bin/asteroidb-poc
Restart=on-failure
RestartSec=5

# 環境変数
Environment=ASTEROIDB_NODE_ID=node-1
Environment=ASTEROIDB_BIND_ADDR=0.0.0.0:3000
Environment=ASTEROIDB_ADVERTISE_ADDR=10.0.1.1:3000
Environment=ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3
Environment=ASTEROIDB_DATA_DIR=/var/lib/asteroidb
Environment=RUST_LOG=asteroidb_poc=info

# セキュリティ設定（トークンは EnvironmentFile で管理）
EnvironmentFile=/etc/asteroidb/env
# /etc/asteroidb/env に ASTEROIDB_INTERNAL_TOKEN=xxx を記載

# リソース制限
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

```bash
# サービス管理
sudo systemctl enable asteroidb
sudo systemctl start asteroidb
sudo systemctl status asteroidb
sudo journalctl -u asteroidb -f
```

### 1.4 Bare Metal デプロイ

```bash
# ビルド
cargo build --release

# バイナリ配置
sudo cp target/release/asteroidb-poc /usr/local/bin/

# データディレクトリ作成
sudo mkdir -p /var/lib/asteroidb
sudo chown asteroidb:asteroidb /var/lib/asteroidb

# 起動
ASTEROIDB_NODE_ID=node-1 \
ASTEROIDB_BIND_ADDR=0.0.0.0:3000 \
ASTEROIDB_ADVERTISE_ADDR=10.0.1.1:3000 \
ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3 \
ASTEROIDB_DATA_DIR=/var/lib/asteroidb \
RUST_LOG=asteroidb_poc=info \
/usr/local/bin/asteroidb-poc
```

---

## 2. 環境変数リファレンス

| 変数名 | 必須 | デフォルト | 説明 |
|--------|------|-----------|------|
| `ASTEROIDB_NODE_ID` | いいえ | `node-1` | ノードの一意識別子 |
| `ASTEROIDB_BIND_ADDR` | いいえ | `127.0.0.1:3000` | HTTP サーバのバインドアドレス |
| `ASTEROIDB_ADVERTISE_ADDR` | いいえ | `BIND_ADDR` と同じ | 他ノードからの接続先アドレス |
| `ASTEROIDB_CONFIG` | いいえ | なし | JSON 設定ファイルパス |
| `ASTEROIDB_AUTHORITY_NODES` | いいえ | `auth-1,auth-2,auth-3` | Authority ノード ID（カンマ区切り） |
| `ASTEROIDB_DATA_DIR` | いいえ | `./data` | データ永続化ディレクトリ |
| `ASTEROIDB_INTERNAL_TOKEN` | いいえ | なし | 内部 API 認証用 Bearer トークン |
| `ASTEROIDB_BLS_SEED` | いいえ | なし | BLS 鍵生成用 hex シード（32 バイト） |
| `RUST_LOG` | いいえ | なし | ログレベル（tracing-subscriber 形式） |

---

## 3. 監視・アラート設定

### 3.1 メトリクスエンドポイント

AsteroidDB は JSON 形式でメトリクスを提供します。

```bash
# メトリクス取得
curl -s http://localhost:3000/api/metrics | jq .

# ヘルスチェック
curl -s http://localhost:3000/healthz
# => {"status":"ok"}

# SLO ステータス
curl -s http://localhost:3000/api/slo | jq .
```

### 3.2 利用可能なメトリクス一覧

`GET /api/metrics` が返す `MetricsSnapshot` のフィールド:

| メトリクス | 型 | 説明 |
|-----------|------|------|
| `pending_count` | u64 | 現在の保留中 Certified Write 数 |
| `certified_total` | u64 | Certified Write 累計 |
| `certification_latency_mean_us` | f64 | 証明レイテンシ平均 (us) |
| `frontier_skew_ms` | u64 | Authority frontier 最大スキュー (ms) |
| `sync_failure_rate` | f64 | 同期失敗率 (0.0-1.0) |
| `sync_attempt_total` | u64 | 同期試行累計 |
| `sync_failure_total` | u64 | 同期失敗累計 |
| `delta_sync_count` | u64 | デルタ同期累計 |
| `full_sync_fallback_count` | u64 | フルシンクフォールバック累計 |
| `full_sync_fallback_ratio` | f64 | フルシンク比率 (0.0-1.0) |
| `write_ops_total` | u64 | 書き込み操作累計 |
| `rebalance_start_total` | u64 | リバランス開始累計 |
| `rebalance_keys_migrated` | u64 | リバランス移行キー累計 |
| `rebalance_keys_failed` | u64 | リバランス失敗キー累計 |
| `rebalance_complete_total` | u64 | リバランス完了累計 |
| `rebalance_duration_sum_us` | u64 | リバランス所要時間合計 (us) |
| `key_rotation_total` | u64 | 鍵ローテーション累計 |
| `key_rotation_last_version` | u64 | 最新 keyset バージョン |
| `key_rotation_last_time_ms` | u64 | 最新ローテーション時刻 (ms) |
| `peer_sync` | map | ピアごとの同期統計（60 秒スライディングウィンドウ） |
| `certification_latency_window` | object | 証明レイテンシウィンドウ統計（60 秒） |

`peer_sync` の各ピアエントリ:

| フィールド | 説明 |
|-----------|------|
| `mean_latency_us` | ウィンドウ内平均同期レイテンシ (us) |
| `p99_latency_us` | ウィンドウ内 P99 同期レイテンシ (us) |
| `success_count` | 累計成功数 |
| `failure_count` | 累計失敗数 |

`certification_latency_window`:

| フィールド | 説明 |
|-----------|------|
| `sample_count` | ウィンドウ内サンプル数 |
| `mean_us` | ウィンドウ内平均レイテンシ (us) |
| `p99_us` | ウィンドウ内 P99 レイテンシ (us) |

### 3.3 Prometheus 連携

AsteroidDB はネイティブの Prometheus エクスポーターを持ちませんが、JSON メトリクスを Prometheus 形式に変換できます。

**方法 1: json_exporter を使用**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'asteroidb'
    metrics_path: /probe
    params:
      module: [asteroidb]
    static_configs:
      - targets:
          - http://node-1:3000/api/metrics
          - http://node-2:3001/api/metrics
          - http://node-3:3002/api/metrics
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: json-exporter:7979
```

**方法 2: スクリプトで変換**

```bash
#!/bin/bash
# asteroidb-metrics-exporter.sh
# cron で定期実行し、Prometheus textfile collector で収集
NODES="node-1:3000 node-2:3001 node-3:3002"
OUTPUT="/var/lib/prometheus/node-exporter/asteroidb.prom"

for node in $NODES; do
  METRICS=$(curl -s "http://${node}/api/metrics")
  NODE_ID=$(echo "$node" | cut -d: -f1)

  echo "asteroidb_pending_count{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.pending_count')"
  echo "asteroidb_certified_total{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.certified_total')"
  echo "asteroidb_sync_failure_rate{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.sync_failure_rate')"
  echo "asteroidb_frontier_skew_ms{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.frontier_skew_ms')"
  echo "asteroidb_write_ops_total{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.write_ops_total')"
  echo "asteroidb_full_sync_fallback_ratio{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.full_sync_fallback_ratio')"
  echo "asteroidb_certification_latency_p99_us{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.certification_latency_window.p99_us')"
done > "$OUTPUT"
```

### 3.4 Grafana ダッシュボード推奨パネル

| パネル | クエリ対象 | 閾値 |
|--------|-----------|------|
| Certification Latency P99 | `certification_latency_window.p99_us` | Warning: 300ms, Critical: 500ms |
| Sync Failure Rate | `sync_failure_rate` | Warning: 5%, Critical: 10% |
| Frontier Skew | `frontier_skew_ms` | Warning: 5000ms, Critical: 10000ms |
| Pending Writes | `pending_count` | Warning: 100, Critical: 1000 |
| Full Sync Fallback Ratio | `full_sync_fallback_ratio` | Warning: 30%, Critical: 50% |
| Write Throughput | `write_ops_total` (rate) | 情報表示のみ |
| Rebalance Progress | `rebalance_keys_migrated` vs `rebalance_keys_failed` | Failed > 0 で Warning |

### 3.5 アラートルール例

```yaml
# alertmanager rules
groups:
  - name: asteroidb
    rules:
      - alert: HighSyncFailureRate
        expr: asteroidb_sync_failure_rate > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Sync failure rate exceeds 10%"

      - alert: CriticalSyncFailureRate
        expr: asteroidb_sync_failure_rate > 0.3
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Sync failure rate exceeds 30%"

      - alert: HighFrontierSkew
        expr: asteroidb_frontier_skew_ms > 10000
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "Frontier skew exceeds 10 seconds"

      - alert: HighCertificationLatency
        expr: asteroidb_certification_latency_p99_us > 500000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Certification P99 latency exceeds 500ms"

      - alert: PendingWriteBacklog
        expr: asteroidb_pending_count > 1000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Large pending write backlog"

      - alert: HighFullSyncRatio
        expr: asteroidb_full_sync_fallback_ratio > 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Over 50% of syncs are full-sync fallbacks"
```

---

## 4. SLO メトリクスの解釈とアラート基準

### 4.1 事前定義 SLO

AsteroidDB は以下の 4 つの SLO をデフォルトで追跡します（1 時間ウィンドウ）:

| SLO 名 | 条件 | 目標値 | 目標達成率 |
|--------|------|--------|-----------|
| `eventual_read_p99` | レイテンシ < 50ms | 50.0 | 99% |
| `certified_read_p99` | レイテンシ < 500ms | 500.0 | 99% |
| `replication_convergence` | 収束時間 < 5000ms | 5000.0 | 95% |
| `authority_availability` | 可用性 > 99% | 99.0 | 99.9% |

### 4.2 エラーバジェットの解釈

`GET /api/slo` の応答例:

```json
{
  "budgets": {
    "eventual_read_p99": {
      "target": {
        "name": "eventual_read_p99",
        "kind": "LessThan",
        "target_value": 50.0,
        "target_percentage": 99.0,
        "window_secs": 3600
      },
      "total_requests": 10000,
      "violations": 50,
      "budget_remaining": 50.0,
      "is_warning": false,
      "is_critical": false
    }
  }
}
```

**エラーバジェット計算式:**

```
許容エラー率 = 1 - (target_percentage / 100)
実際のエラー率 = violations / total_requests
バジェット消費率 = 実際のエラー率 / 許容エラー率
バジェット残量 = (1 - バジェット消費率) * 100%
```

**例**: 99% SLO（許容エラー率 1%）で 10,000 リクエスト中 50 違反:
- 実エラー率 = 50/10000 = 0.5%
- バジェット消費 = 0.5% / 1% = 50%
- バジェット残量 = 50%

### 4.3 SLO アラート基準

| 状態 | バジェット残量 | アクション |
|------|--------------|-----------|
| 正常 | >= 50% | 監視のみ |
| Warning (`is_warning`) | < 50% | 調査開始、原因特定 |
| Critical (`is_critical`) | < 20% | 即時対応、オンコール通知 |
| 枯渇 | 0% | インシデント宣言 |

**推奨アラート設定:**

```yaml
- alert: SloWarning
  expr: asteroidb_slo_budget_remaining < 50
  for: 5m
  labels:
    severity: warning

- alert: SloCritical
  expr: asteroidb_slo_budget_remaining < 20
  for: 2m
  labels:
    severity: critical
```

---

## 5. バックアップ・リストア手順

### 5.1 バックアップ対象

| ファイル/ディレクトリ | 場所 | 説明 |
|---------------------|------|------|
| データディレクトリ | `$ASTEROIDB_DATA_DIR/` | ストアデータ、永続化状態 |
| ピアレジストリ | `$ASTEROIDB_DATA_DIR/peers.json` | クラスタメンバーシップ |
| システム名前空間 | `$ASTEROIDB_DATA_DIR/system_namespace.json` | 配置ポリシー、Authority 定義 |
| 設定ファイル | `$ASTEROIDB_CONFIG` (任意) | ノード設定 JSON |

### 5.2 バックアップ手順

```bash
# === 定期バックアップスクリプト ===
#!/bin/bash
BACKUP_DIR="/backup/asteroidb/$(date +%Y%m%d_%H%M%S)"
DATA_DIR="${ASTEROIDB_DATA_DIR:-./data}"
NODE_ID="${ASTEROIDB_NODE_ID:-unknown}"

mkdir -p "$BACKUP_DIR"

# データディレクトリのスナップショット
# 注意: AsteroidDB は CRDT ベースなので、ライブバックアップでも整合性は保たれる
cp -r "$DATA_DIR" "$BACKUP_DIR/data-${NODE_ID}"

# メタデータのバックアップ
curl -s "http://localhost:3000/api/metrics" > "$BACKUP_DIR/metrics-${NODE_ID}.json"
curl -s "http://localhost:3000/api/slo" > "$BACKUP_DIR/slo-${NODE_ID}.json"

# 圧縮
tar czf "$BACKUP_DIR.tar.gz" -C "$(dirname $BACKUP_DIR)" "$(basename $BACKUP_DIR)"
rm -rf "$BACKUP_DIR"

echo "Backup saved to ${BACKUP_DIR}.tar.gz"
```

**推奨バックアップ頻度:**

| 環境 | 頻度 | 保持期間 |
|------|------|---------|
| 開発 | 日次 | 7 日 |
| ステージング | 12 時間ごと | 14 日 |
| 本番 | 6 時間ごと | 30 日 |

### 5.3 リストア手順

```bash
# 1. ノード停止
sudo systemctl stop asteroidb

# 2. 既存データのバックアップ（念のため）
mv $ASTEROIDB_DATA_DIR ${ASTEROIDB_DATA_DIR}.old

# 3. バックアップからリストア
tar xzf /backup/asteroidb/20260310_060000.tar.gz -C /tmp
cp -r /tmp/data-node-1 $ASTEROIDB_DATA_DIR

# 4. ノード再起動
sudo systemctl start asteroidb

# 5. 収束を確認
# CRDT マージにより他ノードのデータが自動的に同期される
asteroidb-cli --host localhost:3000 slo
```

---

## 6. ログ設定とログレベル

### 6.1 ログフレームワーク

AsteroidDB は `tracing` + `tracing-subscriber` を使用した構造化ログを出力します。
ログレベルは `RUST_LOG` 環境変数で制御します。

### 6.2 ログレベル設定例

```bash
# 全体を info、AsteroidDB モジュールは debug
RUST_LOG=info,asteroidb_poc=debug

# 同期処理のみ詳細ログ
RUST_LOG=asteroidb_poc::network::sync=debug,asteroidb_poc=info

# 全体を warn、compaction と authority は info
RUST_LOG=warn,asteroidb_poc::compaction=info,asteroidb_poc::authority=info

# 本番環境推奨
RUST_LOG=asteroidb_poc=info

# 障害調査時
RUST_LOG=asteroidb_poc=debug

# 詳細なトレース（パフォーマンスに影響あり）
RUST_LOG=asteroidb_poc=trace
```

### 6.3 主要なログメッセージ

| モジュール | ログ内容 | レベル |
|-----------|---------|-------|
| `runtime::node_runner` | sync cycle 結果、certification 結果 | info |
| `network::sync` | peer sync 成功/失敗、delta/full sync 選択 | info/warn |
| `network::membership` | join/leave イベント、ping 結果 | info |
| `compaction::engine` | checkpoint 作成、compaction 実行 | info |
| `authority::certificate` | 証明書検証、keyset ローテーション | info |
| `placement::rebalance` | rebalance 開始/完了、キー移行 | info |

### 6.4 ログローテーション

systemd 環境では journald が自動的にログを管理します。
Docker 環境では Docker のログドライバを設定してください。

```yaml
# docker-compose.yml に追加
services:
  node-1:
    logging:
      driver: json-file
      options:
        max-size: "50m"
        max-file: "5"
```

---

## 7. パフォーマンスチューニング

### 7.1 Compaction チューニング

Compaction エンジンはチェックポイント方式でログ圧縮を行います (FR-010)。

**デフォルト設定:**

| パラメータ | デフォルト値 | 説明 |
|-----------|------------|------|
| `time_threshold_ms` | 30,000 (30 秒) | 時間ベースのチェックポイント閾値 |
| `ops_threshold` | 10,000 | 操作数ベースのチェックポイント閾値 |

いずれかの閾値に先に到達した時点でチェックポイントが作成されます。

**アダプティブ Compaction:**

アダプティブモード有効時、書き込みレートと Authority frontier lag に基づいてチューニングが自動調整されます:

| 条件 | 動作 | 範囲 |
|------|------|------|
| 書き込みレート > 750 ops/sec | ops_threshold を半減 | 最小 1,000 |
| 書き込みレート < 30 ops/sec | ops_threshold を倍増 | 最大 50,000 |
| Frontier lag > 15 秒 | time_threshold を 50% 増加 | 最大 120,000ms |
| Frontier lag < 1 秒 | time_threshold を 25% 減少 | 最小 10,000ms |

チューニング間隔はデフォルト 30 秒です。書き込みレートは 60 秒のスライディングウィンドウで計測されます。

**Compaction が実行される条件:**
1. チェックポイント閾値（時間 or 操作数）に到達
2. 同じキー範囲・ポリシーバージョンの Authority 過半数がチェックポイント以降の更新を取り込み済み

Compaction が進まない場合は Authority ノードの可用性を確認してください。

**チェックポイント履歴の保持:**
- アダプティブモード時: デフォルト 10 世代まで保持（`max_checkpoint_history`）
- 非アダプティブ時: 無制限

### 7.2 同期 (Sync) チューニング

**NodeRunner デフォルト設定:**

| パラメータ | デフォルト値 | 説明 |
|-----------|------------|------|
| `sync_interval` | 2 秒 | anti-entropy 同期間隔 |
| `ping_interval` | 10 秒 | メンバーシップ gossip 間隔 |
| `certification_interval` | 1 秒 | 保留書き込みの certification 評価間隔 |
| `compaction_check_interval` | 10 秒 | compaction チェック間隔 |
| `frontier_report_interval` | 1 秒 | Authority frontier 報告間隔 |
| `epoch_check_interval` | 60 秒 | epoch 境界チェック（鍵ローテーション）|
| `gc_interval` | 60 秒 | トゥームストーン GC 間隔 |
| `frontier_gc_interval` | 60 秒 | ack-frontier GC 間隔 |

**デルタ同期 vs フルシンク:**

デルタ同期は前回の frontier からの差分のみを送信します。以下の条件でフルシンクにフォールバックします:

- 変更キー数 / 全キー数 > `full_sync_threshold` (デフォルト 0.5 = 50%)
- デルタ同期のペイロードが `MAX_DELTA_PAYLOAD_BYTES` (512 KiB) を超過
- ピアとの frontier 情報が未初期化

**バックオフ:**

同期失敗時は指数バックオフが適用されます:
- 初回失敗: 即座にリトライ
- 連続失敗: `min(INITIAL * 2^failures, MAX_BACKOFF)` で待機
- 最大バックオフ: 2 秒
- 成功時: バックオフリセット

**チューニング指針:**

| 状況 | 推奨調整 |
|------|---------|
| 低レイテンシ環境（同一DC内） | `sync_interval` を 1 秒に短縮 |
| 高レイテンシ環境（WAN） | `sync_interval` を 5-10 秒に延長 |
| 書き込み負荷が高い | `full_sync_threshold` を 0.3 に下げて早めにフルシンク |
| メモリ使用量が多い | `gc_interval` を 30 秒に短縮 |
| フルシンク比率が高い | `sync_interval` を短縮して差分を小さく保つ |

### 7.3 バッチサイズ

同期データは `DEFAULT_BATCH_SIZE` (100 エントリ) 単位でバッチ送信されます。
大量のキーがある環境では、ネットワーク帯域とレイテンシのトレードオフを考慮してください。

---

## 8. ネットワーク設定

### 8.1 ポート構成

| ポート | プロトコル | 用途 |
|--------|----------|------|
| 3000 (デフォルト) | HTTP | クライアント API + 内部 API |

AsteroidDB は単一ポートで全トラフィック（クライアント/内部通信）を処理します。

### 8.2 API エンドポイント一覧

**クライアント API（認証不要）:**

| メソッド | パス | 説明 |
|---------|------|------|
| GET | `/healthz` | ヘルスチェック |
| GET | `/api/metrics` | メトリクス取得 |
| GET | `/api/slo` | SLO ステータス |
| POST | `/api/eventual/write` | Eventual 書き込み |
| GET | `/api/eventual/{*key}` | Eventual 読み取り |
| POST | `/api/certified/write` | Certified 書き込み |
| GET | `/api/certified/{*key}` | Certified 読み取り |
| GET | `/api/status/{*key}` | Certification ステータス |
| POST | `/api/certified/verify` | 証明検証 |
| GET | `/api/control-plane/versions` | バージョン履歴 |
| GET | `/api/topology` | トポロジー情報 |

**内部 API（トークン認証が有効時は要認証）:**

| メソッド | パス | 説明 |
|---------|------|------|
| POST | `/api/internal/sync` | フルシンク |
| POST | `/api/internal/sync/delta` | デルタシンク |
| GET | `/api/internal/keys` | 全キーダンプ |
| POST/GET | `/api/internal/frontiers` | Frontier 送受信 |
| POST | `/api/internal/join` | ノード参加 |
| POST | `/api/internal/leave` | ノード離脱 |
| POST | `/api/internal/announce` | メンバーシップ通知 |
| POST | `/api/internal/ping` | ピアリスト交換 |

**Control Plane API（トークン認証が有効時は要認証）:**

| メソッド | パス | 説明 |
|---------|------|------|
| PUT | `/api/control-plane/policies` | 配置ポリシー設定 |
| DELETE | `/api/control-plane/policies/{prefix}` | 配置ポリシー削除 |
| GET | `/api/control-plane/policies` | 配置ポリシー一覧 |
| GET | `/api/control-plane/policies/{prefix}` | 配置ポリシー取得 |
| PUT | `/api/control-plane/authorities` | Authority 定義設定 |
| GET | `/api/control-plane/authorities` | Authority 一覧 |
| GET | `/api/control-plane/authorities/{prefix}` | Authority 取得 |

### 8.3 内部認証トークン

ノード間通信を保護するために Bearer トークン認証を設定できます。

```bash
# トークン生成（全ノードで同一トークンを使用）
export ASTEROIDB_INTERNAL_TOKEN=$(openssl rand -hex 32)
```

トークン設定時:
- `/api/internal/*` エンドポイントに `Authorization: Bearer <token>` ヘッダが必要
- Control Plane の変更 API（PUT/DELETE）にも認証が必要
- 読み取り専用 API（GET /api/metrics, GET /healthz など）は認証不要

**重要:** 空文字列のトークンは「未設定」として扱われます（認証なし）。
Docker Compose で `${ASTEROIDB_INTERNAL_TOKEN}` が未定義の場合に空文字列が代入される問題を防ぐためです。

### 8.4 TLS 設定

AsteroidDB 自体は TLS 終端機能を持ちません。本番環境では以下のいずれかで TLS を終端してください:

**方法 1: リバースプロキシ**

```nginx
# nginx.conf
upstream asteroidb {
    server 127.0.0.1:3000;
}

server {
    listen 443 ssl;
    ssl_certificate /etc/ssl/certs/asteroidb.crt;
    ssl_certificate_key /etc/ssl/private/asteroidb.key;

    location / {
        proxy_pass http://asteroidb;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

**方法 2: サイドカープロキシ（Envoy, Linkerd など）**

サービスメッシュ環境では mTLS を自動的に処理できます。

### 8.5 ファイアウォール設定

```bash
# クライアントアクセス（外部から）
ufw allow 3000/tcp comment "AsteroidDB client API"

# ノード間通信（クラスタ内部のみ）
# 同一トークンによる認証が有効な場合、ポート制限は任意
ufw allow from 10.0.1.0/24 to any port 3000 proto tcp comment "AsteroidDB internal"
```

---

## 9. ノード追加・削除手順

### 9.1 ノード追加

1. **設定準備:**

   ```bash
   # 新ノードの環境変数を設定
   export ASTEROIDB_NODE_ID=node-4
   export ASTEROIDB_BIND_ADDR=0.0.0.0:3000
   export ASTEROIDB_ADVERTISE_ADDR=10.0.1.4:3000
   export ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3
   export ASTEROIDB_INTERNAL_TOKEN=<既存クラスタと同じトークン>
   ```

2. **設定ファイル作成（任意）:**

   ```json
   {
     "node": {
       "id": "node-4",
       "mode": "Both",
       "tags": ["region:ap-northeast-1"]
     },
     "bind_addr": "0.0.0.0:3000",
     "peers": {
       "self_id": "node-4",
       "peers": {
         "node-1": {"node_id": "node-1", "addr": "10.0.1.1:3000"}
       }
     }
   }
   ```

   少なくとも 1 つのシードピアを指定すれば、残りのピアは gossip で自動発見されます。

3. **ノード起動:**

   ```bash
   asteroidb-poc
   ```

   起動時に fan-out join が実行され、全既知ピアに参加を通知します。

4. **確認:**

   ```bash
   # 新ノードのステータス確認
   asteroidb-cli --host 10.0.1.4:3000 status

   # 既存ノードから新ノードが見えることを確認
   curl -s http://10.0.1.1:3000/api/internal/ping \
     -H 'Content-Type: application/json' \
     -d '{"sender_id":"probe","sender_addr":"","known_peers":[]}'

   # データ同期の進捗確認
   asteroidb-cli --host 10.0.1.4:3000 metrics
   # peer_sync にエントリが増えていることを確認
   ```

5. **Authority に追加する場合:**

   ```bash
   curl -X PUT http://10.0.1.1:3000/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{
       "key_range_prefix": "",
       "authority_nodes": ["node-1", "node-2", "node-3", "node-4"],
       "approvals": ["node-1", "node-2"]
     }'
   ```

### 9.2 ノード削除

1. **グレースフルシャットダウン（推奨）:**

   ```bash
   # Ctrl-C でプロセスを停止
   # → fan-out leave が自動実行され、全ピアに離脱を通知
   # → システム名前空間がディスクに保存される
   ```

2. **手動削除:**

   ```bash
   curl -X POST http://10.0.1.1:3000/api/internal/leave \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{"node_id": "node-4"}'
   ```

3. **Authority から削除する場合:**

   Authority 定義を更新して対象ノードを除外:

   ```bash
   curl -X PUT http://10.0.1.1:3000/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{
       "key_range_prefix": "",
       "authority_nodes": ["node-1", "node-2", "node-3"],
       "approvals": ["node-1", "node-2"]
     }'
   ```

   **注意:** Authority ノード数が過半数を下回る変更は避けてください。

4. **削除後の確認:**

   ```bash
   # 残りのノードで SLO を確認
   asteroidb-cli --host 10.0.1.1:3000 slo
   ```

---

## 10. 配置ポリシー変更手順

### 10.1 配置ポリシーの構造

配置ポリシーはキー範囲（プレフィクス）ごとにレプリカ配置を制御します:

- `replica_count`: レプリカ数
- `required_tags`: 必須タグ（全レプリカが持つべきタグ）
- `forbidden_tags`: 禁止タグ（レプリカに含めないタグ）
- `on_partition`: 分断時の挙動

### 10.2 ポリシー変更手順

1. **現在のポリシーを確認:**

   ```bash
   curl -s http://localhost:3000/api/control-plane/policies | jq .
   ```

2. **新しいポリシーを設定:**

   ```bash
   curl -X PUT http://localhost:3000/api/control-plane/policies \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{
       "key_range_prefix": "user/",
       "replica_count": 3,
       "required_tags": ["region:ap-northeast-1"],
       "forbidden_tags": [],
       "approvals": ["node-1", "node-2"]
     }'
   ```

3. **リバランスの自動実行を監視:**

   ポリシー変更後、NodeRunner がリバランスプランを自動計算し、キーの移行を開始します。

   ```bash
   # リバランス進捗の確認
   curl -s http://localhost:3000/api/metrics | jq '{
     rebalance_start_total,
     rebalance_keys_migrated,
     rebalance_keys_failed,
     rebalance_complete_total
   }'
   ```

4. **注意事項:**
   - ポリシー変更は Control Plane の過半数合意が必要
   - リバランス中もサービスは継続
   - `rebalance_keys_failed` が 0 でない場合は失敗キーの調査が必要

---

## 11. 容量計画

### 11.1 ストレージ見積もり

```
ストレージ = キー数 x 平均値サイズ x レプリカ数 x (1 + オーバーヘッド)
```

| コンポーネント | 見積もり方 |
|--------------|-----------|
| CRDT メタデータ | 値あたり約 100-200 バイト（HLC タイムスタンプ、ドット情報） |
| トゥームストーン | OR-Set/OR-Map の場合、削除操作 1 件あたり約 50 バイト（GC で回収） |
| チェックポイント | キー範囲あたり最大 10 世代（アダプティブモード時） |
| ピアレジストリ | ノード 1 台あたり約 200 バイト |
| システム名前空間 | ポリシー数 x 約 500 バイト |

**計算例:**
- 100 万キー、平均値 1KB、レプリカ数 3、オーバーヘッド 30%
- ストレージ = 1,000,000 x 1KB x 3 x 1.3 = 約 3.9 GB/ノード

### 11.2 メモリ見積もり

```
メモリ = アクティブキー数 x 平均値サイズ + 同期バッファ + メトリクスウィンドウ
```

| コンポーネント | 見積もり |
|--------------|---------|
| KV ストア | キー数 x (キーサイズ + 値サイズ + 200B メタデータ) |
| 同期バッファ | ピア数 x `MAX_DELTA_PAYLOAD_BYTES` (512 KiB) |
| メトリクスウィンドウ | ピア数 x 約 10 KiB（60 秒ウィンドウ） |
| SLO トラッカー | 4 SLO x 観測数 x 約 20 バイト |
| Compaction エンジン | キー範囲数 x チェックポイント履歴数 x 約 200 バイト |

### 11.3 ネットワーク帯域見積もり

```
帯域 = (sync_interval あたりの変更量) x ピア数
```

| シナリオ | 見積もり |
|---------|---------|
| デルタ同期（通常時） | 変更キー数 x 平均値サイズ x ピア数 / sync_interval |
| フルシンク（フォールバック時） | 全キー x 平均値サイズ x ピア数 |
| Frontier 報告 | ピア数 x 1KB / frontier_report_interval |
| Ping/Gossip | ピア数 x 500B / ping_interval |

**帯域削減のポイント:**
- `sync_interval` を適切に設定してデルタを小さく保つ
- `full_sync_threshold` を調整してフルシンク頻度を制御
- bincode シリアライゼーション（内部通信で自動使用）は JSON より約 30-50% 小さい

---

## 12. トラブルシューティング

### 12.1 同期遅延（Sync Lag）

**症状:**
- Eventual 読み取りが古いデータを返す
- `sync_failure_rate` > 0.1 (10%)
- `peer_sync` の `p99_latency_us` が SLO 目標を超過

**診断:**

```bash
# メトリクスで同期状態を確認
curl -s http://localhost:3000/api/metrics | jq '{
  sync_failure_rate,
  sync_attempt_total,
  sync_failure_total,
  delta_sync_count,
  full_sync_fallback_count,
  full_sync_fallback_ratio,
  peer_sync
}'

# SLO バジェットを確認
asteroidb-cli --host localhost:3000 slo
```

**対処:**

| 原因 | 対処 |
|------|------|
| ネットワーク障害 | ピア間の接続を確認、ファイアウォールルールの検証 |
| ピアの過負荷 | 負荷分散、ノード追加を検討 |
| デルタが大きすぎる | `sync_interval` を短縮してデルタサイズを小さく保つ |
| フルシンク頻度が高い | `full_sync_threshold` を下げてフルシンクを先行実行 |
| バックオフ中 | 最大 2 秒で回復するため一時的には待機で可 |

**手動フルシンク:**

```bash
# 正常ノードから遅延ノードへ全データを強制同期
curl -s http://good-node:3000/api/internal/keys | \
  curl -X POST http://lagging-node:3000/api/internal/sync \
    -H 'Content-Type: application/json' -d @-
```

### 12.2 Split-Brain

**症状:**
- 異なるノードが同一キーに対して異なる値を返す
- `frontier_skew_ms` が異常に高い（> 10,000ms）
- 同期失敗率の急増

**診断:**

```bash
# 全ノードの frontier skew を比較
for node in node-1:3000 node-2:3001 node-3:3002; do
  echo "=== $node ==="
  curl -s "http://$node/api/metrics" | jq '.frontier_skew_ms'
done

# ピア間の接続性を確認
curl -s http://node-1:3000/api/internal/ping \
  -H 'Content-Type: application/json' \
  -d '{"sender_id":"probe","sender_addr":"","known_peers":[]}'
```

**対処:**

1. **ネットワーク分断が原因の場合:**
   - 接続を復旧すれば CRDT マージにより自動収束
   - `replication_convergence` SLO で収束を監視

2. **クロックドリフトが原因の場合:**
   - 各ノードの NTP 同期を確認
   - HLC は中程度のドリフトを補償するが、10 秒以上のドリフトは順序異常を引き起こす可能性あり

3. **手動介入が必要な場合:**

   ```bash
   # 正常ノードのデータを信頼ソースとして他ノードに同期
   curl http://trusted-node:3000/api/internal/keys > /tmp/dump.json
   curl -X POST http://diverged-node:3001/api/internal/sync \
     -H 'Content-Type: application/json' -d @/tmp/dump.json
   ```

### 12.3 メモリ増大

**症状:**
- プロセスの RSS が時間とともに増加
- OOM Killer によるプロセス終了

**診断:**

```bash
# メトリクスで状態サイズを推定
curl -s http://localhost:3000/api/metrics | jq '{
  write_ops_total,
  pending_count,
  rebalance_keys_migrated
}'

# キー数の確認（注意: 全キーを転送するため大規模環境では負荷が高い）
curl -s http://localhost:3000/api/internal/keys | jq '.entries | length'
```

**対処:**

| 原因 | 対処 |
|------|------|
| トゥームストーン蓄積 | `gc_interval` を短縮（デフォルト 60 秒） |
| ack-frontier エントリの蓄積 | `frontier_gc_interval` を短縮、`frontier_gc_max_retained_versions` を縮小 |
| Compaction が進まない | Authority 可用性を確認（過半数必要） |
| pending_count が高い | Certified Write のタイムアウト設定を見直す |
| 大量のキーデータ | ノード追加によるデータ分散を検討 |

### 12.4 ディスク使用量増大

**症状:**
- データディレクトリのサイズが想定を超えて増大
- Compaction の完了数がゼロ

**診断:**

```bash
# データディレクトリサイズ
du -sh $ASTEROIDB_DATA_DIR

# Compaction メトリクス
curl -s http://localhost:3000/api/metrics | jq '{
  rebalance_start_total,
  rebalance_complete_total,
  rebalance_keys_migrated,
  rebalance_keys_failed,
  write_ops_total
}'
```

**対処:**

1. Compaction が止まっている場合:
   - Authority ノードの可用性を確認（過半数がチェックポイント以降の更新を取り込む必要あり）
   - `frontier_skew_ms` が大きい場合、Authority の同期遅延を解消

2. チェックポイント履歴の肥大化:
   - アダプティブ Compaction を有効にして `max_checkpoint_history` を設定

3. 古いバックアップファイルの削除

### 12.5 Certified Write のタイムアウト

**症状:**
- Certified Write が `timeout` ステータスを返す
- `pending_count` が増加し続ける

**診断:**

```bash
curl -s http://localhost:3000/api/metrics | jq '{
  pending_count,
  certified_total,
  certification_latency_mean_us,
  frontier_skew_ms
}'
```

**対処:**

| 原因 | 対処 |
|------|------|
| Authority 過半数が到達不能 | Authority ノードの復旧を優先 |
| Frontier 同期遅延 | `frontier_report_interval` を短縮 |
| ネットワーク遅延 | WAN 環境では certification timeout を延長 |
| 高負荷 | Authority ノードのリソースを増強 |

### 12.6 鍵ローテーション失敗

**症状:**
- `key_rotation_total` が期待通りに増加しない
- Certified Read の証明検証が失敗

**診断:**

```bash
curl -s http://localhost:3000/api/metrics | jq '{
  key_rotation_total,
  key_rotation_last_version,
  key_rotation_last_time_ms
}'
```

**対処:**
- BLS シードが正しく設定されているか確認（`ASTEROIDB_BLS_SEED`）
- epoch 設定の確認（デフォルト: 24 時間 epoch、7 epoch グレース期間）
- 詳細は `docs/runbook/key-rotation.md` を参照

---

## 13. 障害復旧手順

### 13.1 単一ノード再起動

```bash
# 1. グレースフルシャットダウン（Ctrl-C）
# → fan-out leave + システム名前空間保存

# 2. 再起動
sudo systemctl restart asteroidb

# 3. 起動後の動作:
# - 永続化されたピアレジストリを読み込み
# - fan-out join で全ピアに再参加を通知
# - anti-entropy sync で最新データに追いつく

# 4. 収束確認
asteroidb-cli --host localhost:3000 slo
# → replication_convergence の violations が 0 になるまで待機
```

### 13.2 データ復元

```bash
# 1. ノード停止
sudo systemctl stop asteroidb

# 2. 破損データの退避
mv $ASTEROIDB_DATA_DIR ${ASTEROIDB_DATA_DIR}.corrupted.$(date +%s)

# 3. バックアップからリストア
mkdir -p $ASTEROIDB_DATA_DIR
tar xzf /backup/asteroidb/latest.tar.gz -C $ASTEROIDB_DATA_DIR

# 4. ノード再起動
sudo systemctl start asteroidb

# 5. CRDT マージにより不足分は自動同期される
# 進捗を監視:
watch -n 5 'asteroidb-cli --host localhost:3000 metrics | grep sync'
```

### 13.3 ピアレジストリ破損時

```bash
# ピアレジストリが壊れた場合、削除して再起動
rm $ASTEROIDB_DATA_DIR/peers.json

# 設定ファイルにシードピアが含まれていれば自動復旧
# 設定ファイルがない場合はシードピアの環境変数を設定して再起動
sudo systemctl restart asteroidb
```

### 13.4 クラスタ再構築

全ノードを失った場合の完全再構築:

```bash
# 1. 最新のバックアップを各ノードにリストア
for i in 1 2 3; do
  ssh node-$i "
    mkdir -p /var/lib/asteroidb
    tar xzf /backup/asteroidb/latest-node-$i.tar.gz -C /var/lib/asteroidb
  "
done

# 2. シードノード（node-1）を最初に起動
ssh node-1 "sudo systemctl start asteroidb"

# 3. 残りのノードを順次起動
sleep 5
ssh node-2 "sudo systemctl start asteroidb"
ssh node-3 "sudo systemctl start asteroidb"

# 4. クラスタの健全性確認
for i in 1 2 3; do
  echo "=== node-$i ==="
  asteroidb-cli --host node-$i:3000 status
  asteroidb-cli --host node-$i:3000 slo
done

# 5. 全ノードのデータ整合性を確認
# CRDT マージにより自動収束するが、SLO で監視
```

### 13.5 Authority 過半数喪失時

詳細な手順は `docs/runbook/disaster-recovery.md` の Scenario 3 を参照。

**要約:**
1. 生存 Authority ノードの復旧を最優先
2. 復旧不可能な場合は Authority 定義を更新して新ノードを指定
3. 全 Authority 喪失時は手動でシステム名前空間を編集し、新 Authority を定義して再起動

---

## 付録: CLI リファレンス

```bash
# ステータス確認
asteroidb-cli --host <addr> status

# メトリクス表示
asteroidb-cli --host <addr> metrics

# SLO 確認
asteroidb-cli --host <addr> slo

# データ操作
asteroidb-cli --host <addr> get <key>
asteroidb-cli --host <addr> put <key> <value>
```
