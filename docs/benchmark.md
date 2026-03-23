# AsteroidDB ベンチマークガイド

## 概要

AsteroidDB のベンチマークは、通常運用時と障害回復時のシステム動作を特徴づける
3 つの主要パフォーマンス指標を計測します:

| # | メトリクス | 計測内容 |
|---|----------|---------|
| 1 | **Eventual write レイテンシ** | 単一の `eventual_counter_inc` 操作をローカルで受理するまでの時間 |
| 2 | **Certified 確定時間** | `certified_write` から majority frontier 進行による `Certified` ステータスまでのエンドツーエンド時間 |
| 3 | **回復収束時間** | パーティション回復後、3 つの乖離ノードが CRDT マージにより同一状態に到達するまでの時間 |

## ベンチマークの実行

### 前提条件

- Rust toolchain (edition 2024)
- リポジトリのクローンと依存関係の解決 (`cargo build`)

### 実行方法

```bash
# ベンチマークを実行（進捗は stderr に、JSON は stdout に出力）
cargo run --example benchmark

# JSON 結果をファイルに保存
cargo run --example benchmark > results.json

# より実際的な数値のためにリリースモードで実行
cargo run --release --example benchmark > results.json
```

### 出力

ベンチマークは進捗と CSV サマリーを **stderr** に、JSON 配列の結果を **stdout** に出力します。

エントリごとの JSON スキーマ:

```json
{
  "name": "eventual_write_latency",
  "iterations": 1000,
  "mean_us": 1.23,
  "p50_us": 1.10,
  "p95_us": 2.50,
  "p99_us": 4.00,
  "min_us": 0.80,
  "max_us": 15.00
}
```

## メトリクスの詳細

### 1. Eventual Write レイテンシ

- **操作**: 異なるキーに対する `EventualApi::eventual_counter_inc`
- **反復回数**: 1000
- **計測内容**: 単一のローカル CRDT 書き込みのウォールクロック時間（ネットワークなし）
- **関連要件**: FR-002/FR-004 -- eventual 整合性パスの基本コスト

### 2. Certified 確定時間

- **操作**: `CertifiedApi::certified_write` の後、2-of-3 Authority の
  frontier 更新と `process_certifications` を実行
- **反復回数**: 100
- **計測内容**: 完全な certification ラウンドトリップ（書き込み + frontier sync + ステータス確認）
- **関連要件**: FR-003/FR-004 -- majority consensus 確認に要する時間

### 3. 回復収束時間

- **操作**: 乖離した PN-Counter 状態を持つ 3 ノードパーティションシナリオ、
  その後のフル CRDT マージ伝播
- **反復回数**: 100
- **計測内容**: マージ伝播開始から 3 ノードすべてが同一状態を保持するまでの
  ウォールクロック時間
- **関連要件**: FR-002/NFR -- ネットワークパーティション後の CRDT 収束保証のデモンストレーション

## 結果記録テンプレート

以下の表をコピーして計測値を記入してください。再現性のため、
コミットハッシュとハードウェアの説明を含めてください。

```
## ベンチマーク結果

**日付**: YYYY-MM-DD
**コミット**: <hash>
**ハードウェア**: <CPU / RAM / OS>
**ビルドモード**: release | debug

| メトリクス                     | 反復回数 | 平均 (us) | P50 (us) | P95 (us) | P99 (us) | 最小 (us) | 最大 (us) |
|-------------------------------|---------|-----------|----------|----------|----------|----------|----------|
| eventual_write_latency        |         |           |          |          |          |          |          |
| certified_confirmation_time   |         |           |          |          |          |          |          |
| recovery_convergence_time     |         |           |          |          |          |          |          |

### 備考

- （異常値、環境固有の事情等を記載）
```

## 結果の再現

1. 対象コミットをチェックアウト:
   ```bash
   git checkout <commit-hash>
   ```

2. リリースモードでビルド:
   ```bash
   cargo build --release --example benchmark
   ```

3. ベンチマークを実行して結果を保存:
   ```bash
   cargo run --release --example benchmark > results.json 2> benchmark.log
   ```

4. ログから CSV を抽出:
   ```bash
   grep -A4 "Results (CSV)" benchmark.log
   ```

5. JSON 出力を使って以前の実行結果と比較:
   ```bash
   # 例: jq で平均レイテンシを比較
   jq '.[].mean_us' results.json
   ```

## プログラムからのアクセス

メトリクスモジュール (`src/ops/metrics.rs`) は以下を公開しています:

- `BenchmarkResult` -- すべての統計情報を含むシリアライズ可能な構造体
- `collect_latencies(name, &[Duration]) -> BenchmarkResult` -- 生の Duration から統計を計算
- `to_csv_row(&BenchmarkResult) -> String` -- CSV フォーマット
- `csv_header() -> &str` -- 対応する CSV ヘッダー

これらは統合テストやカスタムベンチマークで使用できます:

```rust
use std::time::{Duration, Instant};
use asteroidb_poc::ops::metrics::{collect_latencies, BenchmarkResult};

let mut durations = Vec::new();
for _ in 0..100 {
    let start = Instant::now();
    // ... 計測対象の操作 ...
    durations.push(start.elapsed());
}
let result: BenchmarkResult = collect_latencies("my_benchmark", &durations);
println!("{}", serde_json::to_string_pretty(&result).unwrap());
```
