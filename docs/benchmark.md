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

## Criterion Benchmarks (Micro-benchmarks)

In addition to the high-level benchmarks above, AsteroidDB includes Criterion
micro-benchmarks covering CRDT operations, store operations, certification
paths, and cryptographic signatures.

### Running Criterion Benchmarks Locally

```bash
# Run all Criterion benchmarks
cargo bench

# Run a specific benchmark suite
cargo bench --bench crdt_bench
cargo bench --bench store_bench
cargo bench --bench certified_bench
cargo bench --bench signature_bench

# Run the sync comparison benchmark (custom harness)
cargo bench --bench sync_benchmark

# Save a named baseline for later comparison
cargo bench -- --save-baseline my-baseline

# Compare against a saved baseline
cargo bench -- --baseline my-baseline
```

### Available Benchmark Suites

| Suite | File | What it measures |
|-------|------|------------------|
| `crdt_bench` | `benches/crdt_bench.rs` | PnCounter, OrSet, OrMap, LwwRegister operations and merges |
| `store_bench` | `benches/store_bench.rs` | Store put/get, entries_since, snapshot save/load |
| `certified_bench` | `benches/certified_bench.rs` | Certified write, process_certifications, proof verification |
| `signature_bench` | `benches/signature_bench.rs` | BLS vs Ed25519 keygen/sign/verify, aggregate operations, DualModeCertificate |
| `sync_benchmark` | `benches/sync_benchmark.rs` | Full sync vs delta sync payload size comparison |

### Comparing Two Runs Manually

Use `scripts/bench-compare.sh` to compare two sets of Criterion results:

```bash
# 1. Run benchmarks with a baseline name
cargo bench -- --save-baseline before

# 2. Make your changes, then run again
cargo bench -- --save-baseline after

# 3. Compare the two
bash scripts/bench-compare.sh \
  target/criterion   \  # baseline (uses 'before' data)
  target/criterion      # current  (uses latest run data)
```

The script flags any benchmark that regressed by more than 10% (configurable
via `BENCH_REGRESSION_THRESHOLD` environment variable).

## CI Benchmark Pipeline

The project runs automated benchmark regression detection via GitHub Actions.

### Schedule

- **Weekly**: Every Monday at 04:00 UTC (cron schedule)
- **Manual**: Can be triggered via `workflow_dispatch` in the Actions tab

### How It Works

1. The workflow (`.github/workflows/benchmark.yml`) runs all Criterion
   benchmark suites on a fresh `ubuntu-latest` runner.
2. Results are saved as GitHub Actions artifacts with 90-day retention.
3. On subsequent runs, the workflow downloads the previous run's artifact and
   compares using `scripts/bench-compare.sh`.
4. A summary table is posted to the GitHub Actions step summary showing
   baseline vs current timings and percentage change.
5. Any benchmark that regressed by more than 10% is flagged with a warning
   annotation on the workflow run.

### Reading CI Results

1. Go to **Actions** > **Benchmark Regression Check** in the GitHub UI.
2. Open the latest run and check the **step summary** for the comparison table.
3. Download the `benchmark-results` artifact for raw Criterion data.
4. Download the `benchmark-comparison` artifact for the full comparison report.

### Triggering a Manual Run

```bash
# Via GitHub CLI
gh workflow run benchmark.yml
```

## Programmatic Access
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
