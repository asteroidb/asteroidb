# AsteroidDB (PoC)

AsteroidDB は、地球規模から宇宙規模の高遅延・断続接続ネットワークを見据えた分散 KVS の PoC です。  
MVP では「`Eventual` と `Certified` を単一クラスタで統合運用する」ことをコアにしています。

## MVP のコア

- CRDT ベースの KVS (`PN-Counter`, `OR-Set`, `OR-Map`, `LWW-Register`)
- `eventual_write` / `get_eventual` による可用性優先フロー
- Authority ノード群の過半数で確定する `certified_write` / `get_certified`
- 認証状態 API (`pending | certified | rejected | timeout`)
- タグベース配置ポリシー (固定階層に依存しない)
- `system namespace` による配置ポリシーと Authority 定義の管理

## クイックスタート

### 前提

- Rust toolchain (Edition 2024 対応)
- Cargo

### ビルドと検証

```bash
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

### CI (GitHub Actions)

- ワークフロー定義: `.github/workflows/ci.yml`
- トリガ: `pull_request (main)`, `push (main)`, `workflow_dispatch`
- 必須ジョブ:
  - `Format Check` (`cargo fmt --all -- --check`)
  - `Clippy Lint` (`cargo clippy --all-targets --all-features -- -D warnings`)
  - `Test` (`cargo test`)
  - `Release Build` (`cargo build --release`)

`main` ブランチでは、上記4ジョブを必須ステータスチェックとして設定してからマージする運用を前提にします。

### ノード実行 (ランループ)

```bash
cargo run
```

`src/main.rs` は NodeRunner を起動し、認証処理・クリーンアップ・compaction 判定のバックグラウンドループを実行します。

### 分断復旧デモ

```bash
cargo run --example demo_partition_recovery
```

ネットワーク分断から復旧までの CRDT 収束と、Authority 合意による Certified 状態遷移を確認できます。

## HTTP API について

HTTP ルーター/ハンドラーは `src/http/` に実装済みです。  
エンドポイント仕様は `docs/getting-started.md` を参照してください。

`cargo run` を実行すると HTTP サーバーが `127.0.0.1:3000` で起動し、NodeRunner のバックグラウンド処理も同時に開始します。
バインドアドレスは環境変数 `ASTEROIDB_BIND_ADDR` で、ノード ID は `ASTEROIDB_NODE_ID` で変更可能です。

## ドキュメント

- ビジョン: `docs/vision.md`
- 要件定義: `docs/requirements.md`
- 実行・検証手順: `docs/getting-started.md`

## スコープ外

- SQL 互換レイヤ
- フル ACID トランザクション
- 本格的 Byzantine 耐性
- 文章編集系 CRDT (本プロジェクトのスコープ外)
