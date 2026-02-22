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

現状の `src/main.rs` は HTTP サーバーを bind せず、NodeRunner を実行する構成です。  
HTTP 層はテスト内でルーターを直接起動して検証しています。

## ドキュメント

- ビジョン: `docs/vision.md`
- 要件定義: `docs/requirements.md`
- 実行・検証手順: `docs/getting-started.md`

## スコープ外

- SQL 互換レイヤ
- フル ACID トランザクション
- 本格的 Byzantine 耐性
- 文章編集系 CRDT (本プロジェクトのスコープ外)
