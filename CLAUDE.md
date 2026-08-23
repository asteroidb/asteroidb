# AsteroidDB — Claude 作業ガイド

単一クラスタ運用前提の、高遅延・分断環境向けデュアル整合性(Eventual / Certified)分散 KVS(Rust)。
設計の一次情報は `docs/`(architecture.md, requirements.md, vision.md)と、別リポジトリ `../research`(論文サーベイと各トピックの「AsteroidDB への示唆」)。

## 開発ワークフロー規約(重要)

- **実装作業は ultracode(Workflow ツールによるマルチエージェント編成)で行う。** main agent(あなた)は
  オーケストレーションとレビュー・検証・報告に徹し、**プロダクションコードを直接 Edit/Write しない。**
  過去に main agent が直接編集して品質が落ちた経緯があるため、これは意図的なルール。
- 各機能・各修正は、ワークフロー内で「並列読解 → 複数案設計 → 実装 → 多レンズレビュー →
  敵対的検証 → 修正 → コミット」の順で進める。実装/修正エージェントには
  `cargo fmt` / `cargo build --all-targets` / `cargo clippy --all-targets -- -D warnings` /
  `cargo test`(default と `--no-default-features --features native-tls,native-storage` の両方)を
  完走させ、全通過を要求すること。
- 例外: このガイドやハンドオフ文書のような **メタ文書の作成・軽微な調査・git 操作**は main agent が直接行ってよい。
  ルールが守るのは「プロダクションコードの実装」であって、文書やオーケストレーションではない。

## ビルド環境の注意

- openssl が見つからない場合は
  `PKG_CONFIG_PATH=/nix/store/5iga06c6w0pb6p1srgkxzjvrzgf93j1y-openssl-3.6.3-dev/lib/pkgconfig`
  を設定(必要なら `OPENSSL_DIR` / `OPENSSL_LIB_DIR` / `OPENSSL_INCLUDE_DIR` も同ストアの openssl 3.6.3 に)。
  nix store の更新でパスが変わることがある。見つからなければ `ls -d /nix/store/*openssl*-dev` で現行を探すこと。
- feature: `native-crypto`(BLS/Ed25519 実体、無効時は stub)、`native-storage`(redb)、`native-tls`、`wasm`。
  変更は `--features wasm` の `cargo check` も壊さないこと。

## 現在の状況とフォローアップ

`../research` の示唆に基づく7機能(署名パイプライン / BLS PoP / equivocation 検知 / セッション保証 /
WAL / digest anti-entropy / Raft control plane)と、そのマージ前レビュー指摘の修正が **PR #339 で main にマージ済み**。
残タスク(マージ後対応の major と minor 群)は **`docs/followup-plan.md`** に一覧化してある。
