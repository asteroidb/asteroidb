# AsteroidDB - Agent Guide

This document is the **Single Source of Truth** for AI agents (Claude Code, Codex, etc.) working on this project.
Tool-specific config files (`CLAUDE.md`, `AGENTS.md`) should reference this file rather than duplicating its content.

## Project Overview

AsteroidDB は「整合性レベルの異なるワークロードを単一クラスタで統合運用する」分散 KVS。
地上 DC から衛星コンステレーションまで、同一 control-plane で扱える設計を目指す。
Rust で実装し、MVP は CRDT ベース KVS + Authority 合意による Certified 状態の提供。

**Repository**: `anditdb/asteroids` (GitHub, private)
**Status**: Implementation phase. Tasks are tracked as GitHub Issues.

## Architecture Overview

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

### Key Concepts

| Term | Description |
|------|-------------|
| **Eventual** | 可用性優先。ローカル受理後に伝播し CRDT マージで収束 |
| **Certified** | Authority ノード群の過半数合意で確定。証明付きで取得可能 |
| **Authority ノード群** | キー範囲単位で定義される確定判断ノード集合。MVP は majority |
| **ack_frontier** | 各 Authority が取り込んだ更新の HLC 到達境界。圧縮後も追跡可能 |
| **majority_certificate** | Ed25519 個別署名集約。将来 BLS Threshold へ拡張予定 |
| **配置ポリシー** | タグベース。レプリカ数/必須タグ/禁止タグ/分断時挙動を制御 |
| **system namespace** | control-plane 管理領域。配置ポリシーと Authority 定義を格納 |
| **CRDT** | PN-Counter, OR-Set, OR-Map + LWW-Register (MVP) |

Certification states: `pending` | `certified` | `rejected` | `timeout`

CRDT error codes: `INVALID_ARGUMENT` | `INVALID_OP` | `TYPE_MISMATCH` | `KEY_NOT_FOUND` | `STALE_VERSION` | `POLICY_DENIED` | `TIMEOUT` | `INTERNAL`

## Documentation

- **Vision**: `docs/vision.md` - プロジェクトの方針とスコープ
- **Requirements**: `docs/requirements.md` - MVP 機能要件 FR-001〜FR-010 + 非機能要件

Requirements の FR/NFR 番号をコミットや Issue で参照すること。

## Build & Test

```bash
cargo build                    # ビルド
cargo test                     # 全テスト実行
cargo test --lib               # ライブラリテストのみ
cargo test <module_name>       # 特定モジュールのテスト
cargo clippy -- -D warnings    # lint
cargo fmt --check              # フォーマット確認
```

**CI gate**: `cargo fmt --check && cargo clippy -- -D warnings && cargo test`

## Code Conventions

### Module Structure

```
src/
├── lib.rs              # ライブラリルート
├── main.rs             # バイナリエントリポイント
├── crdt/               # CRDT 実装
│   ├── mod.rs
│   ├── pn_counter.rs
│   ├── or_set.rs
│   ├── or_map.rs
│   └── lww_register.rs
├── store/              # KVS ストレージレイヤ
├── authority/          # Authority 合意・証明
│   ├── ack_frontier.rs
│   └── certificate.rs
├── placement/          # 配置ポリシー・タグ管理
├── api/                # クライアント API
├── hlc.rs              # Hybrid Logical Clock
├── node.rs             # ノード定義
├── error.rs            # 共通エラー型
└── types.rs            # 共通型定義
```

### Rust Style

- Edition 2024
- `cargo fmt` + `cargo clippy -- -D warnings` をパスすること
- エラー型は `thiserror` で定義
- `pub` は最小限。モジュール境界で re-export
- テストは `#[cfg(test)] mod tests` で配置、統合テストは `tests/`
- ドキュメントコメント (`///`) は公開 API に必須（英語）

### Error Handling

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CrdtError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid operation for this CRDT type")]
    InvalidOp,
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    #[error("key not found: {0}")]
    KeyNotFound(String),
    #[error("stale version")]
    StaleVersion,
    #[error("policy denied: {0}")]
    PolicyDenied(String),
    #[error("timeout")]
    Timeout,
    #[error("internal error: {0}")]
    Internal(String),
}
```

## Git Conventions

### Commit Message

```
<type>: <description> (#<issue-number>)
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `ci`

### Branch Naming

```
feat/#<issue-number>-<short-desc>
fix/#<issue-number>-<short-desc>
```

### PR Workflow

- PR 本文に `Closes #<issue-number>` を含める
- CI gate を通すこと

## GitHub Issue 駆動ワークフロー

### Task Management

- 全タスクは GitHub Issue で管理: `gh issue list --repo anditdb/asteroids`
- Issue にはフェーズラベル (`phase:foundation` / `phase:core` / `phase:integration`) とモジュールラベル (`mod:crdt` 等) が付与されている
- 要件トレーサビリティは `FR-0xx` / `NFR-0xx` ラベルで確保

### Agent Workflow

1. `gh issue list` で未着手の Issue を確認
2. Issue を読み、依存関係（Depends on）を確認
3. 依存が解決済みの Issue を選んで着手
4. **作業開始時に必ず worktree を作成して、その中で作業すること**:
   ```bash
   git worktree add .claude/worktrees/<branch-name> -b <branch-name> main
   cd .claude/worktrees/<branch-name>
   ```
5. worktree 内でブランチ `feat/#<number>-<desc>` を使い、実装 + テストを書く
6. worktree 内で CI gate を通して push
7. push 後にレビュー（Claude + Codex の両方で独立レビュー）
8. レビュー指摘を修正してから PR を作成（`Closes #<number>`）

**重要**: メインの作業ツリー（プロジェクトルート）のブランチを直接切り替えたり、そこでコミットしたりしないこと。複数エージェントが並行作業する場合、メインツリーを共有すると git 操作が競合する。必ず worktree を使って隔離すること。

### Review Workflow

PR 作成前に以下の独立レビューを並行実行する:

1. **Claude review** — Agent tool でコードレビュー（型安全性、ロジック整合性、API設計）
2. **Codex review** — `codex review --base origin/main` でレビュー（ランタイム挙動、エッジケース）
3. 両方の指摘を統合し、修正が必要なものを対応
4. 修正完了後に PR を作成

### Parallelizable Areas

| Area | Module | Dependencies |
|------|--------|-------------|
| CRDT 各型 | `src/crdt/` | `error.rs`, `types.rs` |
| HLC | `src/hlc.rs` | なし |
| エラー型 | `src/error.rs` | なし |
| 共通型 | `src/types.rs` | なし |
| 配置ポリシー | `src/placement/` | `types.rs` |
| Authority | `src/authority/` | `hlc.rs`, `types.rs` |

### Coordination Rules

- `error.rs` と `types.rs` は最初に確定させる（他が全部依存）
- CRDT 各型は別エージェントに1型ずつ割当可能
- テストは実装と同時に書く
- `cargo test` が通る状態を常に維持
- モジュール境界の `pub` trait/struct 変更時は依存先の担当に通知

### What to Read Before Starting

1. この `docs/agent.md`
2. `docs/vision.md` - プロジェクトの Why
3. `docs/requirements.md` - MVP 詳細要件
4. 担当 Issue の本文と依存関係

## Key Design Decisions

- CRDT API 命名: `crdt.<type>.<op>` (FR-005)
- 合意条件: majority 固定 (FR-003)
- `certified_write` タイムアウト: `on_timeout=error|pending` (FR-004)
- Compaction: 過半数 Authority 取込済のみ圧縮可、チェックポイント 30s or 10,000 ops (FR-010)
- 署名: Ed25519, keyset_version=1 から単調増加, epoch=24h, 過去7 epoch許容 (FR-008)
- ノードモード: `store` / `subscribe` / `both` (FR-006)

## Dependencies (Planned)

```toml
thiserror = "2"          # エラー型
serde = { version = "1", features = ["derive"] }
serde_json = "1"         # シリアライゼーション
ed25519-dalek = "2"      # Ed25519 署名
uuid = { version = "1", features = ["v4"] }  # 一意識別子
tokio = { version = "1", features = ["full"] }  # async runtime
```
