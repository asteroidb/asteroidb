# 変更履歴

## Unreleased

### 機能

- **エンドツーエンド証明書署名パイプライン (FR-008)**: frontier 報告への
  二重署名（報告全体への Ed25519 + チェックポイント HLC への証明書署名）を
  接続し、検証・quorum 処理を強化。証明の受信検証はキーセットレジストリ経由
- **BLS Proof-of-Possession**: BLS 鍵登録時に draft-irtf-cfrg-bls-signature
  §3.3 の PopVerify を必須化し、集約検証への rogue-key 攻撃を遮断
  （`ASTEROIDB_AUTHORITY_KEYS` は 3 セグメント PoP 形式に拡張）
- **Equivocation / split-view 検知**: 署名付き frontier の矛盾ペアを検知し、
  否認不能な証拠を永続化（`equivocation_evidence.json`）・gossip 伝播。
  `GET /api/authority/equivocations` で取得可能。
  ※検知が覆うのは frontier 報告のみで、鍵配布自体の split-view は対象外
  （SECURITY.md「既知の制限事項」参照）
- **セッション保証**: ステートレスなセッショントークンによる
  read-your-writes / monotonic reads（未充足時は `412 SESSION_NOT_SATISFIED`）
- **Write-ahead log とクラッシュリカバリ**: eventual / certified 両ストアの
  WAL（CRC32 フレーム、fail-stop リカバリ、`ASTEROIDB_WAL_RECOVER_TRUNCATE`
  エスケープハッチ）と定期スナップショット
- **Digest ベース anti-entropy**: フル同期フォールバック前に digest 照合で
  差分キーのみを転送し、同期帯域を削減
- **Control plane の内蔵 Raft コンセンサス**: 承認カウント（`approvals`）
  ベースの過半数チェックを廃止し、静的投票者集合
  （`ASTEROIDB_CONTROL_PLANE_NODES`）による Raft ログ複製に置換。書き込みは
  Raft リーダーのみ受理し、他ノードは `503 NOT_LEADER` + リーダーヒント
  ヘッダを返す。`GET /api/control-plane/raft/status` を追加。
  `approvals` フィールドは deprecated（受理されるが無視）

### ドキュメント

- 鍵ローテーション runbook を実態に合わせて全面改訂（自動ローテーションは
  未配線 — 鍵更新は `ASTEROIDB_AUTHORITY_KEYS` 再配布 + 再起動のみ）
- クラッシュリカバリ runbook を改訂（壊れたストアのファイルのみ退避。
  `raft/`・equivocation 証拠・certified ストアはピア再構築不可のため保持）
- Control plane docs を Raft モデル（リーダー宛て + `NOT_LEADER` リトライ）に
  統一

## v0.1.0 (2026-03-08)

初回リリース。

### コア機能

- デュアル整合性モデル: eventual（CRDT ベース）と certified（Authority majority）
- CRDT 型: PnCounter, LWW-Register, OR-Set, OR-Map
- Hybrid Logical Clock (HLC) による因果順序付け
- バッチ処理とバックオフ付き delta ベース anti-entropy sync
- Epoch ベースの鍵ローテーション付き BLS12-381 threshold signatures
- Ed25519/BLS デュアルモード certificate
- タグマッチングとレイテンシ考慮ランキング付き配置ポリシー
- 書き込みレート追跡付き適応型圧縮
- Error budget 計算付き SLO フレームワーク

### 運用

- CLI ツール (asteroidb-cli): status, get, put, metrics, slo
- Docker Compose 3 ノードクラスタ
- Fault injection と netem テストスクリプト
- Criterion マイクロベンチマーク
- マルチノードベンチマークスクリプト

### セキュリティ

- 定数時間 Bearer トークン認証
- Internal エンドポイントの SSRF 保護
- ピアアドレス検証
- Ping アンチポイズニング（既知の送信者 + レートリミット）
