# 変更履歴

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
