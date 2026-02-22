# AsteroidDB 要件定義 (Draft v0.4)

## 1. この文書の目的

本書は AsteroidDB の MVP に向けた要求仕様を定義する。  
対象は「宇宙まで通用する設計思想」を持つ分散 KVS の初期実装である。

## 2. 設計前提

- 初期実装は KVS とする
- 既定の整合性は Eventual Consistency とする
- 一部データに対して Authority ノード群の合意で Certified 状態を提供する
- ノード配置は固定階層ではなくタグベースとする
- CRDT は状態収束のために利用し、文章編集系 CRDT は MVP 対象外とする

## 3. 用語定義

- Authority ノード群: 特定データ範囲の確定判断を行うノード集合
- Certified データ: Authority 合意条件を満たし、確定情報として取得できるデータ
- Eventual データ: ローカル受理を優先し、後で収束するデータ
- 配置ポリシー: データの保持先・複製数・分断時挙動を決めるルール
- ack frontier: 各 Authority が取り込んだ更新時刻 (HLC) の到達境界

## 4. 機能要件 (MVP)

### FR-001: データモデル

- KVS を提供すること
- キー空間はプレフィックスで論理分割できること

### FR-002: 整合性モデル

- 既定の読み書きは Eventual モードで動作すること
- 読み取り API は `get_eventual` と `get_certified` を分離すること
- 合意進捗の確認 API (`get_certification_status`) を提供すること
- `get_certification_status` の返却状態は `pending | certified | rejected | timeout` とすること

### FR-003: Authority ノード群

- Authority はキー範囲単位で定義できること
- 合意条件は過半数 (majority) を MVP の固定条件とすること
- ポリシー更新用に control-plane Authority グループを持てること

### FR-004: 書き込みモード

- ユーザーが書き込み期待値を選べること
- 最低限、以下 2 系統を提供すること
- `eventual_write`: ローカル受理後に伝播
- `certified_write` (strict): Authority 過半数合意後に成功とする
- `eventual_write` + `get_certification_status` の 2 step 運用を可能にすること
- `certified_write` のタイムアウト時応答は API パラメータ `on_timeout=error|pending` で選択可能にすること

### FR-005: CRDT サポート

- 以下の CRDT を MVP 対象として実装すること (暫定)
- Counter: PN-Counter
- Set: OR-Set
- Map: OR-Map + LWW-Register
- MVP の公開 API は型ごと専用 API (`counter_inc`, `orset_add` など) を採用すること
- API 命名規則は `crdt.<type>.<op>` を正規形とすること
- エラー体系は型共通のエラーコードセットを採用すること
- CRDT 共通エラーコードは以下を MVP の最小セットとすること
- `INVALID_ARGUMENT`: 引数不正
- `INVALID_OP`: 型に対して無効な操作
- `TYPE_MISMATCH`: 既存型と操作型の不一致
- `KEY_NOT_FOUND`: 対象キー不存在
- `STALE_VERSION`: 古い文脈/バージョンでの更新
- `POLICY_DENIED`: ポリシー違反による拒否
- `TIMEOUT`: 処理または認証待ちの時間超過
- `INTERNAL`: 内部エラー
- 将来拡張として汎用 op-envelope API (`apply_op(type, op, payload)`) を追加可能にすること

### FR-006: 配置・ローカライゼーション

- ノードには任意タグを付与できること
- 固定階層 (例: Region > DC > Node の強制) を持たないこと
- 階層表現はタグ設計で実現可能であること
- ノード動作モードとして `store` / `subscribe` / `both` を選択可能にすること

### FR-007: 配置ポリシー最小セット

- レプリカ数を指定できること
- 必須タグ/禁止タグを指定できること
- 分断時のローカル書き込み許可を指定できること
- Certified 対象データ範囲を指定できること

### FR-008: 証明情報

- `get_certified` の結果に、合意成立を示す検証可能メタデータを含めること
- MVP は `ack_frontier` を一次実装とすること
- `ack_frontier` は最低限、`authority_id` / `frontier_hlc` / `key_range` / `policy_version` / `digest_hash` を含むこと
- `majority_certificate` は data-plane の `get_certified` から実験導入し、検証後に control-plane へ横展開すること
- MVP の `majority_certificate` は Ed25519 個別署名集約方式を採用すること
- Ed25519 鍵配布は `system namespace` の `keyset_version` 管理で行うこと
- 鍵ローテーションは `epoch` ベースの自動切替をサポートすること
- `keyset_version` の既定初期値は `1` とし、更新ごとに単調増加させること
- `epoch` の既定長は `24h` とし、検証時は現在 epoch に加えて過去 7 epoch 分の鍵を許容すること
- ローテーション手順は `publish(next keyset) -> epoch 切替 -> grace 終了後に旧鍵無効化` を標準とすること
- 将来は Threshold 署名 (BLS 等) へ拡張可能とすること
- 証明対象の粒度はキー範囲単位 (prefix) を MVP とすること

### FR-009: control-plane 管理

- 配置ポリシーと Authority 定義は `system namespace` に格納すること
- `system namespace` の更新は control-plane Authority の合意で確定すること
- 将来拡張はハイブリッド方式 (論理階層パス + タグ) を採用し、大中小クラスタ管理へ対応すること

### FR-010: 圧縮 (compaction)

- CRDT 操作レコードの圧縮条件を定義できること
- MVP の既定は「過半数 Authority 取り込み済み更新のみ圧縮可能」とすること
- 圧縮後も `ack_frontier` を使って更新反映状況を確認できること
- MVP の `digest_hash` 検証は key_range 単位の周期チェックポイント方式を採用すること
- チェックポイント作成トリガは「時間閾値または更新件数閾値」のハイブリッド方式を採用すること
- 将来は操作ログ連鎖ハッシュ (op chain) を併用可能にすること

## 5. 非機能要件 (MVP)

### NFR-001: 分断耐性

- ノード分断時でも Eventual モードの書き込み継続を可能にすること
- 再接続時に CRDT マージで収束できること

### NFR-002: 高遅延耐性

- 高 RTT 環境で操作モードごとのレイテンシ特性が明確であること
- Eventual と Certified の待ち時間差を観測可能にすること
- 長時間分断で一部 Authority が遅延しても compaction が完全停止しないこと

### NFR-003: 故障モデル

- MVP はクラッシュ故障を主対象とする
- Byzantine 耐性は将来フェーズで扱う

### NFR-004: control-plane 可観測性

- ポリシー更新履歴と適用バージョンを追跡できること
- データ plane と control-plane の責務が運用上識別可能であること

## 6. MVP 成功条件

次を満たした場合、MVP を成功とみなす。

- CRDT ベース KVS が動作し、分断後再同期で整合収束する
- Authority 過半数合意による Certified 取得が可能である
- タグベース配置設定により、データの保持先を柔軟に制御できる
- `system namespace` の更新が control-plane Authority 合意で反映される
- `ack_frontier` により、反映状況を確認できる

## 7. スコープ外

- SQL 互換レイヤ
- フル ACID トランザクション
- グローバル最適化された自動配置戦略
- 本格的 Byzantine 合意
- 文章編集系 CRDT

## 8. 未決事項 (次ラウンドで確定)

- `digest_hash` の既定時間閾値/件数閾値と再検証トリガー詳細
