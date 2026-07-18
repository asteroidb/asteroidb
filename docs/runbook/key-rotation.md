# Key Rotation Runbook

## 重要: 自動鍵ローテーションは未配線（unwired）

コードベースには `EpochManager`（24 時間 epoch / 7 epoch 猶予期間、FR-008）が
存在するが、**production で `EpochManager::stage_keys` を呼ぶ経路が存在しない**
ため、`check_and_rotate` は staged keys が無く常に no-op である。つまり:

- スケジュールされた自動ローテーションは**発火しない**。
- `key_rotation_total` / `key_rotation_last_version` / `key_rotation_last_time_ms`
  メトリクスは**増加せず常に 0**（last_version は初期値のまま）。
- `keyset_version` は初期バージョンのまま固定される。
- 「7 epoch グレース期間による旧バージョンへの自動フォールバック」も
  同様に発生しない。
- epoch カウンタの定期更新（`epoch_check_interval`、60 秒）だけは動作して
  おり、これは証明検証時の keyset 失効判定に使われる（ローテーションは
  発火しない）。

したがって、**鍵の更新手段は `ASTEROIDB_AUTHORITY_KEYS` の再配布 + 再起動のみ**
である。以下の手順がその唯一の運用経路となる。

> **やってはいけないこと**: control-plane の
> `PUT /api/control-plane/authorities` は **Authority ノード集合の更新**であって
> 署名鍵の配布ではない。このエンドポイントで鍵ローテーションは行えない
> （なお、このエンドポイントは現在 Raft リーダー宛てであり、旧
> `approvals` フィールドは受理されるが無視される。
> `docs/ops-guide.md` §14 を参照）。

## 脅威モデル上の注意（鍵配布は透明性レイヤの対象外）

equivocation / split-view 検知（透明性レイヤ）が覆うのは **frontier 報告の
一致性のみ**である。鍵配布・鍵更新そのもの — すなわち各ノードの
`ASTEROIDB_AUTHORITY_KEYS` に何が入るか — は検知の死角であり、異なるノードに
異なるキーセットを配る攻撃（keyset split-view）は検知できない。keyset の
完全性は **env 配布経路（構成管理・シークレット配布）の運用信頼**に依存する。
keyset 履歴への STR（Signed Tree Root）チェーンの導入は次期課題。
詳細は `SECURITY.md` の「既知の制限事項」を参照。

## ローテーションを行うべきタイミング

- 鍵漏洩が疑われる場合（緊急ローテーション）。
- 新しい Authority ノードが参加し、自身の署名鍵エントリを配布する場合。
- コンプライアンス上の定期ローテーション（手動でスケジュールすること —
  上記のとおり自動では発生しない）。

## 手動ローテーション手順

1. **新しい鍵素材の生成**: 対象ノードで新しいシード
   （`ASTEROIDB_BLS_SEED`、16 進 32 バイト）を設定して起動する。Ed25519 /
   BLS 鍵ペアと PoP はシードから導出される。

2. **配布用エントリの収集**: 各ノードは起動ログに自身の配布用エントリを
   出力する:

   ```
   Authority key entry for ASTEROIDB_AUTHORITY_KEYS distribution: <node-id>=<ed25519>/<bls>/<pop>
   ```

   全 Authority ノード分のエントリを収集する。第 3 セグメントは BLS
   Proof-of-Possession（rogue-key 攻撃対策）で、3 セグメント形式が正
   （形式仕様は `docs/ops-guide.md` の `ASTEROIDB_AUTHORITY_KEYS` の項を参照）。

3. **`ASTEROIDB_AUTHORITY_KEYS` の一斉更新**: 全ノードの env を新しい
   エントリ一式（カンマ区切り）へ更新する。漏洩・退役した鍵のエントリは
   このとき**削除**する。

4. **ローリング再起動**: 全ノードを順次再起動して新しい env を反映する。
   バイナリ更新を伴う場合や strict モード
   （`ASTEROIDB_REQUIRE_SIGNED_FRONTIERS=1`）で運用中の場合は、
   `docs/ops-guide.md` の「BLS 鍵配布のローリングアップグレード手順」の
   順序（バイナリ先行 → env 一斉更新）と strict モードの注意に従うこと。

## 検証

自動ローテーションは無いため、`key_rotation_*` メトリクスでの検証は
**できない**（常に 0 のまま）。代わりに以下で確認する:

1. 各ノードの起動ログに、新しい配布用エントリが出力されていること。
2. ピアの署名付き frontier 検証エラー（unknown authority / signature
   verification failed 等の警告）がログに**出ていない**こと。strict モード
   ではこの失敗は certification の停滞として現れる。
3. certified read が引き続き有効な証明を返すこと:

   ```bash
   asteroidb-cli --host seed:3000 get my-certified-key
   ```

## 緊急ローテーション（鍵漏洩時）

手順は上記と同一。加えて:

1. 漏洩した鍵のエントリを `ASTEROIDB_AUTHORITY_KEYS` から**必ず削除**する
   （更新後の env に残さない）。
2. 漏洩ノードの `ASTEROIDB_BLS_SEED` を必ず新しい値に差し替える。
3. 漏洩鍵で equivocation の証拠が記録されている場合は
   `docs/runbook/troubleshooting.md` の「Authority Equivocation Detected」
   の手順（証拠保全）を先に実施する。

## ロールバック

新しいキーセット配布で問題が出た場合は、旧 env（旧エントリ一式）へ戻して
再度ローリング再起動する。「猶予期間内の自動フォールバック」は存在しない —
戻す操作も env 再配布 + 再起動である。
