# 操作値の物理回収: 現状と次の実装境界

2026-11-10 の Rust 実装確認。対象は新しい `/api/operations/*` 経路。

## 現状

経路を固定した routed モードでは、`OperationRelay::compact_certified_ledger_bodies` が証明済みで読み取りに冗長な操作の ledger 本体を `OperationReceipt` に置き換える。最終宛先の完了通知と退役境界が確定した packet inbox と outbox のコピーも、別の条件で回収する。しかしこれは、古い値のバイト列がノードの永続領域から消えることを意味しない。non-routed の直接 gossip モードには最終宛先の完了通知と退役境界がなく、受信 custody コピーを保持する。

| 保持先 | 古い値が残る理由 |
| --- | --- |
| `operation_evidence.log` | ledger receipt 化前の証明書は値を含む `BoundOperation` を保持する。receipt 化後、通常サイズの証明書は必要な隣接先の証明書 outbox に完全なコピーを登録し、8 MiB 超なら証明書中の全 support に対する全隣接先の永続 ACK を確認してから compact 証拠へ原子的に書き換える |
| `policy-witness/` | 完全な署名済み Create を初回に検証・永続化する。必要な policy-only 配送の ACK が揃うと、作成者の compact 署名、宣言、commitment、固定した信頼設定を残して値を回収する。後続 Add の Authority admission はこの値なし記録を再検証する |
| support・証明書配送キュー | 未 ACK の送信項目は再送のため完全な操作・証明書を保持する。ACK 済み項目は値なしの識別記録に置き換える |
| `operation-supports.log` | 必要な隣接先の ACK と検証済み compact 証拠が揃うまで操作本体を保持する。条件成立後は receipt・署名付き root・包含証明を残して値を回収する |
| Authority の `operation-roots.log` と `admitted` ledger | 必要な support の ACK と検証済み証拠が揃うまで本体を保持する。条件成立後は発行位置・subject・commitment・署名付き root・包含証明と admission receipt を残す |
| Authority の `signing-inbox/` | 最終宛先の署名処理待ちと完了通知の再送に完全な転送を保持する。ローカル root の発行、検証済み証拠に基づく ledger receipt 化、送信側の退役境界がすべて永続化された後に記録を削除する。転送 ID と内容 commitment は `transfer-catalog/` に残す |
| 作成ノードの `roots/` | 作成ノードが Authority も兼ねる場合、配送完了と ledger receipt だけでは署名仕事が終わったことを示せない。Authority の永続 root log と admitted ledger から発行位置・subject・commitment の一致を再検証できたとき、ローカル root を回収する。再起動時も relay より先に Authority worker が証拠を復元する。root log が欠ける場合は回収済み root の起動を拒否する |
| 中継ノードの custody | routed の純粋中継は application ledger に本体を入れず、署名済み packet と転送記録を保持する。受信時に値なし custody receipt を fsync し、下流の完了通知と上流からの退役境界が確定した後は compact な転送記録を残して packet 本体を回収する。catalog 保存前のクラッシュで ACK されなかった packet は、再起動時に根拠のない orphan として削除する。non-routed の直接 gossip では退役境界がないため、受信 custody コピーを保持する |

従って現在の GC は routed で条件が成立した ledger と配送コピー、および条件が成立した証拠、support、Authority root の値を回収できる。policy-only witness は操作本体の配送が non-routed でも、独立した固定経路の ACK 条件が成立すれば回収できる。ただし、**ノード全体からの値の完全な物理回収ではない**。未完了の配送コピーと non-routed の受信 custody コピーは残る。運用上の容量見積りにこの区別を使う。

## 実装済みの接続と残る境界

1. `OperationReceipt` の操作 ID・内容 commitment・宣言・action kind に対し、Merkle 包含証明と Authority 署名を値本体なしで検証できる形式を定める。証明書の受信時には従来どおり完全な操作を検証し、値を落とすのは証明済み・読み取りに冗長・配送不要を確認した後に限る。
2. 証拠ストアにクラッシュ安全な圧縮記録を追加する。圧縮後も再起動時に信頼済み keyset と scope で証拠を再検証し、同じ ID の異なる内容を拒否する。旧 append log の完全なレコードを残すだけでは物理回収にならないため、ファイル置換とディレクトリ fsync まで含める。
3. 操作 ID 状態の応答に、値本体を含まない証拠形式を追加する。ledger receipt 化後は `compact_certificate` と `operation_receipt` を返し、完全な `certificate` は返さない。提出者が元の内容まで独立に確認するには、保管した署名済みパケットと receipt を照合する必要がある。通常読みと Certified 読みは retained winner・delete とその完全な証拠から計算する。
4. 作成者は宣言と Create 本体の commitment に署名し、後続 Add/Delete はその値なし証拠を引き継ぐ。Create 本体の value・HLC・mode は commitment と照合する。Add/Delete の packet は Create の初期値を含まない。

手順 1 の証拠形式として、`CompactOperationCertificate` は操作 ID・commitment・署名付き root と包含証明だけを持つ。`verify` は、別途認証済みの永続 `OperationReceipt` と一致することを要求し、scope、keyset、全 Authority 署名、`n-f` の quorum を従来の完全な証明書と共通の検証関数で確認する。状態照会と再提出応答は ledger receipt 化後に compact 証拠を返す。証拠ストアは完全な新ログを fsync してから旧ログへ原子的に置き換え、再起動時に compact 証拠を再検証する。圧縮前に現行ログへ保存された完全レコードは読み込める。定期処理は、通常サイズなら必要な隣接先の証明書 outbox 登録を、8 MiB 超なら全 support の個別配送に対する全隣接先の永続 ACK を確認してから証拠ストアを圧縮する。保存済みの完全な証明書から個別配送を再登録できる。

値の回収条件に物理時間や ACK 不在を使わない。必要な証拠と配送義務が残る間は本体を保持し、削除記録・操作 ID・commitment・証明根拠は失わない。

support ストアと Authority root は永続的な値なし記録を持つ。操作 ID と内容 commitment に加え、Authority ごとの support と発行位置を保持する。完全な本体の再送が必要な隣接先には圧縮前に配送キューへの登録と ACK を完了させる。受信済み ACK だけで新しい隣接先への責務が消えるわけではないため、最初の圧縮時に隣接先 manifest を固定する。変更には過去の配送義務を引き継ぐ移行処理が必要。

`OperationSupportStore::compact_receipted_and_sync` は、receipt・署名付き root・包含証明を残して support ログから値を除く永続書き換えを提供する。定期 GC は、ledger receipt と再検証済み compact 証拠が一致し、各 support の現在の必要配送先に対する ACK が永続化されている場合に呼ぶ。最初の圧縮前に neighbor manifest の digest を support ストアへ fsync して固定し、再起動時に配送先・適用範囲の変更を拒否する。経路変更を可能にするには、過去の配送義務を引き継ぐ移行処理が別途必要。

配送キューは受信側 ACK を永続化すると再送を止める。non-routed ではこの ACK、routed ではさらに最終宛先の完了通知を圧縮条件とし、closed index を保存してから、`.send` の署名済み packet と ACK・完了通知の詳細を値なしの同一性記録に圧縮する。未 ACK 項目は完全な再送本体を保持する。起動時と再登録時は値なし記録も照合し、同一 ID の別内容を拒否する。証拠ログの圧縮条件も ACK 済みの値なし配送記録を認める。compact な作成参照を持つ後続操作は、Create 本体または receipt から作成時ポリシーを確認できるまで、ledger 本体と署名済み custody packet の回収対象にしない。回収前に作成根拠を確認した事実は `known-creation-policy/` に fsync してから packet を消す。回収後の状態照会は receipt・圧縮済み packet の同一性記録・この marker を照合する。marker のない圧縮記録は保守的に `PolicyUnknown` とし、元の署名済み packet が同一内容で再提出された場合に作成根拠を再検証して marker だけを復元する。

Authority の署名処理待ち記録は、root 発行だけでは完了通知の再送に必要なので消さない。sender の退役境界で再送義務が終わり、検証済み証拠による ledger receipt と、再起動後も root log から再構成できる同じ操作のローカル発行記録が揃ったときだけ削除する。`signing-inbox/` の削除と packet 回収の間で停止しても、退役済みの catalog と root log が同一性を保持し、再起動時に回収を再試行する。
