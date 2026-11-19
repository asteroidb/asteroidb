# 作成ポリシーだけを先に届ける経路の設計境界

Lean の `EdgeRouting.Packet.policy` は、認証済みの作成ポリシーを操作本体と独立に配送できる。`EdgePolicy.State.certifiedResult` は、キーの本体がなくても Disabled を学習済みなら `NotTargeted` を返す。Rust は完全な署名済み Create を最初に検証し、専用のリンク認証付き経路で固定宛先へ配送する。配送義務が終われば値なしの認証済み記録を残す。

## 安全な受信根拠

- policy-only packet は完全な署名済み Create と値なしの作成者署名を含む。`SignedBoundOperation::verify` と作成時の admission を通し、Create action、宣言、作成者と操作 writer の署名、namespace 権限を確認する。
- `SignedCreation` の宣言と Create commitment だけでは、Create の値と HLC に一致する本体を確認できない。この証拠だけで mode を学習済みにしない。
- 受信ノードは `namespace/key → creation_id, mode, bound_commitment` を永続化し、既存の Create 本体・receipt、他の policy witness、後続操作の宣言と矛盾する packet を拒否する。

## 配送と再起動

- policy-only の受信と再送には隣接 peer のリンク署名、固定 route manifest、専用の永続 outbox を使う。操作本体の配送方式が non-routed でも policy-only だけ独立した固定経路を設定できる。既存の operation transfer にそのまま載せると最終受け取り先が Create を application ledger に保存するため、policy-only 状態にならない。
- 受信側は検証済みの完全な Create packet を別の `policy-witness/` に fsync してから ACK する。未 ACK の送信記録は再送用の完全な packet を保持する。ACK 済みの送信記録は値なし identity に原子的に置換し、必要な宛先への ACK が揃った後に受信側の witness も値なし記録へ置換する。再起動時は作成者署名、writer 鍵集合、namespace、作成ポリシー、送信先と ACK の固定情報を照合する。
- Certified 読みは本体がなくても witness が Disabled なら `NotTargeted`、Enabled なら `Unknown`。compact Add の作成根拠にも一致する witness を参照する。Authority は自身の admission ledger に Create がなくても、同じ inbox の完全な署名済み witness または初回にそれを検証して残した値なし記録を再検証し、参照 commitment が一致する Add を署名対象にできる。Create witness 自体は Authority ledger へ入れず、署名対象にも増やさない。
- 値なし witness は完全な Create を初回に検証し永続化した inbox だけが構築する。Authority はこの記録を再検証して後続 Add の作成根拠に使う。未 ACK の配送、通常の操作 ledger、他の値保持先はそれぞれ独立の回収条件に従う。

実装済み経路は公開 `POST /api/operations/policy/submit`、内部 `POST /api/internal/operations/policy-witness`、`operation_policy_outbox/` の再送記録である。公開 submit は writer 署名と所有 prefix を検査し、`ASTEROIDB_OPERATION_POLICY_DESTINATIONS` の宛先ごとに送信記録を fsync してから応答する。内部 hop はリンク署名と送受信 ID、固定 route・宛先、Create 署名を検査し、witness を fsync してから ACK する。次 hop の送信登録だけが失敗しても ACK を返し、保存済み witness から稼働中と再起動後の再照合で不足送信を登録する。未 ACK は再起動後も再送する。宛先設定を省いた状態で保存済み policy outbox がある場合、起動を拒否する。

`ASTEROIDB_OPERATION_POLICY_DESTINATIONS` は既存の required destinations と同じ JSON 行形式を使うが、操作本体の配送先とは独立に選ぶ。policy-only 専用の経路は `ASTEROIDB_OPERATION_POLICY_ROUTE_EDGES` と `ASTEROIDB_OPERATION_POLICY_ROUTES` を一緒に設定する。省略時は操作本体の `ASTEROIDB_OPERATION_ROUTES` を使う。non-routed の操作本体と併用する場合は専用経路と `ASTEROIDB_OPERATION_LINK_SEED` / `ASTEROIDB_OPERATION_LINK_KEYS` が必要。全ホップは経路上で rank が減少し、隣接先の `key_prefixes` で許可される必要がある。ACK 前の再送コピーには値が残る。包括的な Lean/Rust refinement と、policy-only 経路を含む end-to-end の無限実行進行性は未証明。
