# 旧 Certified 経路の Authority scope pin

`CertifiedApi` はローカル書き込みを受け付けた時点の Authority 集合を
`PendingWrite` と永続 `CertifiedScopePin` に保持し、その集合の過半数からだけ証明書を発行する。
発行済み証明書は保存中の書き込み HLC と origin policy version に照合して公開する。
後から live な名簿を変更しても、既に発行された書き込み時 scope の証明書は失効しない。

## 永続化済みの範囲

現行 WAL の `CertifiedUpsertScoped` は値と scope pin を一つのレコードで保存する。
旧 `CertifiedUpsert` を含む v5 以前の WAL は引き続き読める。
checkpoint は checksum 付き `certified.scope-pins.bin` と origins sidecar を
一時名で保存し、snapshot 保存後に昇格し、必須 marker を保存してから WAL を prune する。
同じキーへの再書き込み中にクラッシュしても、古い snapshot の pin を上書きせず、
新しい snapshot と古い sidecar が残った場合は WAL から新しい pin を復元する。
新形式の marker がある状態で scope sidecar が欠けたり壊れたりした場合は起動を拒否する。

## 残る境界

複製先では発行元の名簿を復元できない。`CertifiedDeltaEntry` は key、値、HLC、
origin policy version を運ぶが、range と Authority 集合を運ばない。
そのため複製受信時は受信側が観測した名簿を pin し、以後の WAL・checkpoint・再起動で保持する。
これを発行元の書き込み時集合と同一だとは主張しない。
旧 WAL・snapshot など元の名簿を持たない既存データも、名簿を推定で復元できない。

`../aste-lean` の旧証明書モデルは固定 Authority 集合を前提とし、
名簿変更や元の集合を運ぶ protocol をモデル化していない。
ここを修正する際は、受信した集合が実際にその書き込みの scope だったことを
検証できる根拠も必要になる。現在の制御プレーンは policy version の履歴を
持つが、過去の構成員集合を検証できる履歴は持たない。
`PutAuthority` は placement の `PolicyVersion` を変えずに名簿だけを更新できる。
`SystemNamespace.version_history` は版番号のみで、Raft log の古い command は
snapshot compaction で消える。自動生成 Authority の名簿はノードローカルに導出される。
したがって同じ origin policy version でも同じ Authority 集合だったとは判定できない。
Bearer 認証された peer からの roster 申告だけで証明書を発行してはならない。

## 安全な拡張に必要なもの

1. 他ノード由来の scope pin を検証できる信頼済みの過去名簿または
   quorum 署名付き制御プレーン証拠。検証できない pin は認証根拠に使わない。
2. 旧 bincode peer と混同しない version 付き delta 経路。既存の
   `/api/internal/certified/delta` に positional なフィールドを追加しない。

過去名簿を保存するだけでは、送信者が申告した名簿をその書き込みに結び付けられない。
値・HLC・発行時名簿を結ぶ検証可能な発行時証拠も必要である。

既存データに元の名簿がない場合、その情報は推定で復元できない。
移行時の fallback は元の集合の証明とは区別して扱う。
