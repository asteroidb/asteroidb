# 設計 v2 — 原則の規範化と証明意味論の再設計

作成: 2026-07-30。入力: `docs/design/_review-notes.md`(問題台帳、git 未追跡の作業メモ)。
本文書は v1 設計群(`README.md` / `membership-epochs.md` / `wire-negotiation.md` /
`certified-value-plane.md` / `range-states.md`、コミット 06b2df1)を**減算的に**見直した
上位文書である。v1 との優先関係は §8 の supersession 表が定める。

本文書の要主張は執筆前に実コードで再検証済み:
`report_frontiers_at` が全 scope に**報告時の時計読み値**を同一スタンプすること
(frontier_reporter.rs — 報告値の出所は `now`)、証明書署名が
`(key_range, checkpoint, policy_version)` のみを覆い digest を含まないこと +
checkpoint = 1000ms 床丸め(frontier_sig.rs / certificate.rs)、認証判定
(`is_certified_at_for_scope`、整列 k 番目)と証明書組立(attestation_pool の
バケット走査)が**別述語**であること、`Store::applied_origins` の不変条件
「origin o の HLC ≤ h の**全**書込の効果を含む」(kv.rs)。

---

## 1. 規範原則(台帳 §0 の採用)

> **沈黙は情報ではない。物理時間は「いつ仕事をするか」を決めてよいが、
> 「何が真か・何が安全か」を決めてはならない。**

真偽・安全の判定に入ってよいのは、各ノードのローカルな論理時系列(単調カウンタ、
per-origin 位置、dot、floor)と、それらを転送・統合して得られる証拠のみ。

**判定基準**(新規コード・設計レビューで機械的に適用する):

- その wall-clock 値を**別の値に変えたとき、安全性が変わるか?** 変わるなら違反
  (安全性が時間に依存している)。スループット・レイテンシ・リソース使用量だけが
  変わるなら適法(スケジューリング)。
- **応答の不在を根拠に、待つ相手の集合を狭めていないか?** 狭めていれば違反
  (分断環境では沈黙が常態)。集合の変更は明示的操作か、保守方向の自動化
  (追加)のみ。

**例外台帳**(原則と衝突するが意図的に残すもの。追加には本表の更新を必須とする):

| 例外 | 理由 | 扱い |
|---|---|---|
| LWW-Register の「後勝ち」 | 物理時間の意味論をユーザーが実際に欲しがる唯一の場所 | プロダクト仕様として明示。docs にスキュー幅の警告を残す(暗黙にしない) |
| HLC スキュー拒否(60s)/ attestation 遠未来拒否 | HLC の物理成分が自ら開けた穴への蓋(外部アンカー防御) | HLC を使い続ける限り維持。§3 のバケット削除で attestation 側の攻撃面は縮小 |
| 能力プローブ系 TTL(digest 非対応キャッシュ等) | 「いつ再試行するか」= スケジューリング。誤りの代償はフォールバック 1 往復のみで安全性非接触 | 適法と分類(v1 A3 の「TTL 却下」との見かけの矛盾はこの分類で解消 — A3 が却下したのは能力の**真偽**を TTL で失効させる案) |
| 保持期間の下限(gc_retention 等)・報告間隔・スロットル | 仕事の頻度 | 適法。ただし「窓を超えたら安全側の集合から外す」用途への転用は違反(§4) |

---

## 2. 現状の帰結(何が壊れているか — 台帳 §1 の確定)

certified の証明書が実際に主張しているのは
**「この区画の権威名簿の過半数が、このポリシー版のもとで、このチェックポイント
(1 秒グリッド)に署名した」**だけである。

- フロンティア報告値はデータと無関係に前進する(時計読み値)→ 「過半数の frontier が
  書込 ts を超えた」=「過半数の時計が経過した」。
- 証明書署名は digest を含まない → 報告署名の内容束縛は equivocation 検知にしか
  効いていない。
- 値は複製されない(D2)、status はキー名引きで値と突き合わない(D3)。

さらにこの構造は台帳 §5 の丸め負債(二重述語、バケット攻撃面と上限群、
checkpoint 遅延、つまみの結合、後追い証明書埋め機構)を**すべて自分で生んでいる**。
docs の文言修正(到達性証明への限定)は正直化ではあるが、未踏提案・requirements の
中核主張(「取り込んだ更新の到達境界」「過半数が受信・適用済み」)を満たさない。
**修正すべきは文言ではなく意味論である。**

---

## 3. 中核再設計: coverage 証明(証明意味論の再接地)

### 3.1 新しい意味論

書込 w の識別を(origin ノード o、その書込に writer が発行した HLC t)とする。

> **certified(w) ⟺ 区画の権威名簿の過半数の authority a について
> `a.applied_origins[o] >= t` が、a の署名済み報告で確認できる**

`applied_origins` は既存のセッション保証の per-origin applied frontier
(kv.rs、不変条件「origin o の HLC ≤ h の全書込の効果を含む」、max-monotone、
証明可能に完全な転送でのみ前進)である。**新しい格子は作らない。**
この系で唯一スカラー壁時計だったフロンティアを、既に 3 箇所(dot / floor /
セッショントークン)で使われている per-origin 語彙に揃える(台帳 §2.2 の逆転の解消)。

性質:

- **データ依存**: applied_origins は完全転送の証拠でのみ前進する。時計経過では
  1 ミリも動かない。
- **落とす向きが保守側**: 前進しない誤り(false Pending)はあっても、
  持っていないのに持っていると言う誤りは構造的に無い(台帳 §2.1 の解消)。
- **兼務の解消**(台帳 §2.0): 証明の仕事は「特定の 1 書込 (o,t) が覆われているか」
  であり、per-origin 位置の点比較で足りる — 全体要約(スカラー)は不要。
  要約を必要とするのは GC 側だけになり、そちらは署名も検証可能性も要らない
  (§3.5)。⚔T2 の単一パラメータ結合が外れる。
- 範囲外の書込まで含意する(applied_origins は store 全体の不変条件)が、
  これは保守方向の過剰であり健全(範囲別に弱める最適化は不要)。

### 3.2 報告とワイヤ

報告は「scope ごとの時計値」から「**authority の per-origin applied frontier の
署名付きスナップショット + store root digest**」に変わる。

- 形: `{ authority, origins: {origin → hlc}, digest, report_seq, policy_versions }`。
  エントリ数は origin 数 = クラスタのノード数オーダ(セッショントークンと同形。
  上限・間引きはトークンと同じ規律 — 間引きは false-Pending 方向にしか効かない)。
- `report_seq` は authority ローカルの単調カウンタ。equivocation の識別を
  「同一 (authority, report_seq) で内容が異なる」に再定義し、frontier_hlc 同一性への
  依存(1 秒丸めの副産物)を外す。観測保持も「authority ごと最新 K 件」の件数ベースに
  変更可能になる(台帳 §3.5 の 120s 窓の処分)。
- ワイヤは既存 FrontierPushRequest への **append-only フィールド追加 + serde(default)**
  で運べる(実績ある機構)。新フレーミングは不要 — これが v1 A3 Stage 1-3 を
  カットできる根拠でもある(§7)。
- 旧報告(時計 basis)と新報告(coverage basis)の区別は新フィールドの有無そのもの。
  **混在期、旧形式の報告は coverage に数えない(fail-closed)** — 全 authority が
  更新されるまで新規 certify は保留される。これは「正しくない証明を発行し続ける」
  より正しい。ローリング手順を ops-guide に明記する。

### 3.3 証明書 = 報告署名の集合(グリッドとバケットの削除)

報告署名は**すでに報告の全内容(digest 含む)を覆っている**。証明書はこれを使う:

> **certificate(w) = { (report_a, sig_a) : a ∈ 名簿, report_a が w を覆う }
> が過半数に達した集合**

- クライアント検証: 各報告の署名検証 + 「origins[o] >= t」の包含確認 + 名簿過半数の
  数え上げ。証明書が主張することと検証されることが一致する(§1.2 の解消)。
- **checkpoint grid(1000ms 床丸め)は削除**: 全員が同一バイト列に署名する必要が
  なくなるため。台帳 §3.3 / §5.2 / §5.3 が消える。
- **attestation pool のバケット構造は削除**: 「w を覆う報告を authority ごとに最新 1 件
  持つ」だけでよい(フロンティアは単調)。遠未来バケット flood の攻撃面と、それを
  塞ぐための上限群・刈り取り論証(台帳 §5.4)が機構ごと消える。admission ゲート
  (M-4 の名簿・版ウィンドウ検査)は残る — これは資源保護であり時間依存ではない。
- **過半数述語は一本になる**: 「覆う報告が過半数あるか」。判定と証明書組立が同じ
  述語になり、後追い埋め機構(控えキー集合・後続 tick 組み直し)が不要になる
  (台帳 §5.1 の解消)。
- **BLS**: fast_aggregate_verify(同一メッセージ前提)は使えなくなる。異メッセージの
  一般 aggregate(blst の AggregateVerify、n ペアリング)に切替 — 署名サイズの利得は
  維持、検証コストは名簿サイズ線形(名簿は小さい前提で許容)。PoP 要件は不変。

### 3.4 遅延特性の正直な変化

現行: certify までの時間 ≈ 報告間隔(データが届いていなくても)。
v2: certify までの時間 ≈ **writer → authority の完全転送 + 次の報告**。

- applied_origins はクレーム付きの完全 pull でのみ前進する(push はクレーム不能 —
  C-2 の教訓による意図的設計)。したがって認証進捗は anti-entropy の完全サイクルに
  律速される。これは「正直さの対価」であり、隠さず docs の遅延特性(NFR-002)に載せる。
- スケジューリング側の最適化は自由: certification worker が pending write の writer に
  対する pull を**誘発**する(authority 側の仕事の前倒し — 原則上完全に適法)。
  これで健全経路のまま実効遅延を現行に近づけられる。

### 3.5 GC ゲートへの波及

GC の authority ゲートは現在「全 Active range の authority の(時計)frontier が
mark を追い越したか」を見ている。v2 では:

- **authority ゲート**: mark 時点の store の per-origin 位置ベクトル(mark スナップ
  ショットに既にある情報)に対し、「過半数 — FR-010 の規定どおり — の authority の
  applied_origins が全 origin で mark 位置以上」で判定。時計比較が消える。
- **peer ゲート**(C-2)は既に証拠ベース(完全 push の証跡)であり意味論不変。
  ただし mark と証跡の**時刻表現**を wall-clock ms からローカル単調時計
  (`Instant` 系 / sweep round 番号)に置換し、NTP 補正での後方ジャンプを閉じる
  (台帳 §3.5 最終行)。C-2 の「データ HLC と壁時計を比較しない」規律は
  「データ HLC とローカル単調時刻を比較しない」としてそのまま維持。

### 3.6 D3 / 値プレーン(v1 A4)への波及

- A4 の骨子(値プレーンの eventual 相乗り、EventualApi 委譲、eventual.rs diff ゼロ、
  移行手順)は**そのまま生きる**。D2/D9 の解はこれで変わらない。
- Step2 の status 導出だけが変わる: 「stored_ts ≤ majority frontier」比較を
  「tracked (o,t) の coverage 判定」に置換。導出マトリクス(A4 §4.2)の行 5 の条件が
  coverage に差し替わるだけで、行 1-4(NotTracked / 非 Active / 移行キー Pending /
  Rejected 優先)と行 6-7 は不変。proof の ts 束縛(偽 Certified の構造排除)も不変。

### 3.7 やらないこと(このスコープの外)

- **値ハッシュ per-write ack(FastPay 型)**: coverage 証明は「クラッシュ故障の正直な
  authority が値を保持している」ことまでを束縛する。**内容の暗号束縛は report digest
  経由の間接束縛**(digest は store 全体のルート)に留まり、authority が嘘をつく場合の
  防御は従来どおり Byzantine フェーズ。CFT の範囲で意味論を正直にするのが v2 の目標。
- **フロンティア圧縮方式の発明**(BVV 等): 台帳 §2.0 の予言どおり、兼務を解いたら
  証明側に要約は不要になった。GC 側の要約は per-origin ベクトルのままでよい
  (ノード数オーダ)。圧縮は問題が実測されるまで設計しない。

---

## 4. 時間依存の処分表(台帳 §3 の全項目)

| 箇所 | 分類 | 処分 |
|---|---|---|
| フロンティア = 時計読み値 | **違反(最深)** | §3 で意味論ごと再設計 |
| membership evict 30-45s → GC 母集合(D4) | **違反** | §5 の明示 roster(evict は安全性に触れなくなる) |
| grace 窓(v1 A2 Phase 1) | **違反(同じ推論の 10 分延長)** | **廃止**。v1 文書の自認(「窓超は現行と同じ」)をもって撤回 |
| checkpoint 1s 丸め | 違反(量子化で合意を購入) | §3.3 で機構ごと削除 |
| 鍵 24h epoch × 7 猶予 | **違反(8 日分断で全拒否)+ 未配線の危険機構** | 未配線のローテーション機構(`check_and_rotate` / `stage_keys` / `rotate_keyset`)を**削除**。運用は現行の明示再配布(env + 再起動)を正とし docs 化。将来の自動回転は「新鍵の行き渡りを証拠(coverage と同形)で確認してから旧鍵無効化」を要件として別設計 |
| 証明の Timeout が状態遷移 | 違反 | Timeout を**保存状態から削除**し、API の応答整形(クライアント期限)に降格。`on_timeout` パラメータの意味論は不変(待つのをやめるのはクライアントの自由 = スケジューリング)。pool 衛生は件数上限のみ |
| fence 後の時間ベース掃除 | 違反 | 「全 authority が新版 basis で報告済み」の証拠ベースに置換(保持上限は衛生として残す) |
| equivocation 120s 窓 | 違反 | report_seq 導入(§3.2)+ authority ごと件数ベース保持へ |
| digest 非対応キャッシュ TTL 10 分 | **適法(スケジューリング)** | 維持。例外台帳に分類根拠を記載(§1) |
| GC mark の wall-clock ms | 形は適法・実装が脆弱 | ローカル単調時計へ置換(§3.5) |
| HLC スキュー拒否 60s | 例外(外部アンカー) | 維持(例外台帳) |
| LWW 後勝ち | 例外(プロダクト仕様) | 明示化(例外台帳) |

---

## 5. メンバーシップ v2: 明示 roster(v1 A2 の全面差し替え)

### 5.1 設計

GC・certification の安全性が参照する母集合を、**明示的に管理される data-node roster**
に一本化する。

- **roster** = `{node_id → addr}` の flat 集合。制御プレーン複製コアの第 4 対象
  (v1 OwnershipState から epoch / 4 状態ライフサイクル / per-scope 化 / vN ラッチ /
  証跡刻印を**全部落とした**もの)。
- **追加は保守方向なので自動でよい**: 未知ノードとの初回接触で自動登録
  (追加は GC を「より多く待たせる」方向 = 安全)。明示 API でも可。
- **削除は明示のみ**: `decommission(node_id)` 運用 API。ping 失敗・gossip evict・
  時間経過は roster に**一切影響しない**。dead peer は「オペレータが decommission する
  まで GC が停止する」— これは正しい fail-closed であり、Phase 0 の可観測化
  (ゲート阻止理由の counter/WARN)が「気づける」ことを担保する。
- gossip / ping は到達性(アドレス鮮度・宛先選択・RTT)専用に戻る。
  D4 は「45 秒 vs 10 分」の調整ではなく、**液性が安全性に触れる経路の切断**で根絶。
- 証跡キーの addr → node_id 化(v1 A2 の妥当な部分)は維持。
- decode_response の fail-open 修正(不解読チャンク全キー failed + 続行)は不変。

### 5.2 v1 からの削減

| v1 A2 の要素 | v2 の扱い | 理由 |
|---|---|---|
| grace 付き GC ピア集合(Phase 1 全体) | **廃止** | 原則違反(§4)。roster が同じ欠陥をより単純に閉じる |
| OwnershipState(per-scope epoch、Joined/Active/Departing/Departed) | **廃止 → flat roster** | per-scope epoch は実障害でなく制約文書(旧 C1)由来(台帳 §8.2)。scope 粒度は必要になった時に足す(追加は後方互換) |
| vN 能力ラッチ + v1 凍結ミラー | **廃止** | roster は append-only フィールドで運べる形に設計する(新バイト形を作らないことを設計制約にする)。Raft snapshot 型への追加は serde(default) の実績機構 |
| 証跡への per-scope epoch 刻印 | **廃止** | 消えた概念への防御 |
| decommission API | **維持**(roster 削除操作として) | dead-peer runbook の置換に必要 |
| flush_grace_peer API | **廃止** | grace が無い |

roster 変更は Raft 経由 = 分断少数派では削除操作が不可。**これは正しい**
(生死不明のノードを安全性母集合から外す操作は、過半数の合意がある側でのみ
行われるべき)。

---

## 6. リーダーと north star の再検討(台帳 §4)

- **合意が本当に必要なのは「roster と policy/authority 定義の変更」だけ**であり、
  これは authority 名簿の split-brain(二重名簿での二重 certify)を防ぐという
  安全性要件から来る。ここに選出リーダー(Raft)を使うのは妥当。ただし:
  - 読みは結果整合であることを docs で正確に表現する(「CP」の一語で済ませない)。
  - 「ポリシーを変えたいのは分断の最中」への答えは「分断中に変えられないのが安全」
    ではなく「**分断前に分断時挙動をポリシーとして設定しておく**」
    (allow_local_write_on_partition 等)であることを user-guide の設計思想として明記。
- **north star(Scatter 型 range 委譲)は撤回し、研究オプションに降格する。**
  リーダーを N 個に増やす方向は「この系はリーダーレスが強み」という原則と逆行する
  (台帳 §4.4)。スケールアウトの再定義:
  1. 合意フットプリントを「まれにしか変わらない小さな状態」に閉じ込め続ける
     (roster + policy のみ。データ・証明・GC は既にリーダーレス)。
  2. データプレーンの部分複製(配置ポリシーが実際に複製先を絞る配線)。
  3. certification は per-range 名簿 + 報告集めで既にリーダーレスにスケールする。
- `control-plane-scaleout.md` の制約群の改訂: **C1(per-scope epoch キー)は削除**
  (委譲前提の前払いだった)。C2(単一版数への新規依存を増やさない)・C3(voter 静的)・
  C5(準備状態の唯一定義点)・C6(Bootstrap ルート専用)は原則の系として存続。
  C4(交渉層の上)は「新バイト形を作らず append-only で設計せよ。できない場合に
  初めて交渉層を設計する」に緩和。
- BFT 非対称(voter 1 台侵害で全署名投資が迂回される)は CFT スコープの受容として
  docs に明記済みの方針を維持しつつ、**token secure-by-default**(未設定時の internal
  API 拒否/大警告)を近接パッチ群に昇格する(数行級、followup「再考すべき受容済み限界」)。

---

## 7. 減算(cut list)と設計プロセスの修正(台帳 §8)

| 対象 | 裁定 | 再着火条件 |
|---|---|---|
| v1 A2 Phase 1(grace)+ Phase 2(OwnershipState) | **カット → §5 の roster に置換** | — |
| v1 A3 Stage 1-3(v= 交渉・strict・受動学習) | **カット**。Stage 0(fallback 3 重複製の共有モジュール統合 = 純減)のみ実施対象として残す | 「append-only + serde(default) で表現できない新バイト形」が実際に必要になった時。§3.2 の新報告も roster も append-only で設計するため、既知の需要は無い |
| v1 A1(range_states) | **存続・文書スリム化**: enum + 単一アクセサ + 挙動変更 2 行(GC/compaction 母集合)+ シード降格が本体。許可行列・遷移表は system_namespace.rs の doc コメントに移し、設計文書は決定と変更点のみに縮約 | — |
| v1 A4(値プレーン統合) | **存続**: Step2 の判定基準のみ §3.6 で差し替え | — |
| Phase 0 パッチ群(D5/D6/D8/decode/docs) | **存続**(部分実装は stash に退避済み)。docs 訂正は本文書の意味論と整合させて再確認 | — |
| checkpoint grid / attestation pool バケット / 後追い証明書埋め / 未配線鍵ローテーション | **削除対象**(§3, §4) | — |
| 記法 | 新規文書では R-x/C-x/I-x/T-x/Stage/Step の英数記法を増やさない。欠陥台帳(D 番号)と平叙のフェーズ名のみ | — |

プロセスの教訓: v1 は「勝者 + N 点接ぎ木」の全会一致加算で削除圧力を欠いた。
以後の設計は**「何を消すか」を必須成果物にする**(本文書の形式を踏襲)。

---

## 8. supersession 表(v1 文書との優先関係)

| v1 文書 | 状態 |
|---|---|
| `README.md`(v1 決定サマリ) | §2(実行順)・§3(Phase 0 仕様)は有効。A2/A3 行と C1 参照は本文書が上書き |
| `range-states.md` | 有効(スリム化予定 — §7) |
| `membership-epochs.md` | **全面 superseded**(§5)。decode 修正・addr→node_id 化・decommission の 3 要素のみ v2 に引き継ぎ |
| `wire-negotiation.md` | Stage 0 のみ有効。Stage 1-3 は superseded(§7) |
| `certified-value-plane.md` | 有効。ただし §4.2 行 5 の判定基準を本文書 §3.6 が差し替え |
| `../control-plane-scaleout.md` | north star 節と C1 を本文書 §6 が改訂(同文書に改訂を反映済み) |

## 9. 実行順 v2(設計のみ — 実装は保留中)

1. **Phase 0**(退避済み部分実装の再開 + D6 + ゲート/レビュー): 意味論に依存しない
   欠陥修正群。
2. **range 準備状態**(v1 A1 縮約版): D1 の恒久解消。
3. **明示 roster**(§5): D4 根絶 + decode 修正。
4. **値プレーン統合**(v1 A4): D2/D9。
5. **coverage 証明**(§3): 本丸。報告形式・証明書・述語一本化・グリッド/バケット削除。
   4 の後(status 導出が共有 store 前提のため)。
6. 時間依存の残処分(§4 の Timeout / fence / equivocation 窓 / 単調時計化)と
   鍵ローテーション機構の削除は 1〜5 の合間に独立パッチとして混ぜられる。

## 10. 未決点(次の設計ラウンドで)

1. 報告の origins map の間引き規律(トークンの 64 上限と同値でよいか、authority 報告は
   全量必須とするか)。
2. 混在期 fail-closed の運用緩和(旧 basis 報告に「レガシー扱いの明示フラグ」を残し、
   ops が移行完了を確認する手順)。
3. coverage 判定の対象 origin が roster から decommission された場合の証明書の意味
   (名簿は書込時点の policy 版に束縛されるので、版ウィンドウ規律で自然に閉じるはず —
   要確認)。
4. GC authority ゲートの「mark 位置ベクトル」の具体形(mark スナップショットの
   per-origin max で足りるか、tombstone の再スタンプとの相互作用)。
5. BLS AggregateVerify の実測コスト(名簿サイズの現実的上限の確認)。
