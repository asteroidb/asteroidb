# コア再設計 確定設計一式(A1〜A4 + 横断統合)

作成: 2026-07-28。HEAD 4b16294(行番号アンカーは f48dc04 時点 — 実装時はシンボル名で
再接地。Phase 0 の D6 が sync.rs / node_runner.rs の push 経路行番号を先にずらすため、
`push_changed_keys` / `send_with_json_fallback` / `gc_gate_diagnose` 等のシンボル参照を正とする)。

前提: `docs/core-redesign-roadmap.md`(継ぎ目裁定・壊すな核・実行順)/
`docs/control-plane-scaleout.md`(C1〜C6)/ `docs/followup-plan.md`(欠陥台帳)。
本 design/ は 8 エージェント編成(並列読解 → 各領域複数案 → 相互採点 → 全主張の実コード
抜き取り検証 → 横断統合)の確定成果物。

**注意: 既存 docs(architecture.md / ops-guide.md / api-reference.md / user-guide.md /
followup-plan.md)の更新は各フェーズの実装時に行う。今回の成果物は design/ のみ。**

## 文書構成

| 文書 | 領域 | フェーズ |
|---|---|---|
| [range-states.md](range-states.md) | A1: S1 range_states() 導出ビュー + シード降格(R1+R2、D1 吸収) | 1a / 2 |
| [membership-epochs.md](membership-epochs.md) | A2: S2 grace 付き GC ピア集合 + scope 付き epoch 所有層(R5、D4 吸収) | 1b / 4 |
| [wire-negotiation.md](wire-negotiation.md) | A3: S5 ワイヤバージョン交渉層(R3、実証不足 #5 内包) | 2 |
| [certified-value-plane.md](certified-value-plane.md) | A4: S3 certified 値プレーン統合 Step1+2(R4、D2/D3/D9 吸収) | 3 |

## 1. 決定サマリ

| 領域 | 採用案 | 捨てた案と理由(要点) |
|---|---|---|
| A1 | 角度 A「最小差分」コミット列 A→E + B から 6 点接ぎ木(PolicyOnly 命名 / allowlist 規約 / gc_gate_diagnose 純関数 / 写像同値テスト / 許可行列・遷移表の一級文書化 / 運用ドリフト明文化)。採点 A=23.5 / B=20.0 | B「4-variant + MembershipSource」: 消費者ゼロのフィールド・毎 tick gauge の 2 系統併存・書換え面の広さ — 過剰設計の歯止めに抵触 |
| A2 | 案 A「node_runner 局所 → 複製コア拡張の二段」+ B から 12 点接ぎ木(decode B 形 / prune 分類語彙 / flush_grace_peer API / 再起動安全性論証 / 残余 D4 窓ピン / clamp+WARN / Departing 規則 / fingerprint 変更検知 / C5 言語化 / PutOwnership 単一コマンド)。採点 A=24 / B=19 | B「宛先据え置き + ns 投影」: relay 盲点残存(roadmap 逸脱)、`ep == joined_epoch` ゲートの曖昧さ、bincode 応答の旧→新復号失敗に無自覚、C2 違反面が近い |
| A3 | 案 A「Content-Type パラメータ交渉 + 受動学習」+ B から 3 点接ぎ木(kill switch / 凍結フィクスチャ全型列挙 / strict 両面ピン)+ ping 相乗りを deferred 記録。採点 A=45/50 / B=40.5/50 | B「hello エンドポイント + instance トークン + features 集合」: 認可面追加・404 雑音・staleness 窓 1h・将来要求先行 |
| A4 | 案 B「certified_write の EventualApi 委譲」+ A から 3 点接ぎ木(precheck 事前拒否 / certified_flow 合成点集約 / 移行由来キー Pending 規則)。採点 A=19.5 / B=23 | A「certified_merge_write を EventualApi に追加」: merge/WAL poison コアに diff(roadmap リスク #1)、Rejected→Certified 反転バグ、移行のクロックシード内部矛盾、不要な tick フック |

## 2. 実行順(横断統合で確定)

roadmap Phase 0〜5 と整合。再配置 2 点を含む確定順:

1. **Phase 0(即時・並行)**: 下記 §3 のパッチ群 + docs 訂正(D10 / user-guide:435)。
2. **Phase 1a**: A1 コミット A〜C(導出ビュー + gc_gate_diagnose + 消費側切替 +
   昇格時 sync client 修正)。**コミット B を Phase 1b より先にマージ**(R-c 合流仕様)。
   ~~コミット D~~ は A4 Step1 に移管(resolve_scope エラー面統一)。
3. **Phase 1b(1a と並行可、ただし B マージ後)**: A2 Phase 1(grace 母集合 +
   decode_response 修正 — 同一レビュー文脈必須)。
4. **Phase 2**: A1 コミット E(シード降格 = R2)/ A3 交渉層(並行可)。
5. **Phase 3**: A4 Step1 → Step2(v1 ワイヤ凍結のまま)→ その後 S4 v2 ワイヤ
   (R3 後、v3 メッセージとして。**Step1 と v2 の同時変更禁止**)。
   **R3⇄R4 の順序は soft**(A4 ワイヤ接触ゼロ — R3 遅延は R4 をブロックしない)。
   hard なのは S4 v2 → R3 のみ。
6. **Phase 4**: A2 Phase 2(R2 完了後 — Bootstrap 経路直列化 D-5)。
7. **Phase 5(研究層)**: 既存 roadmap どおり(本 design/ のスコープ外)。

## 3. Phase 0 パッチ群仕様(統合確定版)

1. **D5 STALE 化**: 対象は `eventual_register_set`(eventual.rs:534-558 の `r.set` :554)と
   `eventual_map_set`(:495 の `m.set`)の bool 破棄のみ + `merge_remote_with_hlc` の
   `let _ = self.clock.update(&hlc)`(:663)への ClockSkew counter 追加。
   **merge を止める変更は禁止**(:658-662 の恒久データ喪失論証を維持)。false 時は
   store/WAL/`finish_local_write` を一切走らせず STALE エラーを返す。
   A4 との整合確認済み: A4 の委譲先は `eventual_write`(merge_value 経路)で typed set
   経路と**書込パスを共有しない**。certified の stale LWW が changed=false の無音 no-op に
   なるのは CRDT merge の正当な意味論であり D5 の対象外(1 行明記)。
2. **D6 checkpoint**: delta push の Err 経路(node_runner.rs:2817-2839)のみ。
   (a) entries は HLC 昇順維持・チャンク境界は順序保存(sync.rs:914-916 は既に順序保存、
   :917 の HashMap 化はチャンク内のみで無害)。
   (b) 戻り値に「**先頭からの連続ゼロエラーチャンク prefix の末尾 HLC 境界**」を追加し、
   Err 時も push_frontiers をそこまで前進。
   (c) **per-key merge エラーのあるチャンク以降は frontier に寄与しない**(連続 prefix
   規則)— A2 の decode B 形(不解読チャンク全キー failed + 残余続行)と組んだとき
   「成功チャンクの最大 HLC まで前進」だと失敗チャンクを恒久スキップする(:2824-2828 の
   警告する正にそのバグ)。連続 prefix 規則がこれを構造的に排除。**この規則は共通仕様
   として本 D6 設計に置き、Phase 1b(A2)が適合実装する。**
   (d) `push_acked_wall_ms` は従来どおりゼロエラー全量完遂(Ok)時のみ(:2810-2815)—
   D6 は証跡意味論(C-2 核)に一切触れない。digest push の evidence_valid
   all-or-nothing(:4449-4463)も非接触。
   (e) 恒久 per-key 失敗(poison 型)が frontier を pin する fail-closed は維持 —
   解消対象は接触窓容量起因の transport 部分失敗 livelock に限定。
3. **D8 counter**: equivocation.rs:337-366 の eviction に counter + 上限設定化。
   eviction 廃止禁止(:334-336)。
4. **`#[must_use]`**: `LwwRegister::set`(lww_register.rs:31)/ `OrMap::set`
   (or_map.rs:114)等の bool 戻り CRDT 変異に付与。src 内の破棄サイトは D5 対象の
   2 箇所のみ(eventual.rs:495,554)— **D5 と同一 PR**(順序: D5 修正 → must_use 付与で
   clippy -D warnings が再発を封じる)。
5. **docs 訂正(D10 + user-guide:435)**: Phase 0 時点では「証明 = 到達性(時計通過)の
   証明、値は単一コピー」という**現状**を記述し Phase 3 を先取りしない(A4 着地時に
   再更新)。A1 の ops-guide「certified 運用開始手順」は Phase 2(コミット E)側の
   同梱物であり Phase 0 に含めない。

decode_response 修正の帰属は Phase 1b のまま(A2 と同一レビュー — push 証拠チェーン
C-2 核)。**supersession**: roadmap Phase 0 節の S6 パッチ群列挙に decode_response が
含まれるが、本統合はこれを **Phase 1b(A2 Phase 1 と同一 PR 群)に確定訂正**する
(二重実装・相互落としの排除。roadmap 本体の追随更新は実装時)。「total_pushed 経由で証跡前進しないか」は検証済み: 証跡前進は Ok 腕
(:2814-2815)と digest 経路(:4414/:4444)のみで Err 腕に scan_wall_ms 挿入は無い。

## 4. scope 語彙の共有(横断統合の確定)

- range キーの正準型 = `KeyRange { prefix: String }`(types.rs:16-18)。**適用先は
  所有軸(A2 OwnershipState)と新設の永続構造**。導出ビュー(A1 の `range_states()` /
  `range_state_for_key`)は既存 2 マップ(`HashMap<String,_>`)整合のため String prefix
  キーを維持する(意図的逸脱として一本化 — 実装時の型選択で迷わない)。key→range 解決は
  `get_authorities_for_key` の最長一致(system_namespace.rs:123-129)唯一で、A1 の
  `range_state_for_key` がこれを包む。**A4 は最長一致を再実装しない(明文条件)。**
- 1 つの prefix キー上に **3 つの直交軸を統合しない**:
  準備状態軸(`RangeState`、A1・導出・非永続・C5 定義点)/ 証明軸(`FrontierScope`、
  ack_frontier.rs:44 既存・無変更)/ 所有軸(`OwnershipState`、A2 Phase 2・Raft 複製・
  **RangeState の variant にしない**)。
- 液性ドメイン(A2 grace 集合・A3 WireCapCache)は peer addr キーのままで正しい
  (到達性の簿記であり安全性母集合ではない。C1 の適用先は所有軸のみ)。
- A2 Phase 2 の証跡は**記録時に per-scope epoch スナップショットを刻み**、ゲート評価は
  `evidence.epoch_at[s] >= member.admitted_epoch[s]`(グローバル epoch 1 本は C1 違反)。

## 5. C1〜C6 適合宣言

| 制約 | A1 | A2 | A3 | A4 |
|---|---|---|---|---|
| C1 scope 付きキー | 対象外 | Phase 2 per-scope epoch + 証跡刻印で適合 | キャッシュは液性ドメインで対象外 | 対象外 |
| C2 単一版数非依存 | 2 マップ読取のみ・bump 非呼出 | fingerprint に per-scope epoch 混合(ns.version() 非依存) | 無関係 | 無関係 |
| C3 voter 静的原則 | 非接触 | 静的維持・能力ラッチも静的集合が母集合 | static_peers 不変 | 非接触 |
| C4 交渉層の上 | ワイヤゼロ | Phase 2 Ownership 版 vN 形は A3 上(Phase 4 > 2。vN は strict マーカー v2 と別採番 — membership-epochs.md §3 の版番号規律)。flush_grace_peer は JSON 管理 API で対象外(明記条件) | 交渉層そのもの | ワイヤゼロ |
| C5 唯一の定義点 | 定義点を創設 | ownership view は兄弟アクセサ・readiness 非寄与 | 無関係 | precheck/status とも range_states() 消費 |
| C6 Bootstrap ルート専用 | シード降格で排除 | OwnershipState は複製対象として「載る」が初期化は PutOwnership(相乗りさせない) | 非接触 | 非接触 |

## 6. 壊すな核チェックリスト(全 PR 共通)

**無修正パス必須の検証資産**(シグネチャ機械追随以外の変更は PR で拒否):
- [ ] 静止テスト 4 本(tests/delta_sync.rs:741, 828, 884, 1065)
- [ ] golden digest(digest.rs:1052-1128、scheme v2 凍結)
- [ ] property テスト(property_quorum / CRDT changed 契約)
- [ ] gc ゲート単体(node_runner.rs:8118-8229)

**非接触を確認する核**(挑戦は反証付き明示提案のみ・本設計群では発生しない):
- [ ] gc_authority_gate_passed / gc_peer_gate_passed の判定式(母集合供給側のみ変更可)
- [ ] C-2 証跡意味論(push_acked_wall_ms = ゼロエラー完遂 push のみ / pull を push 証拠に
      使わない / データ HLC と壁時計を比較しない)
- [ ] RR ゲート 3 条件 / MergeEffects.changed 契約 / 単一 origin タグ再導入の恒久禁止
- [ ] WAL poison 順序不変条件・merge-dominance 規律
- [ ] M-12 report clock floor / silence / 実行時昇格
- [ ] attestation 版ウィンドウ(LAG=2/LEAD=1)+ M-4 pool 上限
- [ ] fail-closed 揮発性(再起動で Certified→Pending)
- [ ] Bootstrap 冪等 reset-and-import + version_floor
- [ ] Raft 投票者静的設定(gossip/epoch 追従の恒久禁止)
- [ ] membership.rs SSRF/アドレス検証群
- [ ] 永続層バージョン体系 / digest scheme_ok / JSON 外部 API / M-14 observed レーン
- [ ] equivocation eviction-not-rejection 設計

## 7. 設計間相互作用の裁定(要点)

- **R-a**: A4 のワイヤ接触ゼロ → R3⇄R4 順序は soft。
- **R-b**: WireCapCache は恒久 addr キー。A2 の vN(Ownership 版)ラッチは static_peers で
  addr 解決 → 照会、解決不能/未学習 voter は「未 vN」= ラッチ不成立(fail-closed)。
  ラッチ条件は strict マーカー v2 ではなく必ず vN(membership-epochs.md §3)。
- **R-c**: A1 コミット B(gc_gate_diagnose 切り出し)を先にマージ、A2 Phase 1 は
  peers 引数の供給側のみ(diagnose 本体に触らない)— 両設計の正式合流仕様。
- **R-d**: grace 宛先拡張 × A3 受動学習は無害(応答が無い限り昇格しない)。
- **R-e**: シード刈り後の "" range は A4 precheck で PolicyDenied — ops-guide の決定的
  手順(certified 運用開始は certified policy か SetAuthorityDefinition API 経由)と
  A4 の「certified 書込には Active range が必要」を同一節に束ねる(実装時更新)。
- **R-f**: 行番号ドリフト — 全文書はシンボル名参照を正とする(冒頭注記)。
