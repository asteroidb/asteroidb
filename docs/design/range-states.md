# A1: S1 range_states() 導出ビュー + catch-all シード降格(確定設計)

> **注記(2026-07-30)**: 本設計は `core-semantics-v2.md` §7/§8 により**存続**(文書
> スリム化予定)。`gc_gate_diagnose` は v2 §5.1 の資源圧メトリクス・ゲート阻止診断の
> 実装点として拡張される。GC ゲートの母集合(名簿 ∪ roster)・要求ベクトル(remover
> 集合)・mark の時刻表現(単調時計化)は v2 §3.5 の改訂が優先し、本文 §3 のシグネチャ
> (`mark_ms` / `peers` / `push_acked_wall_ms`)は v1 時点の形である(実装時は v2 準拠で
> 再接地)。

対象: R1 + R2(roadmap Phase 1a / Phase 2)。吸収する欠陥: D1(GC 無音永久停止)、
check_compaction の同型無音停止(未収載潜在)、実証不足 #2 前半(本番同等シード GC 収束 e2e)。

行番号は f48dc04 時点。実装時は必ずシンボル名(`gc_gates_passed` / `gc_authority_gate_passed` /
`check_compaction` / `run_frontier_gc` / `discover_scopes` / `attestation_admissible` /
`resolve_scope` / `build_bootstrap_command` / `recalculate_authorities`)で再接地すること
(Phase 0 の D6 パッチが node_runner.rs / sync.rs の行番号を先にずらす)。

## 1. 決定

range の準備状態(authority 定義と placement policy の合成)を、`SystemNamespace` 上の
**単一の導出アクセサ `range_states()`**(2 マップから毎回導出・非永続・C5 の唯一の定義点)に
集約し、消費側の個別再計算をこのアクセサ経由に切替える(数え方の明示: §4.1 行列は 9 行
だが、本設計のコミット B/C で切替えるのは 6 箇所 — discover_scopes / attestation_admissible /
gc_gates_passed 母集合 / check_compaction / run_frontier_gc / cap-pressure sweep。
resolve_scope と A4 precheck の 2 者は A4 側成果物で切替(§8)、メンバーシップ判定は
§4.3 の allowlist 裁定に従う)。同時に main.rs の
catch-all シード(prefix ""、policy 無し)を `auto_generated: true` に降格し、
`build_bootstrap_command`(raft/node.rs:989-994)の既存フィルタで Bootstrap 複製から外す。
実装は挙動温存の段階コミット列(A→E)で行い、ワイヤ・永続形式変更ゼロ、判定式
(`gc_authority_gate_passed` / `gc_peer_gate_passed`)は不変に保つ。

## 2. 却下案と理由

- **案 B「4-variant enum + MembershipSource + PolicyOnly{certified} フィールド」**:
  概念が重い(certified フィールドと MembershipSource は現時点で消費者ゼロ)、
  attestation_admissible / resolve_scope の書換えで同値写像の検証面が広い、毎 tick 更新
  gauge が既存の「swept 時のみ更新」規律(node_runner.rs:3552-3559)と 2 系統併存になる。
  過剰設計の歯止め(将来要求のみを理由にした構造追加禁止)に抵触。
  ただし B の語彙(variant 命名)・非消費者 allowlist 規約・`gc_gate_diagnose` 純関数化・
  写像同値テスト・許可行列文書は本設計に接ぎ木済み(§3, §4, §7)。
- **シード完全廃止(ASTEROIDB_AUTHORITY_NODES からの明示生成のみ)**: 降格
  (auto_generated:true)で Bootstrap 非複製化と recalc による自然な刈り取りが得られ、
  廃止と同等の効果を旧デプロイ互換を保ったまま達成できるため、より大きい変更は不要。
- **RangeState への ownership variant 追加(S2 Phase 2 先取り)**: 横断統合の裁定により
  所有軸(`OwnershipState`)は独立軸として分離(readiness に寄与しない)。variant 追加はしない。
- **`range_state_for_key` の A4 への遅延(採らない)**: 唯一の消費者(A4 precheck)は
  Phase 3 まで現れず、案 B 却下基準(消費者ゼロの構造追加禁止)と表面上緊張するが、
  本関数は `get_authorities_for_key` を包むだけの薄いラッパ(検証面ゼロ)であり、
  「A4 は最長一致を再実装しない」(横断統合の明文条件)を型で固定するための例外として
  コミット A に残す — 却下基準との差分は「新規構造・新規検証面の無さ」。

## 3. 型・シグネチャ

```rust
// src/control_plane/system_namespace.rs(SystemNamespace のメソッドとして追加)

/// range の準備状態。2 マップ(authority_definitions / placement_policies)から
/// 毎回導出する。永続化しない。C5: これが「準備状態」の唯一の定義点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeState {
    /// placement policy のみ存在し authority 定義が無い。
    /// (旧称 Unconfigured — 「policy のみ」を正しく表すため改名)
    PolicyOnly { policy_version: PolicyVersion },
    /// authority 定義のみ存在し policy が無い(catch-all シード・手動定義の中間状態)。
    /// メンバーシップにのみ寄与し、報告・受理・GC ゲート対象にならない。
    AuthorityOnly { members: Vec<NodeId> },
    /// def ∧ policy が揃った稼働状態。報告・受理・GC/compaction ゲートの母集合。
    Active { members: Vec<NodeId>, policy_version: PolicyVersion },
}

impl SystemNamespace {
    /// 全 range の導出ビュー(prefix → RangeState)。
    pub fn range_states(&self) -> BTreeMap<String, RangeState>;
    /// 単一 range の導出。完全不在は None(4 つ目の variant は作らない)。
    pub fn range_state(&self, prefix: &str) -> Option<RangeState>;
    /// key → scope 解決の一本化: get_authorities_for_key(最長一致, :123-129)を包み、
    /// (prefix, RangeState) を返す。A4 certified_flow の precheck はこれを消費する
    /// (A4 側で最長一致を再実装しない — 横断統合の明文条件)。
    pub fn range_state_for_key(&self, key: &str) -> Option<(String, RangeState)>;
}
```

注意: 両マップは `HashMap`(system_namespace.rs:38-39)であり順序保証は無い。
`range_states()` は BTreeMap に詰め直して決定的順序を与える(WARN/テストの安定化のため)。

```rust
// src/runtime/node_runner.rs — GC ゲート診断の純関数化(コミット B)

/// gc ゲート阻止理由。counter/WARN の供給源。
pub(crate) enum GcGateBlock {
    AuthorityOnlyScopeInPopulation { prefix: String },   // 旧: v1 捏造で待たされていた scope
    FrontierMissing { prefix: String, authority: NodeId },
    FrontierBehindMark { prefix: String, authority: NodeId },
    ReportNotAdvanced { prefix: String, authority: NodeId },
    PeerEvidenceMissingOrStale { peer_addr: String },
}

/// 判定の純関数。gc_authority_gate_passed / gc_peer_gate_passed(:3657-3705)を
/// 呼び出す形で包み、判定式そのものは変更しない(壊すな核 (2))。
/// None = 全ゲート通過。Some = 最初の(または全ての)阻止理由。
fn gc_gate_diagnose(
    defs: &ActiveScopeSet,            // range_states() から導出した Active のみの母集合
    versions: &HashMap<String, PolicyVersion>,
    frontier_set: &FrontierSet,
    peers: &[PeerConfig],             // Phase 1b(A2)はこの引数の供給側だけを差し替える
    push_acked_wall_ms: &HashMap<String, u64>,
    mark_ms: u64,
) -> Option<GcGateBlock>;
```

横断統合 R-c の正式仕様: **A1 コミット B(`gc_gate_diagnose` 切り出し)を先にマージし、
A2 Phase 1 は呼び出し側で `peers` 引数に grace 母集合を渡すだけ**(diagnose 本体に触らない)。

可観測化(S6 テンプレート準拠、metrics.rs の AtomicU64 counter 群と同型):

```rust
pub gc_gate_blocked_authority_total: AtomicU64,  // tick 単位で +1
pub gc_gate_blocked_peer_total: AtomicU64,       // tick 単位で +1
```

WARN は 600 秒スロットルで 1 本、GcGateBlock の詳細(理由種別 + scope/peer)を
フィールドに載せる。counter は tick 単位(ラベル無し)— 既存 metrics.rs にラベル付き
counter の前例が無く、粒度は WARN 側が担う(未決点 (2) の裁定)。ops-guide 12.3 には
WARN のフィールド(reason/prefix/authority/peer)で調査する手順を記載(実装時更新)。

## 4. 意味論(消費側ごとの表)

### 4.1 許可行列(状態 × 消費者)

| 消費者(シンボル) | PolicyOnly | AuthorityOnly | Active | 完全不在(None) |
|---|---|---|---|---|
| frontier_reporter `discover_scopes` | 報告しない | 報告しない(現行 :199-205 の除外と同値) | 報告する | — |
| `attestation_admissible`(certified.rs:204-) | UnknownRange 拒否(現行同値 — def 不在チェック :214-216 が NoPolicy 判定 :220-222 より**先**に評価される。counter 上は両者とも attestation_rejected_unknown_range_total に集約 :293-295) | NoPolicy 拒否(現行同値) | 版ウィンドウ判定へ | UnknownRange 拒否 |
| `gc_gates_passed` 母集合(:3603-) | 対象外 | **対象外(変更点: 現行は v1 捏造で AND 要求 = D1)** | 対象 | — |
| `check_compaction`(:3309-) | 対象外 | **対象外(変更点: 同型解消)** | 対象 | — |
| `run_frontier_gc`(:4608-) | スキップ | スキップ(現行 if-let と同値) | 対象 | — |
| cap-pressure sweep(certified.rs:1089-1099) | 対象外 | 対象外 | 対象 | — |
| `resolve_scope`(certified.rs:491-)= 書込許可 | PolicyDenied | PolicyDenied(エラー面統一は A4 に委譲、§8) | 許可 | PolicyDenied |
| メンバーシップ判定(is_definition_member 系) | 非メンバー | **メンバー(AuthorityOnly はメンバーシップにのみ寄与)** | メンバー | 非メンバー |
| A4 certified_flow precheck(Phase 3) | PolicyDenied | PolicyDenied | 許可 | PolicyDenied |

挙動変更は太字 2 行(GC/compaction 母集合からの AuthorityOnly 除外)のみ。
他は現行挙動の一点定義化(同値リファクタ)。

### 4.2 遷移表(T1〜T7)

| # | 遷移 | 契機 | 効果 |
|---|---|---|---|
| T1 | None → AuthorityOnly | SetAuthorityDefinition(policy 無し) | メンバーシップのみ発生。GC 母集合に入らない |
| T2 | None → PolicyOnly | certified policy 作成(def 未生成の瞬間) | recalc tick で T4 へ |
| T3 | AuthorityOnly → Active | policy 作成 + recalc / SetPolicy | 報告・受理・ゲート対象化。**昇格ノードは frontier_sync_client 再構築 + init_report_floor(§6 コミット C の 3 行修正)** |
| T4 | PolicyOnly → Active | recalculate_authorities が auto def 生成 | 同上 |
| T5 | Active → AuthorityOnly | policy 削除(manual def 残存) | 報告停止・ゲート母集合から即時除外(D1 の恒久解消点) |
| T6 | Active → PolicyOnly / None | auto def が policy 消失で刈られる(:192-203) | 降格シードもこの経路で消滅(初回 recalc tick、sentinel :780 で強制) |
| T7 | AuthorityOnly → None | def 削除 | メンバーシップ喪失 |

この表と 4.1 の行列は `system_namespace.rs` の導出規則 doc コメントにも採録すること。

### 4.3 非消費者 allowlist(C5 規律)

`all_authority_definitions()` の直接使用は以下 5 者に限定し、PR レビュー時に grep で強制
(実 grep 接地済み — 導入初日から偽陽性/偽陰性ゼロで敷ける):

1. `recalculate_authorities` 内部(system_namespace.rs)
2. `build_bootstrap_command`(raft/node.rs:990)
3. `system_namespace.rs` 内部(導出の実装自身とテスト)
4. `list_authorities`(handlers.rs:992 — GET /api/control-plane/authorities)。
   生 def の列挙・診断 API であり準備状態を解釈しないため許容(分類根拠付き)。
5. main.rs:568 の observer-authority lifeline 警告(M-17)。policy 無し def も意図的に
   含める def-only メンバーシップ判定(§4.1 メンバーシップ判定行の消費者)であり、
   AuthorityOnly を含む現行挙動が正 — **allowlist 入りを明示裁定**(range_states() 消費への
   切替は同値だが、起動時 1 回の診断であり必須としない)。

旧案の allowlist 1 号 `detect_version_changes` は**削除(訂正)**: 実装は
`snapshot_policy_versions(&ns)` のみを読み `all_authority_definitions` を呼ばない
(node_runner.rs:1274-1279 — fence ポーリングは policy スナップショットのみで def
生アクセサ非使用)。残る grep ヒット(node_runner.rs:3331/3611/4623 =
gc/compaction/frontier-GC、frontier_reporter.rs:198、certified.rs:1091)は全て
コミット B/C の切替対象であり、切替完了後の非 allowlist ヒットはゼロになる。

これ以外の新規消費者は `range_states()` / `range_state_for_key()` を使うこと。
生アクセサ退行はコンパイラで防げないため、この regressions は
`grep -rn "all_authority_definitions" src/ | grep -v <allowlist>` を CI 前レビュー項目にする。

## 5. コミット列(A→E)

- **A**: `RangeState` enum + 3 アクセサの純追加(消費者ゼロ、挙動変更ゼロ)。
- **B**: `gc_gate_diagnose` 純関数の切り出し + gc(`gc_gates_passed`)/ compaction
  (`check_compaction`)の母集合を Active のみに切替(D1 と check_compaction 同型の
  解消点は B で完結)+ counter 2 種 + 600s WARN。`run_frontier_gc` の切替は C に一本化
  (二重帰属の解消)。**このコミットを A2 Phase 1 より先にマージする(R-c)。**
- **C**: reporter(discover_scopes)/ admission(attestation_admissible 前段)/
  cap-pressure sweep / run_frontier_gc を range_states() 消費に切替(同値)+
  **T3/T4 昇格時に frontier_sync_client を再構築する 3 行修正**
  (node_runner.rs:1662-1692 の昇格パスに現在欠落 — A のシードライフサイクルでは
  load-bearing の実在欠陥)。
- **D**: **本エリアでは実施しない。** resolve_scope のエラー面統一
  (InvalidArgument→PolicyDenied)は A4 Step1 の certified_flow 創設コミットに畳む
  (横断統合の裁定 §3。src 2 箇所 + doc 2 箇所のみ、tests に現存せず)。
- **E**: main.rs:371-378 のシードを `auto_generated: true` に降格 + e2e(§7)+
  シード刈り時の一度きり INFO ログ + system_namespace.rs:1067 付近のテストコメント
  「like main.rs does on startup」の機械的追随修正(未決点 (5) の裁定: E に同梱)。
  **E = R2 は Phase 2 の成果物**であり Phase 1a(A〜C)と分割納品する。

## 6. 移行手順

- ワイヤ変更ゼロ・永続形式変更ゼロ・`bump_version` 非呼出。
- R2 差分は main.rs 1 語(`auto_generated: false` → `true`)。混在・ダウングレード全方向:
  - 旧リーダー + 新ノード: 旧リーダーの Bootstrap は manual "" def を複製するが、
    導出ビュー上 AuthorityOnly として無害(GC 母集合に入らない)。
  - 新リーダー + 旧ノード: Bootstrap の auto フィルタ(raft/node.rs:989-994)により
    旧ノードのシードも reset-and-import で除去され、旧バイナリの D1 も空母集合で消える。
  - ダウングレード: 旧バイナリは "" def を再シードするだけで、永続形式は同一。
- 運用ドリフトの明文化: 降格シードは初回 recalc tick(数秒、sentinel :780 が強制)で
  刈られるため、「"" に非 certified policy を置いて即 certify」という旧経路は
  一 tick の競合窓を残して消滅する。ops-guide に決定的手順
  「**certified 運用開始は certified policy 経由か SetAuthorityDefinition API 経由**」で
  上書きする(実装時更新。A4 の「certified 書込には Active range が必要」と同一節に束ねる
  — 横断統合 R-e)。

## 7. テスト計画

**無修正パス必須(壊すな核の検証資産)**: 静止テスト 4 本(tests/delta_sync.rs:741,828,884,1065)/
golden digest(digest.rs:1052-1128)/ property テスト / gc ゲート単体(node_runner.rs:8118-8229
— 純関数呼び出し形のため母集合引数の機械的追随以外は無修正)/
frontier_reporter の policy-less 除外テスト(:548 系)/ admission テスト群。

**新規**:

1. **受け入れ e2e(RED-first)**: `tests/gc_convergence_seed.rs` 新設 — 本番同等シード
   (catch-all "" 含む fresh boot)構成で GC sweep が収束することを固定(実証不足 #2 前半)。
   現行コードでは D1 により RED になることを先に確認してからコミット B を当てる。
2. **gc_gate_diagnose 単体**: 凍結ゲートテスト(:8118-8229)と同一フィクスチャで
   GcGateBlock の理由 5 種を全列挙テスト化。
3. **写像同値回帰ピン**: attestation_admissible の 5 拒否種別
   (UnknownRange / NotRangeAuthority(certified.rs:217-219)/ NoPolicy / 版ウィンドウ /
   fenced)× RangeState 各状態で、切替前後の同値をピン(コミット C の供給側切替に適用。
   PolicyOnly は UnknownRange 拒否 — §4.1 の評価順どおり)。
4. **昇格時 frontier_sync_client 回帰テスト**(未決点 (4) の裁定: テストを置く)—
   T3 昇格後に sync client が再構築され frontier push が始まることを固定。
   「シード刈り一 tick 競合窓」は docs 記載のみ(1. の e2e が実質カバー)。
5. **シード非複製 e2e**: 新リーダー Bootstrap 後、フォロワーに "" def が存在しないこと。
6. **旧形式 namespace 後方互換 e2e**: manual "" def 残存の永続 namespace を読み込み、
   AuthorityOnly として無害化される(GC が停止しない)こと。

## 8. 壊すな核との接点

| 核 | 接触 |
|---|---|
| 判定式(gc_authority_gate_passed / gc_peer_gate_passed の二条件・push 証拠限定) | 非接触(母集合の供給側のみ変更、diagnose は判定式を包むだけ) |
| fence/unfence + detect_version_changes 版遷移 | 非接触(実装は `snapshot_policy_versions` のみを読み、def 生アクセサ非使用 — node_runner.rs:1274-1279。§4.3 の訂正参照) |
| attestation_admissible 版ウィンドウ(LAG=2/LEAD=1)+ M-4 pool 上限 | 非接触(前段の母集合判定のみ切替、同値ピン付き) |
| M-12 floor / silence / 実行時昇格 | 非接触。降格時 floor 保持(:1686-1691)+ 昇格時 init_report_floor 再実行(:1679-1685)により降格シード刈りでも安全論証維持(pruning 例外不要) |
| Bootstrap 冪等 reset-and-import + version_floor | 非接触(auto フィルタは既存挙動の利用) |

C1〜C6: C1/C3 対象外、C2 適合(2 マップ読取のみ・bump 非呼出)、C4 ワイヤゼロ、
C5 定義点を創設(本設計そのもの)、C6 シード降格で Bootstrap から排除。

## 9. 未決点(実装時判断でよいもの)

1. WARN に載せる GcGateBlock を「最初の 1 件」にするか「全列挙」にするか(ログ量との
   トレードオフ。推奨: 種別ごと先頭 1 件、計 5 行以内)。
2. `ActiveScopeSet` の具体型(Vec か BTreeMap の型エイリアスか)— gc ゲート単体テストの
   フィクスチャ構築が最少 churn になる形を選ぶ。
3. コミット A/B を同一 PR にするか分離するか(B が RED-first e2e を GREEN 化する構成なら
   同一 PR が自然)。
