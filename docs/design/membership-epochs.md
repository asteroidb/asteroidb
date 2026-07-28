# A2: S2 メンバーシップ — grace 付き GC ピア集合(Phase 1)+ scope 付き epoch 所有層(Phase 2)(確定設計)

対象: R5(roadmap Phase 1b / Phase 4)。吸収する欠陥: D4(分断 30-45s で生存 peer の
live dot を hole-jump が破壊)、decode_response の fail-open(S6 未起票)、dead-peer GC 停止の
runbook 依存、addr キー証跡の宙吊り、cluster_nodes/registry/静的 voter の三重ソース。

行番号は f48dc04 時点。シンボル(`gc_gates_passed` / `gc_peer_gate_passed` /
`gc_inbound_gate_passed` / `decode_response` / `push_changed_keys` / `remove_peer` /
`recalculate_authorities`)で再接地すること(Phase 0 D6 が push 経路の行番号を先にずらす)。

**Phase 1 と Phase 2 は別フェーズ・別 PR 群。Phase 1 は Raft に一切触らない。**

---

## Phase 1: grace 付き GC ピア集合(node_runner 局所、D4 即応)

### 1. 決定

gossip evict(membership.rs:100 `MAX_PING_FAILURES=3`、:348-380 `remove_peer` 完全消去)から
GC 安全性母集合を切り離す。node_runner 内に **grace 付き GC ピア集合**を追加し、evict された
peer も grace 窓(既定 `2 × gc_retention`、下限 `gc_retention` 固定)内は
(a) `gc_gate_diagnose` の `peers` 引数の母集合、(b) `gc_inbound_gate_passed`(:3589)の母集合、
(c) 証跡 map 8 種(prune :3227-3246 の対象)の保持母集合、(d) sync/relay/frontier push の
**宛先**母集合に残す。宛先拡張は roadmap Phase 5(CT gossip)の前提「Phase 1b の relay 盲点
解消」に対応し、evict 中の生存 peer への証跡再生成を可能にする。同時に
`decode_response` の fail-open(sync.rs:929-936 — 2xx ボディ不解読で error_keys 空 →
push 証跡偽前進)を「不解読チャンク全キー failed + 残余バッチ続行」に修正する
(同一レビュー文脈必須 — roadmap D-6)。

### 2. 却下案と理由

- **案 B「宛先据え置き・ゲート母集合のみ grace」**: relay 盲点(evict 中の生存 peer が
  split-view 中継の盲点)が Phase 2 まで残存し、roadmap の Phase 1b 帰属に逸脱。
  evict 中の生存 peer への証跡再生成もできない。
- **decode 修正 A 形「不解読チャンクで残余バッチ abort」**: 既存の per-key エラー前例
  (sync.rs:944-949)に不整合で、分断環境での伝播を止める。merge は CRDT 冪等なので
  続行は無害。B 形(続行)を採用。ただし敵対的レビューで「Err 経路に証跡前進が無い」ことを
  最終確認する条件付き(横断統合で確認済み: 証跡前進は Ok 腕 :2814-2815 と digest 経路
  :4414/:4444 のみ。単体テストでも固定する — §6)。
- **leave ハンドラからの自動 grace flush**: 証跡削除は GC 安全性に直結するため明示運用
  操作(`flush_grace_peer` API)に限定。leave 直後も grace 窓内に残るのが既定
  (fail-closed 方向。未決点 (6) の裁定)。

### 3. 型・シグネチャ

```rust
// src/runtime/node_runner.rs

/// evict された peer を grace 窓内保持する簿記。揮発(再起動で消える — §5 の安全論証)。
struct GcPeerGrace {
    /// node_id → (最終 PeerConfig, evict された壁時計 ms)。
    /// 除去規則: registry に同一 node_id が存在する限りエントリを持たない
    /// (addr が違っていても除去)。gossip 再加入(同一 node_id)で除去。
    evicted: HashMap<NodeId, (PeerConfig, u64)>,
    /// 窓幅。既定 2 × gc_retention、下限 gc_retention(clamp + WARN、S6 規律)。
    grace_ms: u64,
}

impl NodeRunner {
    /// GC 安全性母集合 = registry 現在集合 ∪ grace 窓内の evicted peer。
    /// gc_gate_diagnose / gc_inbound_gate_passed / prune / 宛先選択の唯一の供給源。
    fn gc_peer_population(&self) -> Vec<PeerConfig>;
}
```

- **evict 誤認の排除(node_id キーの理由)**: gossip reconcile の `update_address`
  (membership.rs:406-415)は同一 node_id の addr 差し替えであり `remove_peer` を経由しない。
  Phase 1 は membership.rs 非接触なので evict 検知は registry スナップショット差分になるが、
  addr キーの簿記だと旧 addr の消滅を evict と誤認して grace に入れ、応答しない旧 addr が
  grace 窓全体(下限 gc_retention=300s、既定 2 倍)で `gc_peer_gate_passed` を fail-closed
  停止させる(現行は prune :3227-3246 が旧 addr 証跡を即刈るためこの停止は存在しない —
  新規退行になる)。node_id キー + 上記除去規則がこれを構造的に排除する(addr 変更後の
  新 addr は registry 現在集合側から母集合に入る)。
- 設定: `ASTEROIDB_GC_PEER_GRACE_MS`(省略時 2 × gc_retention)。env 値が
  gc_retention 未満なら clamp + WARN(S6 規律)。プログラム構築時は
  `debug_assert!(grace_ms >= gc_retention_ms)` を併用。
- 管理 API: `POST /api/internal/gc/flush_grace_peer`(body: `{ "node_id": ... }`、JSON 専用
  — 簿記キーに整合)。
  Phase 2 の decommission API 到着前の dead-peer 運用逃し弁(恒久死亡 peer を grace から
  即時除去し、runbook の「registry から消えるまで GC 停止」を短絡)。
  **C4 注記: JSON 管理エンドポイントであり bincode ワイヤ非対象 — A3 交渉層より先に
  入れても C4 違反にならない(横断統合の裁定)。**
- decode 修正: `decode_response` 失敗時、当該チャンクの全キーを failed 扱いにして
  `SyncPushError` に `undecodable_responses: u32` フィールドを追加、counter
  `sync_push_undecodable_response_total` を加算。残余チャンクは続行。

### 4. 意味論(消費側ごとの表)

| 消費側 | 現行母集合 | Phase 1 後 | 分類根拠 |
|---|---|---|---|
| `gc_gate_diagnose` の peers 引数(A1 コミット B の合流点) | registry 現在集合(:3632-3637) | `gc_peer_population()` | 安全性 |
| `gc_inbound_gate_passed`(hole-jump、:3516-3531) | registry 現在集合 | `gc_peer_population()` | 安全性 |
| 証跡 map の prune(:3227-3246、8 map) | evict で即刈り | grace 母集合基準で一律保持 | **「prune 母集合 = 実際に接触する母集合」不変条件**(下記) |
| sync push / relay / frontier push 宛先(:2264-2293, :2460-2474) | registry 現在集合 | grace 母集合(evict 中の生存 peer にも送る) | 到達性だが、証跡再生成と relay 盲点解消のため拡張 |
| gossip ping / RTT / 発見 | registry | 変更なし | 液性 |
| Raft voter / authority 配置 | 静的 env / cluster_nodes | 変更なし(Phase 2) | — |

prune の分類根拠(B の語彙で明文化): 8 map には証跡 5 map(push_frontiers /
push_acked_wall_ms / pull_reconciled_wall_ms 等)と液性 3 map(backoffs /
digest_unsupported / observed_last_sent)が混在するが、Phase 1 は宛先を grace 母集合に
拡張するため **8 map 一律 grace 基準が首尾一貫**(backoffs / observed_last_sent は
接触対象の簿記であり、接触し続ける以上保持する)。

### 5. 再起動安全性と残余限界(invariant として固定)

- **「両方消えるから安全」**: grace 簿記は揮発だが、証跡 map も揮発。再起動後は証跡が
  空なのでゲートは fail-closed(sweep しない)— grace 簿記の喪失が fail-open を
  生まないことを invariant として明文化しテストで固定。
- **残余 D4 窓**: 分断が grace 窓を超えると現行と同じ挙動(母集合から消え証跡も刈られる)。
  この限界をテストでピン留めし、ops-guide に「grace 超の長期分断は Phase 2 の
  decommission 運用で扱う」と明記(実装時更新)。

### 6. テスト計画(Phase 1)

**無修正パス必須**: gc ゲート単体(node_runner.rs:8118-8229 — 純関数呼び出し形)/ membership.rs 全テスト
(SSRF 検証群 :45-97, :452-601 含む)/ 静止テスト 4 本 / golden digest。

**新規**:
1. **受け入れ e2e(実証不足 #2 後半)**: evict 閾値超(> 45s)かつ grace 窓内の分断 →
   復帰で、hole-jump が生存 peer の live dot を破壊しないこと(D4 の再現 → GREEN)。
2. **対照系**: 分断が grace 窓超の場合は現行挙動と一致すること(残余限界のピン)。
3. **decode 修正単体**: 2xx 不解読応答でチャンク全キー failed・残余続行・
   `push_acked_wall_ms` が前進しない(Err/部分失敗経路に `scan_wall_ms` 挿入が無い)こと。
4. **再起動安全性**: grace 簿記喪失後の初回 tick でゲートが fail-closed であること。
5. **flush_grace_peer**: flush 後に当該 peer が母集合から消え、証跡も刈られること。
6. grace 設定 clamp + WARN の単体。
7. **update_address 非退行ピン**: peer の広告 addr が変わっても(同一 node_id が registry に
   居る限り)grace エントリが生成されず、GC が grace 窓分停止しないこと。

e2e は短縮 interval 構成で実測し、grace 既定(2×gc_retention)の妥当性を確認
(下限 gc_retention は固定 — 未決点 (3))。

---

## Phase 2: scope 付き epoch 所有層(Raft 複製、三重ソース統合)

### 1. 決定

データ所有メンバーシップを Raft 複製コアの **ns 外の第 4 複製対象 `OwnershipState`** として
導入する。scope(range prefix、`KeyRange` 型)ごとに epoch 単調増加の明示遷移
(Joined → Active → Departing → Departed)を持ち、GC ゲート・hole-jump・
`recalculate_authorities` の母集合を「epoch 付き所有集合のスナップショット」消費に切替、
証跡キーを addr から node_id(+epoch)へ移す。ワイヤは A3 交渉層上の **Ownership 版 vN 形**
(N は実装時に A3 の WIRE_MAX+1 で採番 — S4 per-key HLC の v3 が先行していれば v4。
A3 の strict マーカー v2 とは**別物** — §3 の版番号規律)+ 静的 voter 全数の vN 能力ラッチ +
状態駆動フォールバック(Phase 4 > Phase 2 で順序保証)。
decommission 運用 API を追加し dead-peer runbook 依存を打ち止める。

### 2. 却下案と理由

- **SystemNamespace への投影(案 B)**: 「bump しない」規律依存で C2 違反面が近い
  (既存 mutation は全て bump_version する系 :74/:103/:164 に非 bump 経路の新設が必要)。
  ns 外の第 4 複製対象なら ns 完全非接触。
- **`ep == p.joined_epoch` 型ゲート条件(案 B)**: 証跡記録時に scope 現行 epoch を刻む
  素直な実装だと、無関係メンバーの Admit 1 件で全証跡が恒久無効化されゲートが死ぬ。
  **`evidence.epoch >= member.admitted_epoch`(単調・無曖昧)を採用。**
- **Register+Admit+Activate の複数提案合成(案 A 原案)**: 中途失敗で半端状態を残す。
  **`PutOwnership` 単一決定論コマンド**を init API の裏に置き部分 init を構成上排除(B 採用)。
- **OWNERSHIP_SEED env 自動提案(案 B)**: R2 のシード降格思想と逆行。明示 API のみ。
- **pull 単方向の decommission 完了前提(案 B)**: push+pull 双方向(Begin 後 mark を跨ぐ
  証跡)の方が保守的・fail-closed。A 形を維持。

### 3. 型・シグネチャ

```rust
// src/control_plane/(新モジュール ownership.rs 相当)

/// Raft 複製コアの第 4 対象。scope(range prefix)ごとの所有集合。C1: キーは KeyRange。
pub struct OwnershipState {
    /// KeyRange(prefix)→ scope ごとの所有情報。生 String でなく KeyRange を直接使う。
    pub scopes: BTreeMap<KeyRange, ScopeOwnership>,
}

pub struct ScopeOwnership {
    /// scope ローカルの単調増加 epoch。遷移(Admit/Depart 等)ごとに +1。
    pub epoch: u64,
    pub members: BTreeMap<NodeId, OwnershipMember>,
}

pub struct OwnershipMember {
    pub status: OwnershipStatus,   // Joined | Active | Departing | Departed
    /// Active になった時点の epoch。ゲート条件は evidence.epoch >= admitted_epoch。
    pub admitted_epoch: u64,
    pub addr: String,              // 耐久 addr(S3 Step3 の NodeId→addr 解決の供給源)
}
```

- **Raft コマンド**: `PutOwnership { scope: KeyRange, ownership: ScopeOwnership }`
  (単一決定論コマンド)+ 遷移コマンド(Admit / BeginDeparture / CompleteDeparture)。
  JSON 永続(log.json)は `serde(default)` 末尾追記で足りる(storage.rs:17-18 の意図的
  JSON)。**凍結ミラーはワイヤ v1 形の定義としてのみ使う**(下記)。
- **ワイヤ**: ControlPlaneState は InstallSnapshotRequest / NamespaceSnapshotResponse
  (types.rs:230-235/259-272)に bincode 同梱され、post_internal は content-type のみで
  復号・フォールバック無し(raft_transport.rs:173-183)。よって**旧→新方向(新ノードが
  旧 voter の bincode 応答を復号)で末尾追記は EOF 失敗する**。対策: v1 凍結ミラー
  (OwnershipState 抜き)+ A3 交渉層上の vN 形 + **Ownership 形を定義するワイヤ版 N 以上を
  静的 voter 全数が広告して初めて Ownership 系コマンドを propose 可能にするラッチ**
  (ラッチ不成立中は状態駆動フォールバックで v1 形を送る)。ラッチの母集合は静的 voter 集合
  (C3、main.rs:242-249)。解決不能または cap 未学習の voter は「未 vN」= ラッチ不成立
  (fail-closed、横断統合 R-b)。
  **版番号の規律(ラッチ空洞化の禁止)**: A3 の WIRE_V2 は strict decode 宣言のみで
  バイト列は v1 と同一(wire-negotiation.md I1)。OwnershipState を含む ControlPlaneState
  新形はバイト互換でないため **v2 ではあり得ない**。src/http/codec.rs には版パラメータが
  現存せず(accepts_bincode :66-105 は q= のみ解釈)、A3 実装後は**全バイナリが v2
  (strict マーカー)を広告する**ため、ラッチ条件を「v2 広告」と書くと、本形を復号できない
  voter が残るローリング中(A3 済み・Phase 4 未適用)でも早期に開き、vN snapshot/log が
  旧 voter の strict decode / EOF で全滅する。**ラッチ条件は必ず「vN(Ownership 版)以上の
  広告」で条件付けること。**
- **読み取りビュー**: `ownership_states()` は S1 導出ビューの**兄弟アクセサ**として同居
  (ControlPlaneState を保持する層に置き、SystemNamespace の range_states() と同一
  消費面から見える配置 — 具体モジュールは R1 成果物の形に合わせる、§7 未決点)。
  **ownership は readiness に寄与しない**(RangeState に variant 追加しない — 横断統合裁定)。
- **変更検知**: cluster_fingerprint(node_runner.rs:1620-1638)に per-scope ownership
  epoch 列を混合。ns.version() 非依存(C2)。
- **証跡の epoch 刻印(横断統合の必須指示)**: push/pull 証跡は per-peer(全 scope 一括)
  だがゲート照合は per-scope。**証跡記録時に per-scope epoch のスナップショット
  (prefix → epoch の写し)を証跡エントリに刻み、scope s のゲート評価は
  `evidence.epoch_at[s] >= member.admitted_epoch[s]`** とする。グローバル epoch 1 本を
  刻む形は C1 違反(委譲時に証跡キーまで焼き込まれる)。pull 証跡の採取点も
  「記録時スナップショット」に統一(旧未決点 (2) の解消)。

### 4. 意味論(消費側ごとの表)

| 消費側 | Phase 2 後の母集合 | 規則 |
|---|---|---|
| `gc_gate_diagnose` peers / `gc_inbound_gate_passed` | scope ごとの Active ∪ Departing メンバー | **Departing はゲート母集合に残す**(証跡完遂まで安全性に寄与)|
| `recalculate_authorities` 供給 | Active のみ | **Departing は authority 候補から除外**(B の規則を明示) |
| 証跡キー | node_id(+ 記録時 per-scope epoch 刻印) | addr 変更(update_address :413-415)で証跡が宙吊りにならない |
| sync/relay 宛先 | OwnershipMember.addr + gossip 補完 | 到達性は引き続き gossip が補助 |
| Raft voter | 静的 env のまま | C3 恒久。epoch 層に自動追従させない |
| Bootstrap | OwnershipState は reset-and-import に「載る」 | 初期化責務は `PutOwnership`(init API 裏)であり Bootstrap に相乗りさせない(C6 適合の明文化) |

遷移意味論: Joined(staging、ゲート・authority とも対象外)→ Admit で Active
(admitted_epoch 記録)→ BeginDeparture で Departing(authority 除外・ゲート残留)→
push+pull 証跡が Begin 後の mark を跨いだことを確認して CompleteDeparture で Departed
(全母集合から除去、grace/証跡も flush)。

### 5. 移行手順

1. R1/R2(Bootstrap 経路安定)完了後に着手(roadmap D-5 直列化)。
2. v1 凍結ミラー + vN 形を A3 交渉層に登録(A3 の新版メッセージ追加手順に従い、
   実装時に N = WIRE_MAX+1 を採番。strict マーカー v2 と別採番 — §3 の版番号規律)。
3. ラッチ実装: 静的 voter 全数の vN 広告を WireCapCache 経由で確認するまで
   Ownership 系 propose を拒否(明示エラー)。ローリング中は v1 形で従来動作。
4. `PutOwnership` init API で現行 cluster_nodes 相当を一括投入 → 消費側を
   ownership スナップショット消費に切替(cluster_nodes / registry の三重ソース統合)。
5. decommission API(BeginDeparture/CompleteDeparture)公開、dead-peer runbook を
   ops-guide から置換(実装時更新)。

### 6. テスト計画(Phase 2)

- ゲート系単体は純関数シグネチャ維持で機械的型追随のみ。
- epoch 妥当性の敵対的検証: 無関係メンバーの Admit が既存証跡を無効化しないこと
  (`>= admitted_epoch` の単調性)/ 記録時スナップショット刻印の per-scope 独立性。
- ローリング混在 e2e: 旧 voter が居る間 Ownership propose がラッチで拒否され、
  全 voter vN 後に成立すること。**v2(strict マーカー)のみ広告する voter が残る構成では
  ラッチが開かないこと(v2/vN 混同の空洞化ピン)**。旧→新 snapshot install が v1 形
  フォールバックで成功すること。
- decommission e2e: Departing 中の証跡完遂 → Departed で GC が停止しないこと。
- Bootstrap e2e: OwnershipState が reset-and-import に含まれ冪等であること。

---

## 壊すな核との接点(両 Phase)

| 核 | 接触 |
|---|---|
| C-2 証跡意味論(push_acked_wall_ms はゼロエラー完遂 push のみ / pull-advanced を push 証拠に使わない / データ HLC と壁時計を比較しない、:3430-3452, :3677-3691) | 非接触。decode 修正はむしろ偽前進を塞ぐ(fail-open → fail-closed)。D6(Phase 0)の連続 prefix 規則にも適合実装 |
| ゲート純関数構造(&[PeerConfig] + map 引数) | 維持。供給側(gc_peer_population / ownership スナップショット)のみ差替え |
| TombstoneGc mark-and-sweep / compaction_floor / Stage 2 規律 | 非接触 |
| Raft 投票者静的設定(main.rs:242-249) | 恒久維持(C3)。ラッチも静的集合を母集合に使う |
| membership.rs SSRF/アドレス検証群 | 非接触(gossip は発見・到達性レイヤとして温存) |

## 未決点(実装時判断でよいもの)

1. grace 既定 2×gc_retention の実測調整(e2e 短縮 interval 構成で確認後。下限は固定)。
2. `ownership_states()` の具体的配置モジュール(R1 の成果物の形に依存 — R1 実装と整合を
   取る。要件は「range_states() と同一消費面・readiness 非寄与」のみ固定)。
3. vN ラッチの WireCapCache 照会 API 形状(A3 実装の確定待ち。要件は「静的 voter 全数の
   vN(Ownership 版)広告確認・fail-closed」のみ固定)。
4. `flush_grace_peer` の認可(internal token 必須は自明として、追加の confirm パラメータを
   要求するか)。
5. Phase 2 の遷移コマンド粒度(Admit 等を PutOwnership の特殊形にするか独立コマンドか)—
   決定論・冪等であれば可。
