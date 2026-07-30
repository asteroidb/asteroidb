# A4: S3 certified 値プレーン統合 Step1+2(確定設計)

> **一部差し替え(2026-07-30)**: 骨子(値プレーン統合・D2/D3/D9 の解・移行手順)は
> `core-semantics-v2.md` §8 により**存続**。ただし Step2 の status 導出基準(§4.2 行 5)は
> 「`stored_ts <= majority_frontier`」比較から **coverage 判定(tracked (o,t) の被覆)**に
> 差し替え(v2 §3.6 — 導出の分母は評価時の namespace でなく**書込時 pin 版の名簿**、
> v2 §3.3)。また Timeout は保存状態から削除され(v2 §4 / §12 FR-002)、導出 status が
> timeout を返す行は消滅する(timeout は certified_write の on_timeout=error 系応答値
> 専用 — ラウンド 3 m5/m8)。行 1-4, 6-7 の評価順・fail-closed 規則は不変。

対象: R4(roadmap Phase 3)。吸収する欠陥: D2(certified 値の非複製 — ライター喪失で
読めない)、D3(2-step 未実装 — 未知キー永遠 Pending / eventual 上書き後の偽 Certified)、
D9(certified × SyncPolicy::Interval/Off で ack 済み恒久喪失 — eventual 同等保護に格下げ)、
実証不足 #1(ライター喪失後の certified 読み契約テスト)。

行番号は f48dc04 時点。シンボル(`certified_write` / `eventual_write` /
`finish_local_write` / `merge_remote_with_hlc` / `get_certification_status` /
`resolve_scope` / `is_certified_at_for_scope` / `process_certifications` / `reject_write`)で
再接地すること。

**順序制約(hard ではないが計画順)**: Step1+2 は v1 ワイヤ凍結のまま着地し、S4 v2
(per-key HLC ワイヤ)とは**非同時**(壊すな核への二重変更禁止 — roadmap Phase 3)。
v2 が入って初めて非ライターノードの record_change HLC が origin 収束し、status 回答が
クラスタ収束する(それまではライター上の status が正・非ライターは保守的に Pending 寄り)。
R3(交渉層)⇄ R4 の順序は **soft**(A4 のワイヤ接触ゼロのため R3 遅延は R4 を
ブロックしない — 横断統合 R-a)。

## 1. 決定

`certified_write` を **EventualApi への委譲**として再定義する。CertifiedApi から
store(certified.rs:272,337)/ clock(:338)/ wal・last_wal_pos(:323-327)を削除し
**証明プレーン専任**に縮退。値相は無修正の `eventual_write`(eventual.rs:347-356、
`finish_local_write` の poison 順序 :296-305 を継承)。既存 delta/digest sync が値を
複製して D2 解消、eventual の recovery fence 体系(persistence.rs:286-359)への相乗りで
D9 は eventual 同等に格下げ、M-7 digest が certified 値を初めて内容束縛する。
durability ack は eventual_wal の `wait_wal_durable` に一本化(M-16 意味論維持)。
Step2 で `get_certification_status` を「共有 store の per-key HLC vs
majority_frontier_for_scope」の**導出 status** に再定義し D3 を解消。3 相合成
(precheck → 値相 → track 相)は新設 `src/api/certified_flow.rs` の
`certified_write_flow` / `get_certified_flow` に**唯一の合成点**として畳む。
ワイヤ変更ゼロ。着地は 2 コミット(Step1: store 統合+委譲書込 / Step2: 導出 status)。
**ただし Step1 と Step2 は同一リリース(コミットは bisect 用に分離可、デプロイ単位は一体)**:
Step1 は store と共に `rebuild_pending_from_store`(certified.rs:398, :413-436 —
`self.store` 全キー走査で pending を再登録)の入力を失うため同時に廃止され、fail-closed
揮発性の担い手は Step2 の導出 status(§7)。Step1 単独稼働の窓は作らない(作ると再起動後の
certified 書込が追跡から脱落し恒久 Pending/proof:null — certified.rs:402-411 が自己文書化
する再発バグ — になるか、共有 store 走査に置換すれば eventual 全キーが pending_writes に
流入して M-4 上限の Timeout 追い出し :714-735 を招く)。

## 2. 却下案と理由

- **案 A「certified_merge_write を EventualApi に追加」**: eventual.rs(merge/WAL poison
  コア — roadmap リスク #1 の名指し対象)に diff が入り、内蔵テスト・poison/WAL 系
  テストの無修正パスが崩れる。B(委譲)は eventual.rs を 1 行も触らず構造的にリスクを
  排除する — 決定打。
- **案 A の status 導出順(frontier-Certified を Rejected より先に評価)**:
  `reject_write`(certified.rs:1271)で明示却下済みの書込が frontier 通過で Certified に
  反転する。現行 `process_certifications`(:1169-1188)は Pending のみ昇格で Rejected は
  不変 — B の「Rejected(ts 一致)が導出 Certified に優先」が現行意味論に忠実。
- **案 A の invalidate_stale_proofs tick フック**: 正当性は ts 束縛(proof は
  `cached.write_timestamp == stored_ts` のみ添付)が担い、リモート merge 上書きも
  塞ぐ — フックより強い。メモリ衛生は既存 MAX_CERTIFIED_CACHE eviction(:584-595)で
  既充足。roadmap の D3 帰属文言「cache 無効化を store 書込フックへ」からの逸脱は
  この反証をもって明示的に採る。
- **A から接ぎ木として採用(却下ではない)**: (1) 事前拒否 precheck(§4.1)、
  (2) 合成点の certified_flow 集約、(3) status 規則 [c](移行由来の HLC 未追跡キーは
  Pending — fail-closed)。

## 3. 型・シグネチャ

```rust
// src/api/certified.rs — 縮退(証明プレーン専任)
pub struct CertifiedApi {
    // 削除: store: Store, clock: Hlc, wal / last_wal_pos、
    //       rebuild_pending_from_store(:413-436 — 入力の store が消える。
    //       fail-closed 揮発性は Step2 の導出 status が引き継ぐ — §1 の同一リリース制約)
    // 残置: namespace, pending_writes, certified_cache, attestation_pool,
    //       ack_frontier, equivocation, report_floor, ...(証明機構一式)
}
// PendingWrite.value も削除(diagnostics.rs:121-139 は status 集計のみ使用 — 検証済み)。

// src/api/eventual.rs — 無変更(diff ゼロ)。

// counter 非退行ガード(certified.rs:764-779)を静的純関数として抽出:
/// eventual ロック下で merge 前に呼ぶ。counter の表現不能な退行を fail-stop。
pub fn counter_write_representable(store: &Store, key: &str, value: &TypedValue)
    -> Result<(), CrdtError>;

// src/api/certified_flow.rs — 新設(唯一の 3 相合成点)
/// 相 1: precheck — certified ロック → range_state_for_key()(A1 消費、最長一致を
///        再実装しない)→ Active でなければ PolicyDenied(store 不変)→ ロック drop。
/// 相 2: 値相 — eventual ロック → counter_write_representable → eventual_write
///        → last_wal_pos 捕捉 → ロック drop → wait_wal_durable(M-16 一本化)。
///        wait は必ずロック解放後(handlers.rs:129-134 の明文契約『Called AFTER the
///        API lock is released』— :200-206 の eventual 経路と同型。ロック保持中に
///        待つと SyncPolicy::Always の group-commit fdatasync が全 eventual 書込・
///        anti-entropy sync ループ・delta 応答 :1772 を certified 書込ごとに直列化する)。
/// 相 3: track 相 — certified ロック → scope 再解決 → pending_writes へ登録。
///        再解決が失敗(相間で def 消滅)しても値は durable — CertificationTimeout
///        同型の「値は durable・証明不能」応答にマップ(500 にしない)。
pub async fn certified_write_flow(...) -> Result<CertifiedWriteAck, CrdtError>;
pub async fn get_certified_flow(...) -> CertifiedReadResult;

/// Step2: 導出 status の読取入力(A の value_view 形)。
pub struct ValueView {
    pub present: bool,
    /// record_change の per-key HLC。移行由来キーは未追跡になり得る(→ Pending)。
    pub stored_ts: Option<HlcTimestamp>,
}
```

**ロック規律 L1**: eventual/certified の**同時保持禁止(逐次・非重畳取得のみ)**。
逐次取得ではデッドロック上の順序制約は生じないため取得順は任意(certified_flow 自身が
certified(precheck)→ eventual(値相)→ certified(track)の逐次)。既存例
handlers.rs:1762-1770(certified ロック取得→スコープ drop)→ :1772(eventual ロック)も
逐次取得の前例であり、監査箇所は certified_flow の 1 点に集約。CertifiedApi が Store を
型として持たない事実が規律を構文的に補強する。

## 4. 意味論

### 4.1 書込(certified_write_flow)

| 相 | 状態 | 失敗時の契約 |
|---|---|---|
| precheck | range_state_for_key ≠ Active | PolicyDenied、**store 不変**(現行 certified.rs:711 と同順の契約を維持)。エラー detail で AuthorityOnly / PolicyOnly / 不在を文字列区別(enum はワイヤ非露出) |
| 値相 | eventual_write → last_wal_pos 捕捉 → ロック drop → wait_wal_durable(ロック解放後 — §3 相 2) | eventual と同一(poison 順序 :296-305 継承。旧 certified WAL 失敗の poison 無し :798-801 も自動的に厳密化)。SyncPolicy Interval/Off では eventual と同一の ack 意味論(D9 の格下げ点) |
| track 相 | pending_writes 登録 | 相間で def が消えた場合は「値は durable・証明不能」= CertificationTimeout 同型契約(docs 明記)。**precheck の二重チェックはしない**(旧未決点 (6) の裁定: TOCTOU 残余は同型契約として受容) |

`resolve_scope` のエラー面統一(InvalidArgument → PolicyDenied)は**本設計の Step1
コミットに同梱**(A1 コミット D の移管 — 横断統合裁定)。接地の書き分け:
変換対象の**コード実サイトは certified.rs:504-507(resolve_scope の no-placement-policy)の
1 箇所のみ**。certified.rs:770 の counter 非退行ガード InvalidArgument は**変換対象外**
(counter_write_representable への純関数抽出時もエラー型を維持 — WAL merge-dominance 規律の
エラー面を変えない)。certified.rs:179 / frontier_reporter.rs:87 は doc コメント、
frontier_reporter.rs:549 は #[test] 内コメントであり、いずれも文言追随のみ(tests の
アサーションに現存せず。api-reference / user-guide 同時更新)。

### 4.2 読取 status 導出(Step2、完全マトリクス — 旧未決点 (1) の解消)

入力: `ValueView { present, stored_ts }`、`range_state_for_key`、tracked entry
(`pw.timestamp`, tracked_status)、`certified_cache`(`write_timestamp`)、
`majority_frontier_for_scope`(ack_frontier.rs:504-515)。評価は上から順に最初の一致:

| # | 条件 | status | proof 添付 |
|---|---|---|---|
| 1 | `!present`(key 不在) | NotTracked | なし |
| 2 | range 非 Active(AuthorityOnly / PolicyOnly / 不在) | NotTracked | なし |
| 3 | `present && stored_ts == None`(移行由来・per-key HLC 未追跡) | **Pending(fail-closed)** | なし |
| 4 | tracked_status == Rejected **かつ** `pw.timestamp == stored_ts` | Rejected | なし(**導出 Certified に優先** — reject の frontier 反転を構造排除) |
| 5 | `stored_ts <= majority_frontier`(frontier 通過) | Certified(Timeout 後の遅延 certify 許容) | `cached.write_timestamp == stored_ts` のときのみ添付(D3 偽 Certified の構造排除。リモート merge 上書きも被覆) |
| 6 | tracked entry があり `pw.timestamp == stored_ts` | tracked_status(Pending / Timeout)をそのまま表出 | なし |
| 7 | それ以外(tracked ts != stored_ts の stale 追跡、未追跡の新 ts) | Pending | なし |

- 骨子: **ts 一致時のみ tracked status を表出**。stale 追跡(certify/reject 後に
  eventual/certified いずれかで上書きされた)は Timeout/Rejected を表出せず Pending に
  落とす(行 7)— 「certify 後の両上書き → Pending + proof 無し」回帰テストで固定。
- 行 4 と行 5 の順序が唯一の非自明点(却下案 §2 参照)。
- 単調性根拠: 「ライター上の Hlc 単調性 + リモート経路の `record_change_max`」。
  `record_change` は **overwrite 意味論**(kv.rs:1136 doc)であり per-key 単調ではない —
  根拠文はこの形で書く(旧案 B の誤引用の訂正)。
- NotTracked は FR-002 の 4 状態への 1 状態追加(api-reference 更新は実装時)。
  応答に reason フィールドは追加しない(旧未決点 (2) の裁定: RangeState をワイヤに
  露出しない。区別が必要な運用調査は書込側 PolicyDenied の detail 文字列で足りる)。

### 4.3 消費側への波及

| 消費側 | 変更 |
|---|---|
| sync / digest / delta 経路 | 無変更(certified 値は共有 store の通常エントリとして流れる — ワイヤ形不変、payload 内容のみ変化) |
| `get_certified` | certified_flow 経由で共有 store 読み + §4.2 導出(非ライターでも値が読める = D2 解消の可視点) |
| diagnostics.rs:121-139 | PendingWrite.value 削除の機械的追随(status 集計のみ使用) |
| node_runner.rs:720/842 の HLC 保険シード | `api.store().max_known_hlc()`(certified store)が消えるため**共有 store の max_known_hlc に置換**(旧未決点 (3) の解消: 移行が certified 値を共有 store に merge 済みなので同値以上。真の保証は M-12 floor のまま)。**経路差**: `with_sync_and_cluster_nodes` は eventual store シードが既にあり(:858 付近)置換のみ。`with_cluster_nodes`(:706-736)は eventual_api を受け取らず(構築時 `eventual_api: None` :767)置換先ハンドルが構築時点に無い — **シグネチャに共有 store(または eventual_api)を追加するか、保険シードを eventual_api 接続時点へ移す(要シグネチャ変更)**。この対処を入れて初めて「保険は喪失せず強化」が両経路で成立する |
| tests/http_wal_durability.rs の certified 系 | eventual WAL の durable watermark 監視へ機械的書換(意味論アサーション「ack ⇔ durable 前進」は不変 — 旧未決点 (4) の範囲確定) |

## 5. 移行手順

1. **場所と順序**: `recover_eventual` 内・**WAL replay 後・fence 判定と
   `EventualApi::recovered`(eventual.rs:80-84)構築の前**に、legacy
   certified.snapshot.bin + wal/certified を検出したら raw `Store` へ
   `merge_value` + `record_change_max` で一方向再生。クロックシードは移行後 store の
   max から取られるため移行 HLC を包含(legacy certified 時計先行時の退行上書きの穴なし)。
2. **session/fence 非接触**: `note_applied` / `applied_origins` を触らない
   (fence floor = 復元時 applied_origin、eventual.rs:139-166。触ると floor 汚染で
   leapfrog)。
3. **型衝突は fail-stop**: 二重 store 時代の同一キー異型はキー名列挙で起動失敗
   (ERROR ログに全キー)。黙って落とさない。
4. **退役**: 強制 snapshot 成功後に legacy ファイルを `*.migrated` へ rename(冪等:
   再クラッシュ時は merge-dominance(certified.rs:747-756)が再適用を無害化)。
5. **可観測性(旧未決点 (5) の確定)**: INFO ログ 1 本(migrated_keys / skipped /
   source paths)+ counter `certified_migration_keys_total`。型衝突時は ERROR + fail-stop。
6. **rolling / downgrade**: ワイヤ変更ゼロ。snapshot v5 形式不変。**Step1 単独のデプロイは
   禁止(§1 — rebuild 廃止と導出 status は同一リリース)。**downgrade した旧
   バイナリは `*.migrated` を見ないため certified 読みが value=None に退行する — ops-guide
   に「rolling 窓中の certified 書込誘導」「*.migrated の除去手順」を明記(実装時更新)。

## 6. テスト計画

**無修正パス必須**: 静止テスト 4 本(tests/delta_sync.rs:741,828,884,1065)/
golden digest(digest.rs:1052-1128)/ property_quorum / gc ゲート単体
(node_runner.rs:8118-8229)/ **eventual.rs 内蔵全テスト(diff ゼロの構造的保証)**/
signing_pipeline_e2e / equivocation_e2e / ack_frontier 単体。

**新規(受け入れ条件)**:
1. **実証不足 #1**: 3 ノード e2e — certified_write 後にライターを喪失し、他ノードで
   certified 読みが値を返す(D2 解消の契約テスト)。Step1 完了条件。
2. 移行テスト: 凍結 legacy fixture(certified.snapshot.bin + wal/certified の凍結バイト、
   実証不足 #5 の精神)からの再生 / 冪等再実行 / 型衝突 fail-stop /
   **merge-dominance(A の m3)**: eventual {a:2} + legacy {a:5} → 移行後 5。
3. fence 相互作用: 移行が applied_origins / session floor を変えないこと。
4. Step2 status: §4.2 マトリクス全行の単体 + **「certify 後の eventual/certified 両上書き
   → Pending + proof 無し」回帰** + property(`stored_ts > majority_frontier` ⇒
   決して Certified にならない)。
5. precheck: 非 Active range への certified_write が store 不変で PolicyDenied
   (現行契約の温存ピン)。
6. durability: certified ack が eventual WAL の durable watermark に束縛されること
   (http_wal_durability ハーネスの certified 系書換)。

## 7. 壊すな核との接点

| 核 | 接触 |
|---|---|
| 証明プレーン一式(admission ゲート M-4 / AttestationPool / equivocation / ReportClockFloor M-12 / frontier 単調性) | 非接触(CertifiedApi は縮退するが証明機構は全残置) |
| fail-closed 揮発性(再起動で Certified→Pending) | **担い手を交代して維持**: frontier は揮発(certified.rs:339/:381 の `AckFrontierSet::new()`)なので再起動後は §4.2 行 5 が成立せず、導出 status は自然に Pending(偽 Certified を返さない)。`rebuild_pending_from_store` は store 削除と同時に廃止(§1 の Step1+2 同一リリース制約が rebuild 無き Step1 単独稼働の窓を閉じる) |
| WAL merge-dominance 規律(certified.rs:747-779) | counter ガードは純関数抽出して温存。移行の冪等性根拠としても使用 |
| MergeEffects.changed 契約 / RR 3 条件 / WAL poison 順序(eventual.rs:181-206/296-305) | **eventual.rs diff ゼロで構造的に非接触**(roadmap リスク #1 の歯止め) |
| M-16 durability ack | wait_wal_durable 一本化で意味論維持(certified はむしろ厳密化) |

C1〜C6: 新規 scope 無しキーゼロ(C1)、namespace 単一版数への新規依存ゼロ(C2)、
Raft/投票者非接触(C3)、新ワイヤゼロ(C4)、precheck/status とも range_states() 消費で
8 箇所目の再計算なし(C5)、Bootstrap 非接触(C6)。

## 8. 未決点(実装時判断でよいもの)

1. certified_flow の関数分割粒度(write/read で共有するヘルパの形)— 合成点が 1 ファイル
   1 箇所である限り自由。
2. CertificationTimeout「同型契約」の応答表現(既存 Timeout status を流用するか専用
   detail を付けるか)— user-guide の文言と同時に確定。
3. `counter_write_representable` の配置(certified_flow 内 or store 隣接)—
   eventual ロック下で呼ばれる規律が守られる限り自由。
4. 移行時の一括 merge のバッチ粒度・進捗ログ間隔(巨大 legacy store の起動時間対策)。
5. SyncPolicy Interval/Off 時の user-guide 文言(「certified も eventual と同一の
   durability 窓を持つ」への書き換え詳細)。
