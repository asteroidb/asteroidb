# フォローアップ計画(PR #339 マージ後)

`../research` の示唆に基づく7機能とマージ前必須修正は PR #339(main へマージ済み)で完了。
本文書は、全体レビュー(コード欠陥 + research 適合 + 相互作用、敵対的検証つき)が
**「マージ後速やかに」** と分類した major と、minor 群の残タスク一覧。

**進め方**: `CLAUDE.md` の規約どおり ultracode(Workflow)で。main agent は直接コード編集しない。
各項目は file:line と要旨のみ記載。着手エージェントは実コードで現状を再確認してから設計すること。
行番号は PR #339 マージ後にずれている可能性があるので、シンボル名で追うこと。

## 推奨着手順

1. ~~**M-8**(最優先)— tombstone GC が収束しない問題。~~ **完了**(per-value compaction floor +
   digest scheme v2。下記「M-8 クローズ記録」参照)。
2. 可用性・DoS 系(~~M-5~~ 完了, ~~M-4~~ 完了 — 下記「M-4/m-7 クローズ記録」参照)
3. 効率系(~~M-6~~ 完了 — 下記「M-6 クローズ記録」参照, ~~M-7~~ 完了 — 下記「M-7 クローズ記録」参照)
4. 検知範囲・整合(~~M-12~~ 完了 — 下記「M-12 クローズ記録」参照, ~~M-14~~ 完了 — 下記「M-14 クローズ記録」参照, ~~M-17~~ 完了 — 下記「M-17 クローズ記録」参照)とテスト(~~M-16~~ 完了 — 下記「M-16 クローズ記録」参照)
5. ~~minor 一括~~ 完了(m-1〜m-6, m-8〜m-12 — 下記「minor 一括クローズ記録」参照。~~m-7~~ は M-4 と同時に完了済み)

## M-4/m-7 クローズ記録(実装済み)

**方式(M-4)**: 一次防衛 = `CertifiedApi::update_frontier_verified` の入口検証
(`attestation_admissible`: range 定義完全一致 ∧ authority set メンバー ∧ policy 存在 ∧
policy_version が現行版 −2..=+1 ウィンドウ内。LAG=2 は `frontier_gc_max_retained_versions`
既定と一致、LEAD=1 は先行報告者の許容)。**入口検証は attestation pool だけでなく
frontier 追跡(`AckFrontierSet`)への advance もゲートする**: `AckFrontierSet` は scope
triple ごとに 1 エントリで固有の上限を持たず永続化もされるため、pool のみ守ると
「拒否された scope が frontier 側に無制限に積もる」形で M-4 の枯渇ベクタが残る。
拒否 scope は `resolve_scope` の要求(定義+policy)と同一条件を満たさないため
frontier 過半数判定にも使われ得ず、正当損失ゼロ。また `FrontierReporter::discover_scopes`
は placement policy を持たない定義(自動シードの catch-all `""` 等)を報告対象から除外
(定義メンバーシップ判定 `is_authority` は policy 有無と独立に維持し、policy 作成後の
refresh で報告対象に昇格)——policy なし range の自己報告が全受信ノードで毎 tick
NoPolicy 拒否 → WARN 恒常発生+flood 信号カウンタ汚染となる経路を報告側で閉じる。
二次防衛 = pool 内ハード上限
(`MAX_POOL_SCOPES`=1024 / `MAX_POOL_SCOPES_PER_AUTHORITY`=64、equivocation.rs と同値)。
上限到達時は新規 scope 拒否(resident 不可侵・既存 scope への参加は無条件受理)+
1 秒スロットルの stale scope 掃除(`retain_scopes`)後に 1 回再試行
(スロットルは wall-clock 逆行を期限切れ扱いにして、時刻巻き戻り中も掃除を抑止しない)。
presence 索引 `authority_scopes` は scope 削除時(gc_scope / retain_scopes / purge)に返還。
メトリクス 6 種(`attestation_pool_scopes` / `attestation_rejected_*_total` ×4 /
`attestation_purged_total`)をイベント駆動で同期。全カウンタ正常時ゼロ期待。

**方式(m-7)**: `ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES=1` のとき
(1) HTTP apply ループで accused 再チェック(同一バッチ内レース・並行リクエストレースを閉じる)、
(2) apply ループ後・同一クリティカルセクション内で新規告発分を `purge_accused_attestations`
(告発前プール分の回収)、(3) 自己報告経路(`NodeRunnerConfig.exclude_accused_authorities`、
main.rs で AppState と同じフラグを配線)でも自ノードが accused なら attestation を pool へ
入れず既存分を purge(is_accused ゲートで毎 tick 冪等、purge 後は O(1))。
自己報告経路の is_accused 判定は HTTP 経路の apply 時再チェックと同様
**certified ロック取得後**に行う(ロック外で読むと、並行リクエストの告発+purge と
自己 attestation 挿入の間に TOCTOU 窓が開き、accused の attestation が最大 1 tick
プールに残って証明書に混入し得る)。
exclude=0(既定)は detect-only 契約どおり一切強制しない。

**範囲外(将来課題)**: 告発**前**に組み立て済み・`certified_cache` にキャッシュ済みの
証明書の遡及失効は本対応の範囲外(evidence ベースの証明書失効プロトコルという別問題。
m-7 の要求範囲「プール済み attestation の以後の証明書組み立てへの混入」は purge で閉じた)。
ops-guide の env 表・メトリクス表・「Frontier 追跡と attestation pool の資源上限(M-4)」節に運用記載あり。

## M-8 クローズ記録(実装済み)

**方式**: `OrSet`/`OrMap` に per-value・per-node の単調ベクタ `compaction_floor` を追加。
certified sweep(mark → retention → C-2 二重ゲート)は tombstone を floor の連続前進に
畳み込む情報等価な圧縮になり、merge は floor を pointwise max で継承(撤回不能)して
covered な stale tombstone / stale live dot を棄却する。digest は scheme v2 の正準形
(live + counters + floor + uncovered deferred)。スナップショット v5 / WAL v2(旧形式は
凍結 decode 型経由で読める・片方向)。Stage 2 hole-jump(`ASTEROIDB_GC_HOLE_JUMP`、既定 off)は
inbound ゲート成立時のみ legacy hole を跨ぐ。

**残タスク**:

- (a) Stage 2 hole-jump の soak 後有効化(運用判断。手順は ops-guide 3.7。
  `gc_floor_stalled_hole_dots` が恒常非ゼロのクラスタのみ)。
- (b) dead peer による GC 停止 — C-2 既存限界は不変(registry に残る dead peer が
  outbound/inbound ゲートを塞ぐ)。floor 化により、将来の過半数ゲート +
  ラガードの floor 追認(復帰時に floor を継承するだけで安全)の下地はできた。
- (c) **floor の認証**: floor は無認証の pointwise max で kill 力を持つため、敵対的 peer の
  水増し floor は撤回不能に live dot を破壊できる(偽造 tombstone と同クラスの脅威だが
  一撃性が高い)。将来 authority 署名付き certificate へ昇格する拡張点。

**受容した限界**: (1) 混在期間は GC 収束が発効しない(fail-safe)。(2) dead peer 停止は上記 (b)。
(3) legacy hole は Stage 2 有効化まで停滞(`gc_floor_stalled_hole_dots` で可観測、fail-safe)。
(4) floor は per-(key,writer) ~50B(NodeId 文字列 + カウンタ、tombstone 1 件と同程度)が恒久残存(件数無制限の tombstone → writer 数定数への圧縮で優位)。
(5) floor は無認証(上記 (c))。(6) sweep は per-key HLC を進めないため floor 伝播は
digest/full sync 機会依存(意図的: GC ごとの delta ストーム回避)。

## M-6 クローズ記録(実装済み)

**方式(RR: redundant relay 抑止)**: CRDT 4 型の merge が「厳密 inflation の有無」を返すようになった
(`MergeEffects.changed` / `PnCounter::merge -> bool` / `LwwRegister::merge -> bool`。
契約: `changed == false ⇒ pre == post`(物理全成分)。join-semilattice 上で merge は単調なので
`changed == true ⟺ 厳密 inflation`)。`Store::merge_value` は `Result<bool>` でこれを中継し
(debug ビルドでは `changed || pre == post` を検査する片方向オラクル付き)、
`EventualApi::merge_remote` / `merge_remote_with_hlc` が RR ゲートを掛ける:

```
skip ⟺ merge が no-op(changed == false)
       かつ store.timestamp_for(key).is_some()
       かつ !store.merge_failed_contains(key)
```

スキップ時は再スタンプ(`record_change` / `record_change_max`)・`note_applied`・WAL 追記を行わない。
untracked キー(per-key HLC 未登録。v1/v2 移行ストア等)は状態同一でも 1 回だけ再スタンプして
delta 可視化する(キーごと高々 1 回 → 有界)。**poison キーはゲートを素通りする**(第三条件):
一過性の WAL 追記失敗で poison されたキーは「マージ済みの状態がメモリにあるのに data record が
WAL に無い」状態であり、送信側の同一値リトライを RR が飲み込むと、WAL に無いデータを ack して
送信側 push frontier を進めてしまい、耐久性修復が「次の sync ラウンド」から「クラッシュ後の
digest anti-entropy 待ち」に退行する。素通りさせればリトライが再スタンプ + 再追記で修復する
(poison 集合は WAL 追記失敗が無い限り空なのでピンポンは復活しない)。`merge_remote_with_hlc` の
`clock.update` と `note_visible` は**無条件のまま**(advisory clock 前進と「response token は
可視状態を必ずカバー」の不変条件を保存)。poison 経路・pull_reconciled 記録条件・
untracked 補償(node_runner)は全て不変。

計装の要点: `rejected_stale_live` / `rejected_covered_deferred` は**非採用イベントであり changed を
含意しない**(含めると lagging peer の stale 再オファーで受信側が恒久 dirty)。counters/floor の
merge は「実際に上がった時のみ」書き込み、幽霊 0 エントリを作らない(`or_insert(0)` 廃止 —
物理変化なのに changed=false となるオラクル破りの排除)。OrMap の other-only キー経路は
「dot を先にフィルタし、全 stale ならエントリも register も作らない」形にリファクタ
(旧: 空エントリ生成→retain 削除の正味 no-op が素朴計装では毎回 changed=true になる穴)。
`killed_by_floor > 0 ⇒ changed` を debug_assert で固定。

効果: 収束済み 2 ノードの双方向 push sync が完全静止(旧実装は無条件再スタンプで
フル CRDT 状態が恒久ピンポン)。書き込み静止後の再送は格子の高さで有界。
`changed_count` が真の変更数に縮み change-rate fallback の誤発火も減少。観測:
`EventualApi::redundant_merge_skips`(in-memory)→ GC tick で
`RuntimeMetrics::sync_redundant_merge_skips_total` に反映(ops-guide のメトリクス表参照)。
テスト: CRDT 4 型の ground-truth テーブル/プロパティテスト(`changed == (pre != post)`)、
API 単体(再スタンプ抑止・untracked 1 回スタンプ・visible 無条件前進・WAL poison キーの
ゲート素通りと data record 修復 — 後者は WAL の test-only fault injection
`WalWriter::inject_append_failures` を使用)、
統合(`tests/delta_sync.rs`: `converged_key_stops_retransmitting` /
`bounded_echo_after_real_change` / `three_node_push_cycle_quiesces` — いずれも修正前 RED —
加えて NodeRunner レベルの `two_node_push_quiesces_after_convergence`: 実 HTTP 上の
本番 sync ループ 2 runner が収束後、`GET /api/internal/keys` の per-key timestamp が
3 サイクル以上完全不変で、`sync_redundant_merge_skips_total` が両ノードでエクスポートされる
ことを検証。ハンドラ直叩きの `push_round` シミュレーションと runner 実装の乖離を防ぐ)。

**却下事項(否定的知見)**:

- **BP(per-key 単一 origin タグによる back-propagation 回避)は不健全で却下**: キーの現在状態は
  複数 origin の join であり得るため、単一タグは第三者寄与を由来ピアへの push から恒久隠蔽し、
  `push_frontiers` の C-2 不変条件と push_acked GC 契約に違反する(検証パネル 3 名が独立に反例構成)。
  採用するなら per-delta の δ-buffer(Enes 原典形)設計が必須。
- `SyncResponse.redundant`(ワイヤでのスキップ通知): 却下。ローカルカウンタで観測は足りる。

**派生フォローアップ(未着手の独立タスク)**:

- **v2 ワイヤ(SyncRequest 末尾に per-key HLC を付加し受信側で origin スタンプ採用)**:
  per-key timestamp のレプリカ間収束により architecture.md「timestamps は digest に含めない」
  制約緩和の道を開く。request 方向は bincode 末尾バイト無視 + 受信側二段デコードで互換成立
  (検証済み)だが、**response 方向(新送信 ← 旧受信の legacy SyncResponse)のクライアント側
  二段デコードが別途必須** — 無対策だとローリングアップグレード中に旧ピアへの全 push が
  失敗扱いになる。push 受信のセッション意味論変更(note_applied→note_visible)の独立レビューも要する。
- **送信側 RR(`delta_against` 系による真の δ 差分送信)**: 受信側 RR(本対応)は転送量自体は
  減らさない(エコーの吸収と再送ループの停止まで)。送信量削減は別タスク。

## M-7 クローズ記録(実装済み)

**方式(Store 内蔵 dirty バケット増分 DigestCache + generation 検証)**: `Store` に
`#[serde(skip)]` の `DigestCache`(`src/store/digest.rs`: per-key digest+bucket メタ、dirty 集合、
mutation generation、invalid フラグ)を内蔵。`data` を変異させ得る全 6 メソッド
(`get_mut`(Some 時)/ `put` / `put_with_timestamp` / `delete` / `merge_value` /
`merge_delta_value`)が同一呼び出し内で `note_dirty`(dirty 挿入 + generation 無条件 ++)。
`merge_value Ok(false)` のみ非 dirty(「物理不変」契約 + debug オラクル + M-6 RR とのシナジー:
収束済み定常はクラスタ全体でクローン 0・ハッシュ 0)。`Store::digest()` は dirty キーのみ
再ハッシュ → 影響バケットをキャッシュ済み 32B per-key digest から再結合 → root 再計算し、
常に `compute_store_digest` とビット同一(debug ビルドは毎 refresh でアサート、golden 3 本
無修正パス + キャッシュ経由 golden を追加)。ロック外作業(cold リビルド / warm 経路の
不一致バケット抽出)は「取得時 generation == 適用時 generation」の全か無か検証のみで正当化
(部分採用 API 不存在 — 設計時に致命指摘された seed_from_full 系の stale meta 汚染を構造的に排除)。
warm-up は `api::digest_warmup::ensure_digest_warm`(run_sync 冒頭、最大 2 試行、
`spawn_blocking`、失敗時は従来のスナップショット経路へフォールバックで挙動不変)。
呼び出し側: `try_digest_pull` は warm 時クローン全廃(`pull_reconciled_wall_ms` 記録条件は不変)、
`try_digest_push` は root 一致/peer-only 判定を T0 digest の key_counts で再ロックなしに行い、
不一致時の抽出は generation 一致なら T0 証明済みで証拠前進、不一致なら**データは送るが
証拠(push_frontiers/push_acked_wall_ms)は据え置き**(安全方向: GC ゲートが待つだけ、
静穏後の root 一致で自己治癒 — 旧設計の「T1 選別の単調性依存」を撤廃)。handler
(`internal_digest_sync`)は warm 時に単一ロックスコープで応答(root 一致 O(1)、不一致は
`clone_bucket_entries` でバケット部分クローンのみ)、cold は従来コードを温存し応答フィールド同一
(cold/warm 等価性テストで固定)。永続化・wire 形式・スキームバージョンは全て不変。

テスト: kv.rs に無効化条件の全列挙(put/put_with_timestamp/delete 存在・不在/get_mut Some・None/
merge inflating/no-op/型不一致 Err/merge_delta/メタデータ非 dirty/既 dirty でも generation 前進/
GC floor 前進)+ ランダム op 列の等価性 + Clone/serde/bincode/WAL replay/migration 経由の
整合性。digest.rs に digest_pass sink 検証 + キャッシュ経由 golden。digest_warmup.rs に
採用/全破棄(F1 回帰)/dirty バースト。tests/digest_sync.rs に push 証拠意味論
(root 一致で前進・安定 generation subset push で前進・並行書き込み時は送るが据え置き(F2 回帰))+
handler cold/warm フィールド同一性。既存 digest 関連テスト(golden 3 本、digest_sync 27 本、
delta_sync、GC ライブロック回帰)は全て無修正パス。ベンチ: `benches/digest_bench.rs`
(N∈{1k,10k,100k} × legacy クローン+全ハッシュ / full hash のみ / cached d=0 / d∈{1,64,1024})。

**派生フォローアップ(未着手の独立タスク)**:

- **M-7-f1** `apply_complete_state`(node_runner.rs)のロック保持 merge ループ — 残る最大の
  ロック保持区間。store 変異ゆえロック外化不可、チャンク化は完全性転送の原子性意味論に触れるため別タスク。
- **M-7-f2** legacy full push fallback(digest 経路正常時は不達)のフルクローンは未最適化のまま。
- **M-7-f3** key_meta の per-bucket 索引化(refresh の O(N) メタ走査と `clone_bucket_entries` の
  O(N) 走査を O(bucket) に)— ベンチが必要性を示した場合のみ。
- **M-7-f4** key_meta 常駐メモリ(キー 1 重複製 + 33B/キー)の長大キー × 大 N 環境での監視/上限
  ドキュメント。

## M-12 クローズ記録(実装済み)

**方式(root-digest + ReportClockFloor hybrid)**: frontier 報告の `digest_hash` を
プレースホルダ `(node_id, HLC)` から eventual store の M-7 root digest
(`sd2:<hex64>` = `sd{DIGEST_SCHEME_VERSION}:{hex(StoreDigest.root)}`、
`frontier_reporter::format_store_digest_hash`)へ置換。digest は tick ごとに一度だけ
計算して全 scope に同一バイトを束縛し、report 署名で凍結する。warm-up 未完了 tick は
`sd2:cold`、eventual 未接続は `sd2:unavailable` のセンチネル(内容非束縛、fail-safe)。
ワイヤ・署名レイアウト・検証・admission・`AckFrontierSet::update` は一切不変
(digest は不透明文字列。旧ノードも新形式の split-view をそのまま検知できる)。

**誤検知ゼロの機構**(digest が HLC の決定的関数でなくなったため、不変条件
「同一 (authority, frontier_hlc) ⇒ 同一 digest」は報告側で維持する):

- プロセス内: `Hlc::now()` の厳密単調性 + tick 内単一計算 + 署名凍結。
- 再起動跨ぎ: **`runtime::report_clock::ReportClockFloor`**
  (`<data_dir>/frontier_report_clock.json`、リース幅 10 秒の write-ahead fsync)。
  順序は「HLC 採番 → `cover()` fsync 成功 → 署名・自己観測・apply・push」で固定し、
  fsync 失敗 tick は報告ごとスキップ(`frontier_report_skipped_floor_total`)。
  起動時は `Hlc::seed_recovered`(skew ガードをバイパスする回復専用 API)でリース値から
  clock を seed —— `Hlc::update` は壁時計逆行 60 秒超(floor が守るべきまさにそのケース)で
  `ClockSkew` 拒否するため使えない(設計時の全案が見落とし、実装で確定した知見)。
- 初回起動・floor 喪失時: **activation grace 180 秒**(= 観測ヘッド保持 120s +
  skew 前提 60s、monotonic)の間は **frontier 報告を完全停止**(何も署名しない)、
  grace 明けに sd2 で再開。プレースホルダ報告では「前世代が sd2 で署名 + floor 喪失 +
  60 秒以内の壁時計逆行」で placeholder-vs-sd2 の偽証拠が成立し得るため(検証レビューで
  確定した欠陥)、無署名だけが両形式方向に安全。grace 中は floor ファイルも作らない
  (grace 中クラッシュは次回 grace を最初からやり直し——「floor 存在 = 全履歴カバー」の
  証拠性を維持)。runtime での authority 昇格(membership 再計算)時もコンストラクタと
  同一の floor 初期化を実行する。floor path 未構成なら sd2 形式は決して有効化しない
  (fail-safe)。残余仮定は「クロック逆行はクラスタ前提 60 秒以内」と「floor ファイルを
  バックアップから復元しない」(staleness はローカル判別不能。ops-guide に運用規則明記)。
- store 復旧 max HLC からの clock seed は保険として実施(certified/eventual 両方)だが、
  data HLC は report HLC を覆う保証がないため floor の代替ではない。

**運用**: キルスイッチ `ASTEROIDB_FRONTIER_STORE_DIGEST=0`(要再起動、ローリング
アップグレード順序制約なし・ダウングレード安全)。誤検知時の回復パスとして
`EquivocationDetector::purge_authority` + `DELETE /api/authority/equivocations/{authority_id}`
(internal token 保護、evidence/accused/observed heads を除去して永続化・gauge 更新)と
runbook「False positive recovery」を新設。メトリクス `frontier_digest_cold_total` /
`frontier_report_skipped_floor_total` / `frontier_nonbinding_digest_total`(受信側:
非束縛 digest 形式の受理累計——悪意 authority の内容束縛オプトアウトの唯一の可観測点)追加。

**テスト**: report_clock 単体(roundtrip/破損/境界 bump/write-ahead 永続化)、
node_runner 単体(実 digest 束縛/SD_UNAVAILABLE/cold センチネル遷移無証拠/
tick 間 store 変異の偽陽性ゼロ/floor 失敗 tick スキップ/far-future リース seed/
floor path なし不活性/grace 中の報告完全停止 → sd2 再開/grace 中クラッシュの grace
やり直し/runtime 昇格時の floor 初期化/キルスイッチ)、equivocation 単体(purge +
永続 payload 縮小)、
e2e(実 Store digest での split-view 検知 + 証拠の root 一致・第三者再検証/同一署名
再送 Consistent/新旧形式混在無証拠/floor 付き再起動 + 内容変化でヘッド保持ピア無証拠/
purge エンドポイントの認可・永続化・attestation 復帰)。

**残余(未着手の独立タスク)**:

- **M-12b**: per-scope digest による evidence の key_range 帰属(root は検知の上位集合
  だが帰属を示さない。設計素案: key_meta の BTreeMap range 集約 + generation メモ、
  per-scope dirty 追跡)。
- PeerReview 型 cross-verification(digest 自己申告と実サービング内容の照合)、
  CT 型 consistency proof(frontier_hlc ずらし回避への対抗)。
- compaction checkpoint digest(engine.rs、FR-010 系統)は**別系統で未解決のまま**。
- 検知不能残余(docs/ops-guide.md 限界節に列挙): HLC ずらし・結託過半数・per-key HLC 等
  メタデータのみの分岐・観測窓外・cold の非束縛 tick・grace 中の報告空白・
  非束縛形式へのオプトアウト(メトリクスで可視化のみ)。

## M-14 クローズ記録(実装済み)

**方式**: `DeltaSyncRequest` / `DigestSyncRequest` の**末尾**に
`#[serde(default)] pub observed: Vec<ObservedAttestation>` を追加(append-only、
`skip_serializing_if` 禁止 — bincode 位置依存の既存規約どおり)。全ノード共通の
`run_sync` が 1 サイクルに 1 回 `gossip_summaries(GOSSIP_SAMPLE_MAX=64)` を採取し、
ピアごとに**最初に送出する 1 carrier**(digest push probe → delta pull 初回 →
delta pull の NetworkError リトライ(未達時のみ同一サンプル再添付)→ digest pull の順)
へ相乗りさせる。同一サンプルの再送はピアごとの
`(フィンガープリント, 配達時刻)`(`observed_last_sent`、ピア数で有界・レジストリ離脱時に
prune)で抑止し、**ヘッドが動かない定常時は 0 バイト**。抑止は
`OBSERVED_RETENTION_MS`(120s)で失効する時限式——受信側の観測索引はメモリのみ
(再起動で消え、ヘッドも同じ窓で age-out)のため、無期限の抑止は中継ホップの再起動や
混在期の旧ピア(末尾バイトを捨てて 200 を返す)のアップグレード後に中継経路を恒久沈黙
させ得る。コストは最悪でもピアあたり 1 窓 1 回の冗長サンプル(受信側 `is_known_exact`
で検証前 dedupe)。受信側は `post_internal_frontiers` の
split-view ブロックを `ingest_relayed_observations`(handlers.rs)へ共通化し、
delta/digest ハンドラがデシリアライズ直後・store ロック取得前・digest scheme 判定前に
呼ぶ(observed はレスポンス内容・ステータスに一切影響しない。`scheme_ok=false`
応答パスでも取込む)。ゲート順は frontier レーンと同一
(registry 無し全捨て → range メンバーシップ → `take(64)` → `is_known_exact` dedupe →
署名検証(失敗は非告発)→ `observe`)。検知時は frontier レーンと同一手順で
`exclude_accused_authorities` 時の attestation purge も発火。

**ワイヤ互換**: 新→旧 bincode は旧デコーダが末尾 observed を残余として無視(200)。
旧→新 bincode は位置 decode 失敗 → 400 → 旧側実装済みの JSON 再送で成功
(`serde(default)` が空充足。M-8 `compaction_floor` と同一の実証済みパターン)。
response 型・`SyncRequest`・frontier レーンは不変(M-6 却下類型を構造的に回避)。
混在期は旧→新方向の**毎リクエスト**が 400+JSON 再送になる(4xx アラート誤報要因、
ops-guide に想定内と明記)。回帰は legacy ミラー構造体テストで固定
(`new_bincode_requests_decode_on_legacy_mirrors_ignoring_trailing_observed` ほか、
bincode 残余非検証への依存も明示的にピン)。

**M-12 整合(grace 論証の改訂)**: 中継は hop ごとに `seen_ms` を再計時し age-out 後の
再 index もあり得るため、「grace 明けには旧世代ヘッドが全ピアで期限切れ」という旧論証は
**廃棄**。安全性はヘッド寿命と無関係のクロック算術
(索引され得る旧ヘッド HLC ≤ W_old+60s、grace 明け初回報告 ≥ W_old+120s)で成立し、
不変量 **`DIGEST_ACTIVATION_GRACE > 2 × MAX_CLOCK_SKEW_MS`**(180s > 120s、
**厳密不等号** — 両側の受理境界は包含的なため、等号では同一 physical の
pre/post-restart ペアが排除できない)を
`digest_activation_grace_covers_clock_swing_budget` で固定。grace 値・沈黙幅・
M-12 テストの変更は不要(実測: 既存 M-12 e2e 全て無変更で通過)。
`equivocation.rs` の detector コア(定数・observe・索引構造)は不変で、検知プール
上限 M-4 がそのまま中継の受信メモリ上限として効く。唯一の変更は
`gossip_summaries` の出力コピーが `ObservedAttestation::for_wire_relay` を通ること:
non-native-crypto(stub)ビルドは BLS フィールドを検証せずに索引し得るが、native
受信側は BLS フィールドをデシリアライズ時(bincode/JSON 両方)に厳格検証するため、
不正な BLS 文字列を 1 件でも中継すると carrier リクエスト全体がデコード不能になる。
そのため stub ビルドは中継コピーから BLS レーンを剥離する(証拠採否は全ビルドで
Ed25519 レーンのみで決まるため検知能力は不変。native ビルドは検証済みの BLS レーンを
そのまま中継)。

**有界性**: 送信 ≤ 64 件 × ~1.2KB ≈ 80KB/peer/サイクル(定常 0)、受信 ≤ 64 件
× 最大 2 Ed25519 検証/request(frontier レーンと同一上限)、中継は周期 tick のみで
reactive flooding 無し・既知エコーは検証前 dedupe(ループ不成立)。
`sync_interval ≥ OBSERVED_RETENTION_MS` の構成は起動時 WARN。
メトリクス: `observed_relay_sync_requests_total` / `observed_relay_sync_accepted_total`
(sync レーン専用)、`split_view_observations_total` は両レーン共通で連続性維持。

**テスト**: 非 authority 標的 split-view の e2e
(`split_view_targeting_non_authorities_detected_via_sync_relay` — 修正前コードで
赤(タイムアウト)を確認済み)、多段中継 e2e(X—Z—Y チェーン)、ハンドラ単体
(取込・scheme_ok=false 取込・偽造非告発・64 件 cap・registry 無し全捨て・
response バイト不変・purge 連動)、runner(attach-once・未変化抑止・NetworkError
再添付・配達記録の失効再送・digest push probe carrier の搭載/配達記録・digest 404
Fallback の非記録と次サイクル再添付)、ワイヤ互換(legacy ミラー)、JSON リトライ
同一 req 再送、grace 不変量(厳密)、stub ビルドの BLS レーン剥離。

**残余(未着手の独立タスク)**:
- サンプラ希釈対策: scope 横断ラウンドロビンのカーソルを呼び出し跨ぎで永続化
  (既存 authority レーンにも効く独立改善)。
- digest は不透明比較のため HLC 完全一致ペア以外の一過性 split-view は検知不能
  (CT 系譜との本質差、ops-guide 限界節 5b に明記)。
- `HeadEntry` への provenance フラグ(中継由来ヘッドの metrics 専用属性)は
  必要になった時点で別タスク化(安全クリティカルな detector コアへの侵襲を回避)。
- 恒久対策としての期限付き legacy ミラー二段デコード(混在期 4xx ノイズが
  問題化した場合の後付け案)。

## M-17 クローズ記録(実装済み)

**方式**: 非 voter(observer)ノードが voter から **committed 済み
`ControlPlaneState` を定期 pull**(新 internal RPC
`POST /api/internal/raft/namespace`、`ASTEROIDB_OBSERVER_NS_PULL_MS` 既定
5000ms、0=無効)して追随する。pull ループは driver の非 voter 分岐
(ラウンドロビン + ジッタ ±20% + 失敗時指数バックオフ上限 30s + pull age
6 間隔超で WARN)。採用ガードは (1) voter は無条件拒否、(2) 応答者が
ローカル voter set 外なら拒否、(3) **`(version_counter, last_applied_index)`
の辞書式単調比較**(OR 合成だと zombie voter の高 index でロールバックする
ため不採用)。適用は `handle_install_snapshot` から抽出した `install_state`
を push/pull で共用し、snapshot meta・log 切り詰め・storage 永続化・
namespace 永続化(applied marker 込み)まで InstallSnapshot 受信 follower と
同一のディスク状態を残す——以後の fence/unfence/`recalculate_authorities`/
`refresh_scopes` は既存 `detect_version_changes` 連鎖(observer 上の
NodeRunner)がそのまま実施。fail-stop は不採用(非 voter authority は
`recalculate_authorities` の正常出力であり、propose 時
(`PUT /api/control-plane/authorities` に非 voter が含まれる場合は 200 +
`warnings` フィールド + WARN)と起動時(observer authority の lifeline
警告、`CONTROL_PLANE_NODES` 未設定誤構成の警告文面追記)に留める)。

**可観測化(全段)**: voter 受信側は `CertifiedApi` 内部カウンタ →
`AttestationPoolStats` → `RuntimeMetrics` の既存パターンで
`attestation_stale_version_total`(**窓内だが現行版より古い pv の受理**。
bump 1 回目から発火する最速信号、per-scope 1 分スロットルの WARN 付き)と
`attestation_rejected_fenced_total`(fence 済み scope への報告破棄。M-17
以前は完全無音)を追加。observer 側は `RaftStatus`/`RaftStatusResponse` に
`observer_ns_pull_success_total` / `observer_ns_pull_failure_total` /
`observer_ns_last_pull_unix_ms` / `observer_ns_version_counter` を追加。

**分断時の設計判断**: observer authority は**署名を止めない**(分断中に
bump が無ければ寄与は完全有効。bump があった場合の寄与消失はどの設計でも
不可避であり、「無音の恒久欠損」→「両側可視の一時欠損 + 分断解消後
1 pull 間隔 + 1 tick での自動復帰」に変換)。分母 `total_authorities` は
縮めない(既存方針踏襲)。

**実装中に発見した設計欠陥(2 件、最小修正済み)**:
1. observer は投票しないため `hard_state.json` が存在せず、pull 採用が
   `log.json` だけを書くと**次回起動時に storage の「log あり hard state
   なし」fail-stop で起動不能**になる——採用時に hard state も併せて永続化。
2. storage の整合性検査は `current_term >= ログ最大 term` を要求するため、
   採用する snapshot 境界の term が自 term より新しい場合は term を単調に
   引き上げる(voted_for は新 term 突入時のみクリア。InstallSnapshot 受信
   follower と同じ挙動で、二重投票リスクなし。応答者の current term
   (`resp.term`)は設計どおり不使用のまま)。

**レビュー指摘の修正(マージ前検証で確定した 2 系統)**:
1. **採用ガードの再起動時フロア**: ガード(3) の local 基準は再起動後
   コンパクション snapshot から復元されるが、voter は apply 毎に
   namespace + marker を永続化し compaction は稀(最大 `log_max` apply 分
   先行)。降格 ex-voter observer が「snapshot より新しいが保持済み
   namespace ビューより古い」pull(遅れた voter の応答)を採用すると
   ビューが耐久的にロールバックするため、apply marker に
   `version_counter` を追記し(旧形式は `None` → snapshot 対へフォール
   バック)、起動時にビューを保持した場合は marker の対をガードの下限
   (`adopt_floor`)にした。
2. **pull カウンタの正確化**: 成功計上と `observer_ns_last_pull_unix_ms`
   更新は「採用 / not-newer」の健全ラウンドのみ。応答者 voter set 外
   (アドレス誤解決——HTTP は成功し続ける)と採用時ローカル永続化失敗は
   `observer_ns_pull_failure_total` に計上して freshness を更新しない
   (でなければ pull-age アラートが誤設定・ディスク障害で沈黙する)。
   `adopt_pulled_snapshot` は bool ではなく `AdoptOutcome`
   (Adopted/NotNewer/VoterRefusal/RejectedResponder)を返し、driver の
   pull 失敗ログは error クラス付き WARN(頻度はバックオフで有界)。

**テスト**(`tests/observer_namespace_sync.rs` + `certified.rs` 単体):
T-0 silent fence 再現(pull 無効 = 旧世界。obs 寄与確認 → bump → stale/
fenced カウンタ発火 → 1 voter 停止で certified write Timeout → 復旧後も
`contributing_authorities` から obs 不在)/ T-1 pull end-to-end(多段 bump
窓外跨ぎ追随 + 段階 A→B カウンタ遷移 + 寄与復帰 + 非 voter authority PUT の
warnings)/ T-2 採用ガード(辞書式単調・zombie 拒否・voter set 外拒否・
voter 拒否・`committed_snapshot` 正当性)/ T-3 再起動永続化 / T-4 分断
(失敗カウンタ → 自動復旧 → observer 上の fence)/ 単独 voter で pull
不起動の回帰。

**読解発見(記録)**: `JoinResponse.namespace` には in-process 消費者が
存在しない(一回きり同期としても未配線)。api-reference.md に「参考情報・
自動適用されない」旨を明記した。継続伝搬は本 pull が担うため、join 時
スナップショット適用の配線は行わない。

**将来の opt-in ノブ候補(実装しない)**: 鮮度 TTL
`ASTEROIDB_AUTHORITY_MAX_NS_AGE_MS`(0=無効既定)——pull age が閾値を超えた
observer authority が署名を自発停止する案。分断中の無害な寄与まで放棄する
ため既定挙動としては不採用と判定した(判定者全員一致)。必要になった場合に
別タスク化。

## M-16 クローズ記録(実装済み)

**成果物**: 新規 `tests/http_wal_durability.rs`(8 テスト)+ Cargo.toml の
`[[test]] required-features = ["native-runtime"]` 宣言。プロダクション変更は
`WalSyncer::durable_watermark()`(read-only getter、durable カウンタの
Acquire ロードのみ)の 1 点だけで、耐久性セマンティクスには触れていない。

**方式**: AppState に `Some(WalSyncer)` を配線し、リカバリは常に本番経路
(`recover_eventual` / `recover_certified`)。flusher を spawn しない
`Held` モードで durable 前進手段(flusher / `wal_rotate`)をテストが独占し、
「300ms 経っても ack が返らない」pend 断定をタイミング非依存にした
(正実装は永久 pend、壊れた実装は μs で 200 → 判定にタイミングが関与しない。
`advance_durable` の呼び出し元が増えた場合はこのファイルの見直しが必要——
ファイル冒頭 INVARIANT コメントに明記)。テスト一覧:
ack 済み書き込みのクラッシュ生存(本番 `spawn_persistence_tasks` 配線 +
ack 時点の on-disk frame 検査)/ Always の ack ゲート(held で pend →
後入れ flusher で解放。Notify permit 貯留を根拠にコメント化)/ certified 側
ゲート + 復旧で Pending 回帰 / 未 ack 書き込みの torn tail 切除と非対称復旧
(ack 済みは残り、未 ack は復元されない)/ WAL append 失敗時の 503 +
token 不在 + 復旧後の非復活(chmod 0o555 + tiny segment、Drop guard で復元、
特権実行時は skip)/ group commit(1 回の rotate で複数 waiter 一括解放 =
`wait_durable` の `>=` と `fetch_max` 単調性の pin)/ Interval 政策の
即 ack(`durable_watermark()==0` の直接証明)/ `internal_sync` の
durable ゲート。fail-first は 2 変異で実施済み:
(1) `wait_wal_durable` の wait 除去 → 6/8 RED、
(2) `wait_durable` の `>=`→`==` → group commit テストが timeout で RED。

**設計書からの逸脱(1 点)**: 設計書 T5 は「ok-1 を rotate で解放してから
chmod → fail-key」だったが、rotate 直後の空セグメントへの初回 append は
ローテーションを起こさない(`seg_records > 0` ガード)ため EACCES が
発火しない。ok-1 の解放 rotate を chmod 前に行わず、失敗フェーズを跨いで
pend させたまま ok-2 の poison-flush ローテーション(seal 副作用の
`advance_durable`)で解放する順序に修正した(テスト内コメントに記録)。

**ack 経路の実バグ**: テスト作成過程・変異検証を通じて未発見(設計書 §7 の
コード精読結論と一致)。既知の意図的挙動 3 点(internal_sync の wait エラー
握りつぶし / 非 Always の即 ack + recovery fence 補償 / fsync 失敗の
process::abort)はバグ扱いしない。

## 残 major(マージ後速やかに)

- ~~**M-8**~~ **完了** — 上記クローズ記録参照。
- ~~**M-4**~~ **完了** — 上記「M-4/m-7 クローズ記録」参照(入口検証 + pool 内ハード上限 + メトリクス)。
- ~~**M-5**~~ **完了** — rotate 専用の `create_or_reclaim_segment` が自ライタの torn create
  (ヘッダ長以下 = frame ゼロ)を truncate 再利用で冪等に回収し、`init_segment` 失敗時は
  best-effort unlink で orphan 自体を残さない。frame を含み得る衝突ファイルは `InvalidData` で
  拒否(fail-safe。ops-guide の永続化節に runbook 追記済み)。`WalWriter::open` 経路は厳格
  create(AlreadyExists で fail-loud)のまま。派生フォローアップ: `rotate_locked` の
  `advance_durable` を `sync_all` 成功直後(create 前)へ移動する件(rotate 失敗時に
  `durable` が過小報告される。`SyncPolicy::Off` 構成で実益。前提: `appended` の増加が
  state ロック下に限られること)は未着手の独立タスク。
- ~~**M-6**~~ **完了** — 下記「M-6 クローズ記録」参照(RR: no-op 判定による再スタンプ抑止。
  BP は否定的知見として却下記録)。
- ~~**M-7**~~ **完了** — 下記「M-7 クローズ記録」参照(Store 内蔵 dirty バケット増分 DigestCache +
  generation 検証)。
- ~~**M-12**~~ **完了** — 上記「M-12 クローズ記録」参照(root digest 束縛 + ReportClockFloor +
  activation grace + 誤検知回復エンドポイント。per-scope 帰属は M-12b として残余記録)。
- ~~**M-14**~~ **完了** — 下記「M-14 クローズ記録」参照(observed レーンの delta/digest sync request 相乗り)。
- ~~**M-16**~~ **完了** — 下記「M-16 クローズ記録」参照(`tests/http_wal_durability.rs`:
  Some(WalSyncer) 配線 AppState での書き込み→ack→クラッシュ再現統合テスト 8 本)。
- ~~**M-17**~~ **完了** — 下記「M-17 クローズ記録」参照(observer への committed namespace pull 同期 +
  無音 fence の全段可観測化。fail-stop は不採用)。

## minor 一括クローズ記録(m-1〜m-6, m-8〜m-12 実装済み)

- ~~**m-1**~~ **完了** — `truncate_to_valid_prefix` を「後続セグメント unlink 群 →(unlink があった
  場合のみ)`fsync_dir` バリア → stop セグメントの truncate/remove → 最終 `fsync_dir`」に並べ替え。
  不変条件「stop の切り詰めが耐久化される時点で後続 unlink は全耐久化済み」を確立し、各ステップ間
  クラッシュは停止点の再検出で冪等に再実行される。テストは `#[cfg(test)]` の操作トレース
  (`TRUNCATE_OPS`)で順序を固定(`truncate_orders_unlink_fsync_before_stop_truncation`)。
  注: 旧順序で実害が出るのは「非最終セグメント Corruption + operator truncate 逃し弁 + 後続
  セグメント存在」の狭い窓のみ(TornTail は is_last 限定で後続が構造的に無い)。
- ~~**m-2**~~ **完了** — `parse_segment` で「is_last かつ `is_torn_create_len`(= ちょうど 16B)かつ
  全ゼロ」を TornTail(valid_len 0)に分類。truncate がファイルを除去した後、次の `WalWriter::open` は
  残存 max+1 として**同じ seq を再利用**する。非最終セグメントの同形状は従来どおり Corruption
  (ガードテストあり)。「magic は書けたが version=0」の canonical prefix は意図的に Corruption のまま
  (安全性無害・可用性のみ、スコープ自制)。`is_torn_create_len` は read/write 両側が共有する述語に
  doc 更新。
- ~~**m-3**~~ **完了** — `FileBackend::save` の write/sync/rename/fsync_dir をクロージャ化し、失敗時に
  自 tmp を best-effort 削除(NotFound 無視、他は warn。元エラーをそのまま返す)。起動時掃除
  `FileBackend::remove_stale_tmp_files`(`<target>.<数字>.<数字>.tmp` 厳密一致のみ、親不在は Ok(0))を
  新設し、`recover_store` の snapshot load 前に配線(失敗は warn のみ)。`FileBackend::new` には
  入れない(並行 save の in-flight tmp 誤爆防止 — 掃除は起動時 1 回限定が安全条件)。
  `ops/mod.rs::write_atomic` の同種残余はスコープ外。
- ~~**m-4**~~ **完了** — `WalWriter::open` が新設ヘルパー `create_dir_all_durable`(backend.rs)で
  ディレクトリを作成しつつ連鎖を fsync。ヘルパーは冒頭で `std::path::absolute` により絶対化
  (相対パスで連鎖が cwd 手前で途切れる論証欠陥を回避。cwd 解決失敗はエラー伝播 = fail-stop 整合)し、
  作成前に「最深の既存祖先」を特定。**fail-stop の fsync 対象はこの呼び出しが変更したものだけ**
  (新規作成ディレクトリ群 + エントリが増えた最深既存祖先)。それより上位の既存祖先は耐久性確立済み
  なので best-effort(失敗は warn)— traverse-only(`--x`、例: 0711 の home)祖先で `File::open` が
  EACCES になっても既存ノードの起動を落とさない(可用性リグレッション回避)。電源断はプロセス内
  再現不能のため主担保は論証レビュー + 完走 smoke テスト + traverse-only 祖先の起動回帰テスト。
- ~~**m-5**~~ **完了** — 純関数 `parse_control_plane_nodes` に分離: 空エントリは警告付きスキップ
  (phantom `NodeId("")` voter による majority 分母の歪みと configuration fencing への混入を排除)、
  未設定・空白のみは従来どおり `[self]`、**設定済みかつ全エントリ空(`","` 等)は fail-stop**
  (確定的な設定破損。`[self]` フォールバックは lone-default 発散を黙って引き起こすため。
  設定エラー fail-stop の前例 = election timeout 検証に整合)。単体テスト 4 系列で固定。
- ~~**m-6**~~ **完了** — `verify_proof` の戻りを `Result<_, ApiError>` に統一。HTTP ステータスは
  全 5 箇所現状維持(registry 未設定 = `CrdtError::Internal` → 500 INTERNAL、他 4 箇所 =
  `InvalidArgument` → 400)、メッセージ文字列不変、ボディのみ構造化 JSON 化。
  ステータスの意味論変更(503 化等)は行っていない。
- ~~**m-7**~~ **完了** — 上記「M-4/m-7 クローズ記録」参照(apply 時再チェック + 告発時 purge +
  自己報告経路 exclude。証明書の遡及失効のみ将来課題として範囲外)。
- ~~**m-8**~~ **完了(コメント訂正 + 挙動固定テストのみ、check-quorum は不採用)** — 起票の
  「リーダー側 check-quorum を追加」は実装しないと裁定: (1) vote 経路のみのガードは無効
  (`handle_append_response` / `handle_snapshot_ack` が inflated term の応答で無条件降格するため
  heartbeat 1 周期以内に降格する)、(2) 応答経路までガードすると復帰 voter が inflated term から
  永久に降りられない liveness バグ、(3) 安全性は election restriction により無傷で、中断は分断復帰時
  1 回・有界(election timeout 既定 min 5s / max 10s → 回復 ≈ 5〜10 秒)。module doc とガード直前
  コメントを実挙動(ガードは follower 限定)に訂正し、挙動固定テスト
  `leader_steps_down_on_inflated_term_vote_from_partition_returnee` を追加。
  **完全な抑止はフル PreVote(新 RPC・ワイヤ変更)が必要 — 将来課題として残す。**
- ~~**m-9**~~ **完了(docs)** — architecture.md digest 節:「発散している部分だけの転送」を
  「発散を含むバケット単位(1/256)の転送」に訂正、「粒度の限界」小段落(増幅コスト・削減率上限・
  第 3 段 IBLT/ConflictSync スコープ外の判断軸)を追加、バケット数 256 に `DIGEST_BUCKET_COUNT`
  scheme 凍結の参照を明記。`src/store/digest.rs` のモジュール doc は正確なため変更なし。
- ~~**m-10**~~ **完了(第 1 段 docs のみ)** — wasm-compat.md 3 箇所と `RedbBackend` 型 doc に
  「experimental / リカバリパス未配線(実永続化は FileBackend スナップショット + WAL)」を明記。
  **第 2 段(feature 整理 or 配線)は今回見送り・別起票**: 公開 API 削除であり、CI 第 2 マトリクスと
  CLAUDE.md が `native-storage` を名指しする中で feature キーを空で残す互換策は第 2 マトリクスの
  指定を無意味化する副作用があるため、「redb を配線するか削除するか」の判断ごと残余タスクとする。
- ~~**m-11**~~ **完了(docs)** — architecture.md トレードオフ節の「Byzantine 障害耐性なし」を拡張:
  攻撃経路(共有トークン認証は**設定時のみ**有効・メッセージ帰属署名なし・voter 1 台侵害で正規手続き
  commit = 過半数侵害不要)、非対称(authority 定義書き換えで Authority プレーンの署名投資が迂回、
  Byzantine 耐性は最弱 voter 1 台で決まる)、布石(per-node 署名 → BFT 置換、いずれも将来フェーズ)。
- ~~**m-12**~~ **完了** — `bench_wal_append_overhead` が tokio ランタイムを起こし
  `WalSyncer::run_flusher` を実起動(案 b)。退行ガードは計測完了後の deadline 2 秒ポーリングで
  `durable_watermark() > 0` を確認(criterion `--test` の 1 パス実行でも flaky にならない)。
  `[[bench]] store_bench` に `required-features = ["native-runtime"]` を追加。
  注: sync_interval の数値は従来の「sync_off と同一パスの重複測定」から Interval 実コスト込みへ
  変わるため過去数値と非連続。

### minor 一括の残余(別起票)

- **redb の配線 or 削除の判断**(m-10 第 2 段): `native-storage` feature と `RedbBackend` を
  リカバリパスに配線するか、feature ごと削除するか。CI 第 2 マトリクス(`native-tls,native-storage`)と
  CLAUDE.md の記述更新を伴うため独立タスク。
- **フル PreVote**(m-8 残余): 分断復帰ノードによる有界なリーダー中断(1 回の再選出)を完全に
  抑止するには PreVote RPC の追加(ワイヤ変更)が必要。将来課題。

## コア十分性 思考実験(PR #340 マージ後、8 レンズ敵対的評価)

総合判定: **不足**(一様ではない — Eventual レーン単体は閉じている。不足は下記 D1/D2 に集中)。
以下は 8 シナリオ思考実験 + クロス検証で confirmed された設計不足。深刻度順。番号は着手管理用。

- **D1**(fatal / 着手中)`src/runtime/node_runner.rs`(`gc_authority_gate_passed`): GC 権威ゲートが
  全 authority 定義を AND で要求する一方、M-4 で frontier reporter は policy 無し定義を報告対象外にした。
  既定シードの catch-all(prefix `""`、`main.rs`)に policy 未設定かつ authority 構成済みだと、来ない
  report を待って sweep が**永久に走らない・無音**。→ GC ゲートの母集合を reporter/admission と同条件
  (policy 無し定義を除外)に揃える + 本番同等シードでの GC 収束統合テスト。M-8 の価値を帳消しにする。
- **D2**(fatal)`src/network/sync.rs`: anti-entropy/sync 経路に certified 参照ゼロ。証明は時計通過
  (到達性)のみを束縛し、値はライターのローカル store+WAL にしか存在しない。ライター喪失で
  「証明付き確定」データが消滅。→ certified store の複製経路(専用 anti-entropy か authority 値転送)を
  設計。当面は docs の保証範囲を「到達性の証明」に訂正。D3 と同根。大型タスク。
- **D3**(major)FR-004 の 2-step(eventual_write → status)未実装: 未知キー永遠 Pending / stale cache の
  偽 Certified。→ eventual 書込を certified 追跡に橋渡し、または 2-step を docs から撤回し status API に
  `NotTracked` を追加。
- **D4**(major)C-2 GC ゲートの母集合が ~30-45 秒で evict される gossip registry 前提のため、分断中の
  Stage 2 hole-jump が evict で live dot を破壊し得る。→ retention 超の期間ピアを保持する「GC 用 grace
  付きピア集合」で安全論証を registry evict から切り離す。
- **D5**(major)ドリフト >60s で acked 書込が無音消失(`hlc.rs` ClockSkew 無音破棄 + `LwwRegister::set()`
  握り潰し)。→ set() 失敗を STALE エラーでクライアントに返す + ClockSkew 拒否カウンタ輸出。
- **D6**(major)接触窓容量 < バックログで delta sync が同一 prefix を永遠に再送する livelock(部分失敗で
  frontier 非前進)。→ 成功バッチ末尾までの frontier チェックポイント化(resumable sync)。
- **D7**(major)単一 Mutex 上の per-peer O(N) 走査群が 10^6 キー帯でロック飽和。→ HLC 順変更索引 +
  ピア並列化。着手前に実測で深刻度確定(実証不足と一体)。
- **D8**(major)equivocation 観測索引が正当 cardinality(authority×range×版)で `MAX_TRACKED_SCOPES=1024`
  超で LRU スラッシュ、検知能力が無メトリクスで消失。→ eviction カウンタ輸出 + 上限を設定化。
- **D9**(major)certified × `SyncPolicy::Interval`/`Off` で ack 済み恒久喪失にガード・fence・警告が皆無
  (eventual と非対称)。→ certified WAL に Always 強制 or 起動時拒否/WARN + ops-guide 明記。
- **D10**(major/docs)`ops-guide` の「旧バイナリダウングレードも安全」は誤り — pre-M-12 は floor/grace を
  持たず placeholder-vs-sd2 偽 POM が成立。→ runbook を「ダウングレードは必ず flag=0 の新バイナリ経由」に訂正。

### 再考すべき受容済み限界(docs 期待値の調整)

- 制御プレーン認証の fail-open 既定 + 平文 HTTP、observer pull の自己申告 node_id 採用(CFT 前提内では
  妥当だが secure-by-default = token 未設定時に internal API 拒否/大警告 + 宛先↔node_id 束縛は数行級)。
- `requirements.md`(split-view 検知)/ `user-guide`(「過半数が承認」)が実装(HLC 完全一致検知/到達性証明)
  より強く読める。Byzantine スコープ外の受容自体は妥当だが文言の限定明記は必須(D2/D10 と同根)。
- 120s 観測保持窓は週分断で split-view 検知カバレッジがゼロになる旨を Byzantine フェーズ計画に明記。

### 実証不足(優先順)

1. Eventual/Certified 境界のクライアント契約テスト(user-guide のコード例を実行 + ライター喪失後の
   certified 読み)。2. 本番同等シード(catch-all 含む)での GC 収束 + evict 閾値超分断→復帰 e2e。
3. 実電源断ハーネス(dm-flakey/LazyFS or kill -9 soak)。4. 10^6 キー×50 ピア級の bench/soak。
5. 凍結旧形状 bincode デコードテスト。

## 参照

- research の示唆: `../research/topics/*.md` 各「AsteroidDB への示唆」節、`../research/whitemap.md`
  (§4 横断的緊張 ⚔T1「tombstone GC ↔ fork/分断復旧」= M-8 の背景、§5-6 未踏論点)
- 元レビュー(37 件、critical 2 / major 18 / minor 17)の全文は PR #339 の議論経緯にある想定。
