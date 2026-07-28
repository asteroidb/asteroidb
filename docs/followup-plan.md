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
4. 検知範囲・整合(~~M-12~~ 完了 — 下記「M-12 クローズ記録」参照, ~~M-14~~ 完了 — 下記「M-14 クローズ記録」参照, M-17)とテスト(M-16)
5. minor 一括(m-1〜m-6, m-8 は小規模コード修正でまとめて処理可能、~~m-7~~ 完了、m-9〜m-11 は docs、m-12 は任意)

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
- **M-16** `tests/wal_recovery.rs`: WAL の HTTP レベル耐久性 ack 経路(wait_wal_durable + last_wal_pos)の
  テストが皆無(全テストが eventual_wal/certified_wal=None)。Some(WalSyncer) を配線した AppState で
  書き込み→ack→クラッシュ再現の統合テストを追加。
- **M-17** `src/main.rs`(Raft observer): Raft は voter にしか複製しないため非 voter(observer)ノードの
  namespace が join 時スナップショットで凍結し、observer authority が旧 policy_version で署名し続けて黙って
  fence され、certification 定足数が静かに縮む。observer への namespace 伝搬経路を追加するか、
  authority が非 voter なら起動を fail-stop。

## 残 minor

- **m-1** `src/store/wal.rs`(truncate_to_valid_prefix): 耐久化順序が逆で、リカバリ中クラッシュにより
  削除済みセグメントが復活しギャップ付きリプレイ。unlink → fsync_dir → truncate → sync_all に並べ替え。
- **m-2** `src/store/wal.rs`: 最終セグメントのゼロ埋めヘッダ(torn create)が Corruption 判定になり無害な状態で
  fail-stop。is_last かつ全ゼロヘッダは TornTail に分類。
- **m-3** `src/store/backend.rs`(FileBackend::save): write 失敗時の tmp を削除せず起動時掃除も無い。
  失敗時 remove_file + 起動時 `*.tmp` 掃除。
- **m-4** `src/store/wal.rs`(open): WAL ディレクトリ連鎖の作成が fsync されず、初回起動直後の電源断で
  ack 済み書き込みが消える。open 時に親ディレクトリ連鎖を fsync。
- **m-5** `src/main.rs`(ASTEROIDB_CONTROL_PLANE_NODES 解析): 空エントリ非除去で末尾カンマが phantom voter を作り
  過半数しきい値を変える。parse_static_peers 同様に空要素スキップ + 警告。
- **m-6** `src/http/handlers.rs`(verify_proof): エラーを平文 (StatusCode, String) で返し他 API の構造化 JSON と不整合。
  ApiError 経由に統一。
- ~~**m-7**~~ **完了** — 上記「M-4/m-7 クローズ記録」参照(apply 時再チェック + 告発時 purge +
  自己報告経路 exclude。証明書の遡及失効のみ将来課題として範囲外)。
- **m-8** `src/control_plane/raft/core.rs`(prevote-lite): ガードがリーダー自身を保護せず、分断復帰ノードの
  inflated-term RequestVote が健全リーダーを即降格。リーダー側 check-quorum を追加、最低限コメントを実挙動に合わせる。
- **m-9**(docs) `docs/architecture.md` / `src/store/digest.rs`: digest sync の設計限界(第 3 段 IBLT/ConflictSync の
  スコープ外理由、1 キー発散でバケット全体転送、削減率上限、バケット数のスキーム凍結)が未記載 +「発散部分だけ転送」の過大表現。
- **m-10**(docs) `src/store/backend.rs`(RedbBackend): default feature でビルドされ docs は「Persistent storage via redb」と
  紹介するが実際はリカバリパスに未配線。wasm-compat.md を訂正し「redb はリカバリパス外」と明記、未使用なら feature 整理。
- **m-11**(docs) `docs/architecture.md`: 制御プレーン Raft に BFT 化布石(メッセージ帰属署名)が無く、
  制御プレーン 1 ノード侵害で Authority プレーンの署名投資が迂回可能な非対称が未明記。制約節に追記。
- **m-12** `benches/store_bench.rs`(bench_wal_append_overhead): "sync_interval" ケースが flusher 未起動で
  sync_off と同一パスの重複測定。ケース削除か tokio ランタイム + run_flusher を起動して測定。

## 参照

- research の示唆: `../research/topics/*.md` 各「AsteroidDB への示唆」節、`../research/whitemap.md`
  (§4 横断的緊張 ⚔T1「tombstone GC ↔ fork/分断復旧」= M-8 の背景、§5-6 未踏論点)
- 元レビュー(37 件、critical 2 / major 18 / minor 17)の全文は PR #339 の議論経緯にある想定。
