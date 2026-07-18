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
3. 効率系(M-6, M-7)
4. 検知範囲・整合(M-12, M-14, M-17)とテスト(M-16)
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
- **M-6** `src/api/eventual.rs`(merge_remote): 全受信エントリをローカル HLC で無条件再スタンプするため
  収束済みキーがフル CRDT 状態で恒久ピンポン(BP/RR 皆無)。merge_value に no-op 判定(RR)、
  可能なら origin タグで back-propagation 回避(BP)。
- **M-7** `src/runtime/node_runner.rs`(digest sync): サイクル毎・ピア毎・双方向にストア全体をロック保持のまま
  ディープクローン + 全キー SHA-256 再計算(O(N²))。サイクル内メモ化 + dirty バケット増分更新、
  最低限ロック外クローン/不要クローン除去。
- **M-12** `src/authority/frontier_reporter.rs`: frontier の `digest_hash` が (node_id, HLC) のプレースホルダのため
  データ内容の split-view は原理的に検知不能。将来 store digest(D(k) 集約)へ置換。
  (限界は docs 明記済み。コードは次期対応)
- **M-14** `src/runtime/node_runner.rs`: ObservedAttestation の gossip レーンが authority 発 frontier push のみに載り、
  非 authority ノードは観測を中継しない。非 authority を狙った split-view は矛盾ヘッドが出会わない。
  observed レーンを delta/digest sync メッセージにも相乗りさせる(CT gossip の前提回復)。
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
