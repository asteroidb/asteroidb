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
2. 可用性・DoS 系(M-5, M-4)
3. 効率系(M-6, M-7)
4. 検知範囲・整合(M-12, M-14, M-17)とテスト(M-16)
5. minor 一括(m-1〜m-8 は小規模コード修正でまとめて処理可能、m-9〜m-11 は docs、m-12 は任意)

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
- **M-4** `src/authority/attestation_pool.rs`: AttestationPool の scope 数に上限が無く、登録済み Authority 1 台が
  policy_version/key_range を変えるだけでメモリ枯渇 DoS 可能。equivocation.rs と同等の scope 数上限 +
  per-authority 上限 + policy_version の現行版検証を導入。
- **M-5** `src/store/wal.rs`(rotate/create_segment): rotate 中の ENOSPC で orphan セグメントが残り、
  以後の全 append・checkpoint が再起動まで `AlreadyExists` で恒久失敗。失敗時 unlink か seq 再取得で自己回復させる。
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
- **m-7** `src/http/handlers.rs`: `ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES=1` でも告発前にプール済み attestation が
  最大 128 checkpoint 分証明書に混入。告発時に AttestationPool から purge + 自己報告経路にも exclude 適用(M-4 と同時に)。
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
