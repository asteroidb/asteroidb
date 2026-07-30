# 制御プレーン無限スケールアウト — North Star と「閉ざさない」設計制約

> **改訂(2026-07-30、`docs/design/core-semantics-v2.md` §6)**: Scatter 型 range 委譲を
> north star から**撤回し、研究オプションに降格**した。リーダーを N 個に増やす方向は
> 「この系はリーダーレスが強み」という規範原則(同文書 §1)と逆行する。スケールアウトの
> 再定義: (1) 合意フットプリントを「まれにしか変わらない小さな状態」(roster + policy)に
> 閉じ込め続ける、(2) データプレーンの部分複製、(3) certification は per-range 名簿で既に
> リーダーレスにスケールする。制約群の改訂: **C1 は削除**(委譲前提の前払いだった)、
> C4 は「新バイト形を作らず append-only で設計せよ。できない場合に初めて交渉層を設計する」
> に緩和、C2/C3/C5/C6 は存続。以下の本文は歴史的経緯と §4 の中央集権点台帳
> (これは引き続き有効な観測)として残す。

作成: 2026-07-28。位置づけ: **実装計画ではなく方位磁針**。未踏提案の「ノードを追加するだけで
単一クラスタのまま無限にスケールできる」という主張を、データプレーンだけでなく制御プレーンまで
届かせるための長期方向と、いま進行中のコア再設計(`docs/core-redesign-roadmap.md`)が
この将来を**閉ざさない**ためのチェックリストを固定する。

## 1. North Star: 再帰的配置

配置ポリシー自体が配置の対象になる。すなわち:

- system namespace は最初から prefix をキーとする range 単位の構造を持つ。この構造を延長し、
  「prefix P 配下の placement policy / authority 定義の合意は、P(または親 prefix)に割り当てられた
  control-plane group が行う」という **range 委譲**を導入する。
- 直系の先行例は Scatter(SOSP'11): キー範囲ごとの独立合意 group の集合として制御平面を構成し、
  range の split / merge / migrate を nested consensus(group 間 2PC + group 内合意)で線形化する。
  隣接 group が partition 境界に合意する assignment consistency が中核不変条件
  (`../research/topics/quorum-consensus.md` §18, §133, §160)。
- 分断耐性上の実利: 現在は少数派パーティションで一切のポリシー更新が止まるが、range 委譲後は
  「その range の group が丸ごと入っているパーティション」は自範囲のポリシーを更新し続けられる
  (例: 軌道面ローカルの配置変更)。

### 再帰は消えない — 薄い静的ルートの原則

Tectonic / Bigtable / GFS / ZooKeeper / HopsFS の全事例が「トップ段は薄い単一点として残る」ことを
示す(`../research/topics/placement-scaleout.md`「制御プレーンの制御プレーン」問題)。多段化は
負荷を対数に落とすがゼロにしない。したがって本設計の到達点は「ルートの消去」ではなく:

- **ルート層(上位 group の構成承認)は小さく・静的で・退屈に保つ**。現在の
  `ASTEROIDB_CONTROL_PLANE_NODES` による静的 Raft voter 集合は、この薄いルートの胚であり、
  委譲構造導入後もルート層の原則(gossip 追従禁止、明示的運用手順による変更)はそのまま昇格する。
- 根に近い prefix ほど immutable 化・全レプリカキャッシュ・明示再分散で守る(HopsFS の教訓)。

### 未解決の核心(research 白地)

制御状態を Certified(合意)で持てば合意ボトルネック、Eventual で持てば authority 集合の
split-view(証明書の検証主体がブレる)。この二律背反の解は `../research` サーベイ範囲で
出ていない(whitemap §5-7)。委譲型設計はこの白地への一つの賭けであり、実装前に設計スパイク
(ロードマップ Phase 5 相当)で以下を確定させる: 委譲境界での証明書検証チェーン、
group 再構成中の certified write 継続(Scatter の assignment consistency の適用形)、
ルート層の障害モード。

## 2. 「閉ざさない」設計制約(進行中の再設計に適用)

以下は core-redesign-roadmap の各フェーズの設計レビューで**必ず確認する**制約。
違反はスケールアウト方向を一段深く閉ざす。

| # | 制約 | 適用先 | 根拠 |
|---|---|---|---|
| C1 | メンバーシップ epoch と所有 replica set のキーは **scope(range)付き**で設計する。実体が当面 1 group でも、グローバル 1 本の epoch にしない | Phase 4(S2 Phase 2) | epoch をグローバルにすると単一 quorum 前提が証跡キー・ゲート母集合まで焼き込まれ、委譲時に全消費側を再改修することになる |
| C2 | namespace の**単一バージョン counter / 単一 bump への新規依存を増やさない**。新設コードは per-range の PolicyVersion(既存)を参照し、グローバル版数は fence ポーリング(既存)に限定 | Phase 1a 以降すべて | 委譲後は per-range バージョン列に分解される。`detect_version_changes` の全体ポーリングは既知の中央集権点として現状維持(新規の同型を作らない) |
| C3 | **Raft 投票者集合の静的原則を維持**し、epoch 層・gossip に自動追従させない | Phase 4 恒久 | 薄い静的ルートの胚。roadmap の恒久禁止事項と同一 |
| C4 | 制御プレーン系の新メッセージは **S5 ワイヤ交渉層の上に載せる**(手作り二段デコードの新設禁止) | Phase 2 以降 | group 間 RPC・委譲・split/merge はすべて新メッセージ。交渉層なしでは互換税が再爆発する |
| C5 | S1 の `range_states()` 導出ビューは「range の準備状態」の**唯一の定義点**であり続ける。委譲導入時は供給側(どの group が合意したか)だけを差し替え、消費側 API は不変に保つ | Phase 1a | 消費側 7 箇所の個別再計算に戻ると、委譲時に解釈分岐が再発する |
| C6 | Bootstrap の reset-and-import は**ルート層専用**の意味論として維持し、委譲 range の初期化を Bootstrap に相乗りさせない | Phase 2(シード降格)以降 | Bootstrap 経路への多重責務は移行不能リスク(roadmap リスク 2)と同根 |

## 3. 無限スケールの二本柱(スコープの正直な区分)

未踏提案の「各ノードのメタデータは割り当てキー範囲に限定され、ノード数に比例して増大しない」は、
現実装ではまだ実現されていない(全ノード全キー複製 + namespace 全複製)。到達には二本柱が要る:

1. **制御プレーンの委譲**(本文書の主題)— Phase 5 スパイク以降。
2. **データプレーンの部分複製** — 配置ポリシーが実際に複製先を絞る配線
   (computed placement × タグ制約の両立。whitemap 次期調査項目 §143)。本文書のスコープ外だが、
   委譲と同じ「range 単位の所有」語彙を共有するため、C1/C5 の scope 付き設計がそのまま前提になる。

## 4. 現在の中央集権点の台帳(委譲時に分解する対象)

f48dc04 時点で単一 quorum / 全複製を前提にしている箇所。委譲設計スパイクの入力。

- `version_counter` と namespace バージョンの単一 bump(`src/control_plane/raft/state_machine.rs`、
  適用時採番)。
- Bootstrap の「リーダーのローカル複製コア全体を 1 エントリで reset-and-import」
  (`src/control_plane/raft/node.rs` build_bootstrap_command)。
- observer pull が namespace **全体**を単位とすること(`POST /api/internal/raft/namespace`、
  `(version_counter, last_applied_index)` の全体辞書式ガード)。
- `detect_version_changes` の全体バージョンポーリング(`src/runtime/node_runner.rs`)。
- catch-all(prefix `""`)シード — Phase 2 で降格予定。降格後も「全 range の既定親」としての
  ルート prefix の扱いは委譲設計で再定義する。
