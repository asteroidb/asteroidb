# A3: S5 ワイヤバージョン交渉層(確定設計)

> **一部 SUPERSEDED(2026-07-30)**: `core-semantics-v2.md` §7 により Stage 1-3
> (v= 交渉・strict decode・受動学習)はカット(将来要求先行 — A3 自身が案 B を却下した
> 物差しが自分に当たる、という指摘の受理)。**Stage 0(fallback 3 重複製の共有モジュール
> 統合 = 純減)のみ実施対象として有効**。再着火条件: append-only + serde(default) で
> 表現できない新バイト形が実際に必要になった時。本文は Stage 0 の仕様と経緯資料として残す。

対象: R3(roadmap Phase 2)。吸収する欠陥: fallback 3 重複製(sync.rs:707
`send_with_json_fallback` / frontier_sync.rs:132 / raft_transport.rs `post_internal`)、
二段デコード 2+1 箇所(sync.rs:1133-1169 pull_delta / :1210-1243 digest_sync /
pull_all_keys の Accept 再試行)、混在期の毎リクエスト 400+JSON 再送、残余黙認
(codec.rs:49-50)の恒久互換機構化、実証不足 #5(凍結旧形状 bincode デコードテスト)。

行番号は f48dc04 時点。シンボル(`send_with_json_fallback` / `decode_response` /
`accepts_bincode` / `serialize_internal` / `post_internal`)で再接地すること。

## 1. 決定

**Content-Type パラメータ交渉**を採用する。リクエストは
`Content-Type: application/octet-stream; v=2`、応答は受信側が同形式でエコーし、
送信側は**毎応答の受動観測**でピア能力を学習する(専用 hello 交換なし・追加 RTT ゼロ)。
`WireVersion` は単一線形軸。codec.rs に 4 関数を純追加(既存関数無変更)、
`src/network/wire.rs` に共有 `WireClient` + `WireCapCache` を新設して 3 クライアントの
fallback/デコード機構を統合する。v=2 以降は strict decode(消費長==全長)、残余黙認は
v1 レーン専用に封印。Stage 0→3 の段階投入(各段バイト等価で独立ロールバック可)。
ops kill switch `ASTEROIDB_WIRE_NEGOTIATION=off` で送信側を「Learning 固定 + v= パラメータ
付与の全面停止」に退避できる(定義は §4)。

## 2. 却下案と理由

- **案 B「hello エンドポイント + instance トークン + features BTreeSet」**:
  hello は認可面の追加と TTL 毎の 404 雑音を生み、能力の staleness 窓が最大 1h
  (受動学習は次の 1 応答で閉じる)。instance トークンは毎応答観測が再起動検知を包含する
  ため不要。features 集合は将来要求先行(過剰設計の歯止めに抵触)— 直交能力が実需要化した
  時点で v bump か別途導入を判断。
- **caps.digest_scheme 事前通知**: `scheme_ok` の権威(digest レーンの既存交渉)と重複し
  かねず keep_intact の趣旨に反する。digest_unsupported キャッシュとの直交性は doc 化のみ。
- **B-2 ping 相乗り(caps を gossip ping に載せる)**: **deferred オプションとして記録**。
  長期分断復帰後の初回交換が Learning に落ちる(1 交換分 JSON)への将来緩和策。
  JSON レーン末尾 append なので後付け無条件安全。採否基準: 実測で 1 交換遅延が問題化した
  場合のみ。今は実装しない。
- **protobuf 等への形式置換**: roadmap 裁定で不要かつ有害(検証資産の無価値化)。

## 3. 型・シグネチャ

```rust
// src/http/codec.rs — 純追加 4 関数(既存 accepts_bincode / serialize_internal 等は無変更)

pub const WIRE_V1: WireVersion = WireVersion(1);
pub const WIRE_V2: WireVersion = WireVersion(2);
pub const WIRE_MAX: WireVersion = WIRE_V2;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct WireVersion(pub u8);

/// "application/octet-stream; v=2" 等から v= を抽出。無印は v1。
pub fn parse_wire_version(content_type: Option<&str>) -> Option<WireVersion>;
/// Content-Type 文字列の組み立て("application/octet-stream; v=N"、v1 は無印)。
pub fn content_type_for(v: WireVersion) -> &'static str;
/// strict decode: bincode 復号後に消費長 == 全長を検査。v2 以降の受信専用。
pub fn deserialize_strict<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError>;
/// Accept ヘッダから受理最大版を抽出(無印 bincode 受理は v1 扱い)。
pub fn max_accepted_version(accept: Option<&str>) -> Option<WireVersion>;
```

```rust
// src/network/wire.rs — 新設(native-runtime ゲート配下、wasm 非対象)

/// ピアごとの学習済みワイヤ能力。恒久 addr キー(液性ドメイン — 横断統合 R-b。
/// 誤キーの代償は JSON フォールバック 1 往復のみ。S2 Phase 2 の node_id 再キー化は
/// GC 証跡 map 限定で、本キャッシュへは波及させない — followup 起票不要)。
pub struct WireCapCache {
    caps: Mutex<HashMap<String, WireVersion>>,   // addr → 学習済み最大共通版
}

pub enum SendLane { Learning, Confirmed(WireVersion) }

pub struct WireClient {
    http: reqwest::Client,       // 呼び出し側の既存 client を注入(timeout 温存)
    cache: Arc<WireCapCache>,
    auth_token: Option<String>,
    negotiation_enabled: bool,   // ASTEROIDB_WIRE_NEGOTIATION != "off"
}

impl WireClient {
    /// POST: Confirmed なら学習済み版で 1 発。Learning なら現行力学
    /// (bincode 送信 → 非 2xx で JSON 再送 = send_with_json_fallback 等価)。
    /// 全応答の Content-Type エコーを観測して cache を受動更新。
    /// 拒否(400 等)を観測したら Confirmed → demote(caps_demotions_total)。
    pub async fn post<Req: Serialize, Resp: DeserializeOwned>(
        &self, url: &str, req: &Req,
    ) -> Result<Resp, WireError>;

    /// GET(pull_all_keys 経路): Confirmed は学習済み版の Accept で 1 段。
    /// Learning は現行の 2 段(bincode Accept → 拒否時 Accept 無し素 GET)を等価再現。
    pub async fn get<Resp: DeserializeOwned>(&self, url: &str) -> Result<Resp, WireError>;
}
```

counter 3 本(S6 テンプレート、metrics.rs の AtomicU64 群と同型):
`wire_json_fallback_retries_total` / `wire_strict_decode_rejects_total` /
`wire_caps_demotions_total`。

**版番号の規律(A2/S4 との整合)**: `WireVersion` は単一線形軸だが、**版番号は機能ごとに
採番される**。v2 は strict decode 宣言のみでバイト列は v1 と同一(I1)— strict マーカーで
あって新バイト形ではない。**新しいバイト形を導入する機能は v3 以降を採番する**
(S4 per-key HLC = v3 — roadmap 割当済み。S2 Phase 2 Ownership 形 = 実装時の WIRE_MAX+1)。
能力ラッチ(A2)などの機能ゲートは、strict マーカー v2 ではなく必ず対象機能の版番号で
条件付けること(membership-epochs.md §3 の版番号規律)。

### ヘッダ意味論(確定 — 旧未決点 (1))

- **リクエスト `Content-Type` の v= = ボディの実バージョン**。受信側は
  `v > WIRE_MAX` なら **400 で騒がしく失敗**(ボディは既に届いており解釈不能を黙認する
  選択肢が無い。送信側はこの 400 を観測して demote するため自己修復する)。
- **リクエスト `Accept` の v= = 送信側が受理できる最大版**。受信側は応答を
  `min(WIRE_MAX, 相手の Accept)` に**クランプ**して常に成功させる(応答は送信側が版を
  選べるため fail は不要)。Accept 無印 bincode は v1、Accept 非 bincode は JSON。
- 応答 `Content-Type` は実際に使った版をエコー — これが受動学習の唯一の情報源。
- パラメータ透過は既存テスト(codec.rs:449-457/491-498 の q=/charset= パース)が
  旧ノードで無害であることをピン済み。

### 不変条件(I1〜I4)

- **I1**: v2 のバイト列は v1 と同一(v2 = strict 検査の宣言のみ)。よって rolling 中の
  版不一致はデコード失敗を生まない。
- **I2**: 受信側 strict(残余で 400)は `Content-Type; v>=2` が明示されたリクエスト/
  応答にのみ適用。無印(v1)レーンは残余黙認を維持(M-14 ピン sync.rs:2108 は
  **v1 レーンのピンとして恒久維持**)。
- **I3**: 学習は応答観測のみで前進し、失敗観測のみで後退(demote)。タイマー失効で
  勝手に昇格しない。
- **I4**: JSON レーン(外部 API 含む)は交渉対象外・無変更(codec.rs:8)。

## 4. 意味論(消費側ごとの表)

| 消費側 | 現行機構 | 移行後 |
|---|---|---|
| sync push(`push_changed_keys` → `send_with_json_fallback` sync.rs:707) | 毎回 bincode → 非 2xx で JSON 再送(500/503 でも) | `WireClient::post`。Confirmed 後は再送ゼロ |
| frontier push(frontier_sync.rs:132) | 同上の複製 | 同上(client timeout 5s 温存) |
| raft RPC(raft_transport.rs `post_internal`) | 同上の複製 | 同上(HTTP_TIMEOUT 5s 温存)。attempts==2 断言テスト(:354-358)は無修正パス |
| pull_delta(sync.rs:1133-1169)/ digest_sync(:1210-1243) | 応答の手作り二段デコード | `WireClient::post` の応答復号に統合(v1 応答は寛容デコード、v>=2 応答は strict) |
| pull_all_keys(sync.rs:1011-)GET | bincode Accept → 拒否時素 GET の 2 段 | `WireClient::get`(Learning で挙動等価、Confirmed で 1 段) |
| 受信側 handlers.rs デコードサイト **8 箇所**(515/1290/1308/1326/1349/1657/1750/1857) | `deserialize_internal`(残余黙認) | `parse_wire_version` で分岐: 無印/v1 → 従来デコーダ、v>=2 → `deserialize_strict`。**全 8 箇所を列挙して置換すること** |
| digest scheme_ok / digest_unsupported キャッシュ | 既存 | **無変更**(直交軸であることを doc コメント化) |
| 外部 JSON API | 既存 | 無変更(I4) |

### タイムアウト実値(接地済み — 旧未決点 (2))

- sync client: 30s + connect 5s(sync.rs:625-627, 639-641)
- frontier sync client: 5s(frontier_sync.rs:69-70, 80-81)
- raft transport: `HTTP_TIMEOUT` 5s(raft_transport.rs:36-37、membership client と整合)
- テスト用コンストラクタ: 100ms(sync.rs:1407 ほか)

`WireClient` は **reqwest::Client を呼び出し側から注入**する構成とし、per-client timeout を
そのまま温存する(WireClient 自身は timeout を持たない)。

### kill switch(確定 — 旧未決点 (7))

- 名称: `ASTEROIDB_WIRE_NEGOTIATION`。値 `off` は **(a) SendLane を Learning 固定**
  (= 現行力学へ退避)**かつ (b) `Accept` / `Content-Type` への v= パラメータ付与の
  全面停止**、の**両方**を意味する(digest の enabled kill switch と同型)。
  (b) が無いと Learning レーンでも Stage 2 以降の `Accept: v=2` 広告に対し受信側が
  応答を v2 にクランプして返し(ヘッダ意味論)、v= が飛び交って strict も発火し得る —
  ops 退避スイッチとして (a)+(b) で定義する。
- 受信側 strict は無効化しない: strict は `v>=2` を明示されたときのみ発火し、
  全ノードが off なら(定義 (b) により)誰も v= を送らないため発火しない(明文化)。
  不正な v= を送る非対応クライアントは 400 で可視化されるのが正しい挙動。

## 5. 移行手順(Stage 0→3)

- **Stage 0(挙動等価)**: `WireClient`/`WireCapCache` 新設 + 3 クライアントの
  fallback/二段デコードを WireClient に統合。ヘッダは従来と同一(v= 未付与)。
  既存テスト無修正パスで等価性を証明。
- **Stage 1(挙動等価)**: codec.rs 4 関数追加 + 受信側 8 箇所に v= 分岐追加
  (v= が来ない限り従来経路)。Stage 0 と**同一 PR の別コミット**とする(旧未決点 (5) の
  裁定: どちらも挙動等価だが bisect 可能性のためコミットは分離、PR は 1 本で可)。
- **Stage 2**: 送信側が `Accept: ...; v=2` を付与開始(Content-Type はまだ v1)。
  応答エコーの学習開始。旧ノードはパラメータを無視して octet-stream にマッチ
  (accepts_bincode のカンマ独立照合)— ワイヤ断絶なし。
- **Stage 3**: 学習済みピアへ `Content-Type: ...; v=2` 送信 + strict 受信 + kill switch。
- 各 Stage はバイト等価(I1)で独立コミット・ロールバック可。旧データ移行なし
  (ワイヤ層限定)。
- **WireCapCache の共有 Arc 構築場所(旧未決点 (3))**: main.rs の runtime 組み立て点で
  `Arc::new(WireCapCache::default())` を 1 個生成し、SyncClient / FrontierSyncClient は
  NodeRunner 構築経由、HttpRaftTransport は main.rs で直接注入。テスト用コンストラクタは
  各自新規 cache で可(共有は本番配線のみの要件)。
- **wasm 境界(旧未決点 (8))**: `network` モジュールは `#[cfg(feature = "native-runtime")]`
  ゲート配下(lib.rs:8-15)であり、`src/network/wire.rs` は wasm ビルドに含まれない。
  codec.rs の追加 4 関数は依存フリーの純関数で wasm 安全。`--features wasm` の
  cargo check 非破壊を PR 合格条件に含める。

## 6. テスト計画

**無修正パス必須**: 静止テスト 4 本 / golden digest(scheme v2 凍結 digest.rs:1052-1128)/
codec.rs ラウンドトリップ群 / legacy ミラーテスト(sync.rs:2037-2050)/
raft attempts==2 断言(raft_transport.rs:354-358)/ WAL v1 replay / property テスト。

**新規(受け入れ条件)**:

1. **実証不足 #5: 凍結フィクスチャの全型列挙** — 全ワイヤ型
   (SyncRequest/SyncResponse、DeltaSyncRequest/Response 両方向、KeyDumpResponse、
   DigestSyncRequest/Response 両方向、FrontierPushRequest/PullResponse、Raft 4 RPC 型)を
   明示列挙し、**hex 定数化した凍結バイト**でミラー構造体依存を断つ。両方向を固定:
   現行デコーダが旧バイトを復号できること / legacy ミラーが現行バイトを復号できること。
2. **strict 封印の両面ピン**: 同一の残余付きボディが「v=2 なら 400 / 無印なら黙認」に
   なる対比テスト(I2 の固定)。
3. **受動学習の収束**: 3 連続 push で初回のみ JSON フォールバックが発生し以後ゼロになる
   counter 断言(混在期ストーム消滅の計測証明 — 受け入れ条件に格上げ)。
4. demote 経路: Confirmed ピアの 400 応答で Learning に落ち、次交換で回復すること。
5. kill switch: `off` で全交換が Learning 固定かつ Accept/Content-Type とも v= 送信ゼロに
   なること(§4 の off 定義 (a)+(b) に接続)。
6. Accept クランプ / v>MAX 400 / エコー学習の単体(codec 4 関数 + WireClient)。
7. GET 経路の挙動等価(Learning 2 段の再現)テスト。

## 7. 壊すな核との接点

| 核 | 接触 |
|---|---|
| bincode・全メッセージ型・serde(default) 既存フィールド | 非接触(v2=v1 バイト同一、I1) |
| JSON レーン / 外部 API JSON 専用(codec.rs:8) | 非接触(I4) |
| WAL/snapshot/digest の版体系(scheme_version+scheme_ok 含む) | 非接触。digest 交渉は直交軸として doc 化のみ |
| M-14 observed レーン / carrier 選出 | 非接触。残余黙認ピン(sync.rs:2108)は v1 レーンのピンとして恒久維持 |
| 静的 voter(C3) | 非接触(static_peers 不変) |

C1/C2/C5/C6 無関係、C3 適合、C4 は本設計そのもの(以後の新ワイヤメッセージ
— S4 per-key HLC(v3)、S2 Phase 2 Ownership 形(vN = 実装時の WIRE_MAX+1)、D2 複製、
PreVote — は本層上の版追加に縮退。§3 の版番号規律参照)。

## 8. 未決点(実装時判断でよいもの)

1. `WireError` の粒度(SyncPushError 等既存エラー型との統合か包含か)— 呼び出し側の
   既存エラーハンドリングを変えない形を優先。
2. WireCapCache のエントリ上限・掃除(膨張は peer 数オーダで実質有界だが、
   membership evict 連動の掃除を入れるかは任意 — 誤 flush の代償は JSON 1 往復のみ)。
3. B-2 ping 相乗り(deferred)の再評価タイミング — 長期分断復帰後の初回 JSON 交換が
   実測で問題化した場合のみ起票。
