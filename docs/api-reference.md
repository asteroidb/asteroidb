# AsteroidDB API リファレンス

本ドキュメントは AsteroidDB の HTTP API の網羅的なリファレンスである。
全エンドポイントのメソッド、パス、リクエスト/レスポンスの JSON スキーマ、エラーコード、認証方式、Content-Type ネゴシエーションについて記載する。

---

## 目次

1. [概要](#概要)
2. [認証](#認証)
3. [Content-Type ネゴシエーション](#content-type-ネゴシエーション)
4. [エラーレスポンス](#エラーレスポンス)
5. [Public API](#public-api)
   - [Eventual API](#eventual-api)
   - [セッショントークン](#セッショントークン)
   - [Certified API](#certified-api)
   - [Status API](#status-api)
   - [Control Plane API (読み取り)](#control-plane-api-読み取り)
   - [Metrics / SLO / Topology](#metrics--slo--topology)
   - [Health Check](#health-check)
6. [Internal API](#internal-api)
   - [Sync](#sync)
   - [Delta Sync](#delta-sync)
   - [Key Dump](#key-dump)
   - [Frontier](#frontier)
   - [Join / Leave](#join--leave)
   - [Announce](#announce)
   - [Ping](#ping)
7. [Control Plane API (書き込み)](#control-plane-api-書き込み)
8. [エラーコード一覧](#エラーコード一覧)
9. [CRDT 値型](#crdt-値型)
10. [CLI コマンドとの対応](#cli-コマンドとの対応)

---

## 概要

AsteroidDB は [Axum](https://github.com/tokio-rs/axum) ベースの HTTP API を公開する。
デフォルトのリッスンポートは `3000`。

API は大きく3つのカテゴリに分かれる:

| カテゴリ | パスプレフィックス | 認証 | 用途 |
|----------|-------------------|------|------|
| Public API | `/api/eventual/*`, `/api/certified/*`, `/api/status/*` | 不要 | クライアントからのデータ読み書き |
| Internal API | `/api/internal/*` | Bearer Token (設定時) | ノード間通信 (sync, membership) |
| Control Plane (書き込み) | `PUT /api/control-plane/*`, `DELETE /api/control-plane/*` | Bearer Token (設定時) | Authority 定義・配置ポリシーの変更 |

Control Plane の読み取りエンドポイント (`GET`) は認証不要で誰でもアクセス可能。

---

## 認証

### Bearer Token 認証

環境変数 `ASTEROIDB_INTERNAL_TOKEN` でトークンを設定すると、Internal API と Control Plane 書き込みエンドポイントに Bearer Token 認証が適用される。

```
Authorization: Bearer <token>
```

- トークンが設定されていない場合（または空文字列の場合）、全エンドポイントが認証なしでアクセス可能（後方互換モード）
- トークンの比較は constant-time 比較 (`subtle::ConstantTimeEq`) で行われ、タイミング攻撃を防止する
- 認証失敗時は `401 Unauthorized` が返る（ボディなし）

**認証が必要なエンドポイント:**

- `/api/internal/*` 全て
- `PUT /api/control-plane/authorities`
- `PUT /api/control-plane/policies`
- `DELETE /api/control-plane/policies/{prefix}`

**認証が不要なエンドポイント:**

- `/api/eventual/*`, `/api/certified/*`, `/api/status/*`
- `GET /api/control-plane/*`
- `/api/metrics`, `/api/slo`, `/api/topology`
- `/healthz`

### curl での認証例

```bash
# Internal API へのアクセス（トークン設定時）
curl -X POST http://localhost:3000/api/internal/join \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-2","address":"10.0.0.2:3000"}'
```

---

## Content-Type ネゴシエーション

### クライアント向け API (Public API)

Public API は **JSON のみ** をサポートする。

- リクエスト: `Content-Type: application/json`
- レスポンス: `application/json`

### Internal API (ノード間通信)

Internal API は 2 つのワイヤフォーマットをサポートする:

| MIME タイプ | フォーマット | 用途 |
|-------------|------------|------|
| `application/json` | JSON | 後方互換、デバッグ用 |
| `application/octet-stream` | bincode | デフォルト、コンパクトバイナリ |

**リクエスト時:**

- `Content-Type: application/octet-stream` → bincode としてデシリアライズ
- `Content-Type: application/json` またはヘッダなし → JSON としてデシリアライズ

**レスポンス時:**

- `Accept: application/octet-stream` → bincode でシリアライズ
- `Accept: application/json` またはヘッダなし → JSON にフォールバック

Accept ヘッダの解析は以下に対応する:

- 大文字小文字を区別しない MIME タイプマッチング
- カンマ区切りの複数タイプ (例: `application/octet-stream, application/json`)
- Quality value (例: `application/octet-stream;q=0.9`)
- `q=0` は「受け入れ不可」を意味する

bincode は JSON より大幅にペイロードサイズが小さく、ノード間通信のパフォーマンスを向上させる。

---

## エラーレスポンス

全てのエラーは以下の統一フォーマットで返される:

```json
{
  "error_code": "ERROR_CODE",
  "message": "人間が読めるエラーメッセージ"
}
```

HTTP ステータスコードはエラー種別に応じて設定される。詳細は[エラーコード一覧](#エラーコード一覧)を参照。

---

## Public API

### Eventual API

#### GET /api/eventual/{key}

ローカルの CRDT ストアからキーの現在値を取得する (eventual consistency)。

- **認証**: 不要
- **パスパラメータ**: `key` - 取得対象のキー
- **クエリパラメータ** (省略可、[セッショントークン](#セッショントークン) 参照):
  - `session_token` - 過去の write/read が返したトークン。ローカルレプリカがトークンの示す書き込みまで追いついている場合のみ値を返す（read-your-writes / monotonic reads）。空文字を渡すと前提条件なしでセッションを開始し、応答トークンのみ受け取る
  - `wait_ms` - 追いつくまで待機する最大時間（ミリ秒、サーバ側上限 5000）。超過時は 412
- **レスポンス**: `200 OK`、トークン不充足時 `412 Precondition Failed`、不正トークン時 `400 Bad Request`

**レスポンスボディ:**

```json
{
  "key": "hits",
  "value": {
    "type": "counter",
    "value": 42
  }
}
```

キーが存在しない場合、`value` は `null` になる（404 ではない）:

```json
{
  "key": "missing",
  "value": null
}
```

`session_token` クエリパラメータを付けた場合のみ、応答に `session_token` フィールドが追加される（付けない場合の応答バイト列は完全に従来通り）:

```json
{
  "key": "hits",
  "value": { "type": "counter", "value": 42 },
  "session_token": "v1:19704a1b2c3.0.6e6f64652d61"
}
```

ローカルレプリカがトークンの示す書き込みまで追いついていない場合、`wait_ms` の待機後も不充足なら 412 を返す（嘘の成功は返さない）:

```json
// HTTP 412 Precondition Failed
// Retry-After: 1
{
  "error_code": "SESSION_NOT_SATISFIED",
  "message": "session token not satisfied for key hits; retry, increase wait_ms, or try another replica"
}
```

**curl 例:**

```bash
# カウンタ値を取得
curl http://localhost:3000/api/eventual/hits

# ネストしたキーの取得
curl http://localhost:3000/api/eventual/users/alice/profile

# write が返したトークン付きで読む（read-your-writes）
curl "http://localhost:3000/api/eventual/hits?session_token=v1:19704a1b2c3.0.6e6f64652d61"

# 追いつくまで最大 3 秒待つ
curl "http://localhost:3000/api/eventual/hits?session_token=v1:19704a1b2c3.0.6e6f64652d61&wait_ms=3000"

# write なしでセッション開始（応答トークンのみ受け取る）
curl "http://localhost:3000/api/eventual/hits?session_token="
```

---

#### POST /api/eventual/write

CRDT 操作をローカルの eventual ストアに適用する。

- **認証**: 不要
- **Content-Type**: `application/json`
- **レスポンス**: `200 OK`

**リクエストボディ** (tagged union 形式):

操作の種別は `type` フィールドで指定する。以下の操作をサポート:

##### counter_inc — カウンタのインクリメント

```json
{
  "type": "counter_inc",
  "key": "hits"
}
```

##### counter_dec — カウンタのデクリメント

```json
{
  "type": "counter_dec",
  "key": "balance"
}
```

##### set_add — OR-Set への要素追加

```json
{
  "type": "set_add",
  "key": "users",
  "element": "alice"
}
```

##### set_remove — OR-Set からの要素削除

```json
{
  "type": "set_remove",
  "key": "users",
  "element": "alice"
}
```

**注意**: 存在しないキーに対する `set_remove` は `KEY_NOT_FOUND` エラー (404) を返す。

##### map_set — OR-Map へのエントリ設定

```json
{
  "type": "map_set",
  "key": "config",
  "map_key": "name",
  "map_value": "AsteroidDB"
}
```

##### map_delete — OR-Map からのエントリ削除

```json
{
  "type": "map_delete",
  "key": "config",
  "map_key": "name"
}
```

##### register_set — LWW-Register への値設定

```json
{
  "type": "register_set",
  "key": "greeting",
  "value": "hello"
}
```

**成功レスポンス:**

```json
{
  "ok": true,
  "session_token": "v1:19704a1b2c3.0.6e6f64652d61"
}
```

`session_token` はこの書き込みの HLC 位置を表す（常に返却される）。次の
`GET /api/eventual/{key}?session_token=...` に添付すると read-your-writes が保証される。
詳細は [セッショントークン](#セッショントークン) を参照。

**curl 例:**

```bash
# カウンタをインクリメント
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"page_views"}'

# OR-Set に要素を追加
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"tags","element":"distributed"}'

# LWW-Register に値を設定
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"sensor-1","value":"42.5"}'

# OR-Map にエントリを設定
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"map_set","key":"metadata","map_key":"region","map_value":"ap-northeast-1"}'
```

**エラーレスポンス例:**

型の不一致（既存のカウンタキーに set_add を実行した場合）:

```bash
curl -X POST http://localhost:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"set_add","key":"hits","element":"x"}'
```

```json
// HTTP 409 Conflict
{
  "error_code": "TYPE_MISMATCH",
  "message": "expected Set, got Counter"
}
```

---

### セッショントークン

Eventual API のオプション拡張として、クライアントセッション保証
（read-your-writes / monotonic reads）を提供する。サーバはセッション状態を
一切持たない（ステートレス）: 書き込み応答が返す不透明なトークンをクライアント
が次の読み取りに添付する方式である。

#### 保証内容

- **read-your-writes**: write が返したトークン付きの read が `200` を返すなら、
  応答値はその write を適用済み（または LWW 順序で支配済み）の状態から読まれて
  いる。追いついていないレプリカは `412` を返す — **嘘の成功は絶対に返さない**
  （不要な 412 = 偽陰性はあり得る）
- **monotonic reads**: read 応答のトークン（観測位置）を次の read に持ち回る
  ことで、別レプリカに切り替えても読み取りが時間的に巻き戻らない

#### トークン形式 (v1)

```text
token   := "v1:" entry ("," entry)*
entry   := physical-hex "." logical-hex "." nodeid-hex
```

- `physical`: u64 の 16 進表現（書き込み HLC の物理時刻 ms）
- `logical`: u32 の 16 進表現
- `nodeid-hex`: 起点ノード ID の UTF-8 バイト列の 16 進表現

例: `v1:19704a1b2c3.2.6e6f64652d61,19704b00000.0.6e6f64652d62`

文字集合は `[0-9a-f.,:v]` のみで URL エンコード不要。クライアントはトークンを
**不透明な文字列として扱う**こと（中身に依存しない）。

制約: 全長 8192 バイト以下、エントリ数 64 以下、ノード ID 128 バイト以下。
サーバが発行する応答トークンはこの制限内に収まるよう間引かれる（古い HLC の
エントリから削除）ため、**サーバ発行のトークンは常に次のリクエストでパース可能**。
エントリ 0 個の `v1:` は有効な空トークン（前提条件なし）で、可視 origin の
無い起動直後のレプリカが発行し得る。
`physical` がサーバ時計 + 60 秒を超えるトークンは 400 で拒否される
（クロック前進攻撃対策。トークンがサーバの HLC に取り込まれることはない）。

#### 寿命

トークンに有効期限はない。判定材料（origin 別適用済みフロンティア）は単調増加
かつ compaction の対象外のため、**古いトークンほど充足しやすい**。再起動後も
スナップショットに永続化される。

#### 保証の限界（重要）

- 保証は「トークンに記載された各 origin ノードの書き込み prefix の包含」のみの
  **保守的判定**である。線形化可能性や完全な因果一貫性は主張しない
  （HLC スカラ列による判定は過剰包含になり得る）
- Counter / Set キーは push 型複製のみの区間では pull 同期周期
  （デフォルト 2 秒）ぶんの 412 が発生し得る。Register キーは値レベル判定
  （LWW タイムスタンプ比較）により即時充足しやすい
- origin フロンティアの前進（applied claim）は「完全な受信」を証明できる
  転送に限られる: 送信側の `applied_origins` の養子縁組（要求 frontier が
  検証済み受信 prefix 以下、かつ送信側の prune フロア以上の pull）とフル
  ダンプのみ。**delta エントリ単体から entry の origin を claim することは
  ない**（送信側完全性は「受信側 ⊇ 送信側」しか証明せず、第三者 origin の
  prefix 保持を証明しないため）
- pull の要求 frontier は検証済み受信 prefix を基準にするため、push で
  同期フロンティアが先行しても claim は次の pull 周期（デフォルト 2 秒）で
  回復する。claim が作れない pull（送信側が prune 済み等）は同一周期内に
  フルシンクへフォールバックして検証済み受信 prefix を再確立する
- CRDT 型衝突（`TYPE_MISMATCH`）が発生したキーはセッション保証の対象外となる
  （恒久的に 412。トークン無しの read は引き続き可能）
- 可視性順序の保証であり**耐久性の保証ではない**（WAL 未実装のため、再起動で
  スナップショット以降の書き込みが失われた場合、トークンでは検出できない —
  自レプリカでは 412 になるだけで嘘は返さない）
- トークンは eventual ストア専用。certified read に流用しても充足しない
  （412 になるだけで害はない）
- 応答トークンはエントリ数上限 64・全長 8192 バイトで間引かれ得る（古い HLC
  のエントリから削除。直前に読んだキーの変更位置はリクエスト由来として優先
  保持される）。間引き後は記載されている origin についてのみ monotonic reads
  を保証する（保証の弱化であり嘘ではない）。書き込み origin が 64 を超える
  クラスタでは、間引かれた origin の書き込みについて読み取りが巻き戻る可能性
  がある — 完全な monotonic reads が必要な場合は origin 数を 64 以下に保つこと
- 応答トークンは applied claim 済みの origin だけでなく**可視状態全体**
  （unclaimed マージで見えるようになった寄与を含む）を被覆する。観測した値を
  被覆しないトークンを発行すると、別レプリカでの巻き戻り（monotonic reads の
  嘘）につながるため。過剰被覆は偽陰性（412）方向にしか作用しない
- トークンは無署名である。偽造・改竄されたトークンは自分の read が 412/400 に
  なるだけで他クライアントやサーバに害を与えない（BFT 拡張時に Ed25519 署名を
  検討予定）

#### 内部プロトコル互換性(運用注意)

セッション保証の導入で `DeltaSyncResponse` / `KeyDumpResponse`(Internal API)に
フィールドが追加された。JSON では後方互換だが、bincode
(`application/octet-stream`)は位置依存のため旧ノードの bincode 応答は新ノード
でデコードできない。新ノードの pull はデコード失敗時に **JSON で自動リトライ**
するため、ローリングアップグレード中も pull ベースの anti-entropy は JSON
経路で継続する(帯域効率は落ちる)。混在期間を短くするためアップグレードは
ロックステップ推奨(`KeyDumpResponse.timestamps` 追加時と同じ前例)。

スナップショット形式もバージョン 3 に更新された(セッション関連フィールドの
追加)。v1/v2 スナップショット(JSON / bincode とも)はロード時に自動マイグレー
ションされるが、v3 スナップショットを旧バージョンのコードで読むことはできない
(ダウングレード時はスナップショットの取り直しが必要)。

#### 利用例

```bash
# 1. 書き込み → トークンを得る
TOKEN=$(curl -s -X POST http://node-a:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"greeting","value":"hello"}' | jq -r .session_token)

# 2. 別ノードでトークン付き read（追いついていなければ 412）
curl "http://node-b:3000/api/eventual/greeting?session_token=$TOKEN"

# 3. 412 の場合: wait_ms 付きでリトライ（複製が届き次第 200）
curl "http://node-b:3000/api/eventual/greeting?session_token=$TOKEN&wait_ms=3000"

# 4. 応答の session_token を次の read に持ち回る（monotonic reads）
```

---

### Certified API

#### POST /api/certified/write

Authority ノード群の過半数合意によって認証が必要な値を書き込む。

- **認証**: 不要
- **Content-Type**: `application/json`
- **レスポンス**: `200 OK` (pending の場合) または `504 Gateway Timeout` (on_timeout=error の場合)

**リクエストボディ:**

```json
{
  "key": "sensor-1",
  "value": {
    "type": "counter",
    "value": 42
  },
  "on_timeout": "pending"
}
```

| フィールド | 型 | 必須 | デフォルト | 説明 |
|-----------|-----|------|-----------|------|
| `key` | string | はい | - | 書き込み先キー |
| `value` | CrdtValueJson | はい | - | 書き込む CRDT 値 |
| `on_timeout` | string | いいえ | `"pending"` | タイムアウト時の挙動: `"pending"` または `"error"` |

**`on_timeout` の動作:**

- `"pending"` — 合意タイムアウト時、ステータスを `Pending` として返す（クライアント側で後からポーリング可能）
- `"error"` — 合意タイムアウト時、`504 Gateway Timeout` + `TIMEOUT` エラーを返す

**成功レスポンス:**

```json
{
  "status": "Pending"
}
```

`status` は `"Pending"` または `"Certified"` のいずれか。
`"Timeout"` は `on_timeout=error` の場合にのみ返され、その場合の HTTP ステータスは `504 Gateway Timeout`（このレスポンスフォーマットではなくエラーレスポンスになる）。
`"Rejected"` は `certified_write` の直接のレスポンスには含まれず、後から `GET /api/status/{key}` でポーリングした際にのみ観測される

**curl 例:**

```bash
# 認証書き込み（デフォルト: on_timeout=pending）
curl -X POST http://localhost:3000/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{"key":"sensor","value":{"type":"counter","value":100}}'

# タイムアウト時にエラーを返す
curl -X POST http://localhost:3000/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{"key":"sensor","value":{"type":"register","value":"critical"},"on_timeout":"error"}'
```

---

#### GET /api/certified/{key}

認証ステータス、frontier、証明バンドル付きの値を取得する。

- **認証**: 不要
- **パスパラメータ**: `key` - 取得対象のキー
- **レスポンス**: `200 OK`

**レスポンスボディ:**

```json
{
  "key": "sensor-1",
  "value": {
    "type": "counter",
    "value": 42
  },
  "status": "Certified",
  "frontier": {
    "physical": 1700000000000,
    "logical": 5,
    "node_id": "auth-1"
  },
  "proof": {
    "key_range_prefix": "sensor",
    "frontier": {
      "physical": 1700000000000,
      "logical": 5,
      "node_id": "auth-1"
    },
    "policy_version": 1,
    "contributing_authorities": ["auth-1", "auth-2"],
    "total_authorities": 3,
    "certificate": {
      "keyset_version": 1,
      "signatures": [
        {
          "authority_id": "auth-1",
          "public_key": "aabbccdd...（hex エンコード、32バイト）",
          "signature": "11223344...（hex エンコード、64バイト）",
          "keyset_version": 1
        }
      ]
    }
  }
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `key` | string | 要求したキー |
| `value` | CrdtValueJson \| null | CRDT 値（存在しない場合は null） |
| `status` | string | 認証ステータス: `"Pending"`, `"Certified"`, `"Rejected"`, `"Timeout"` のいずれか。`Rejected` は後から certification 評価で拒否された場合にのみ出現する |
| `frontier` | FrontierJson \| null | HLC frontier（証明時のタイムスタンプ） |
| `proof` | ProofBundleJson \| null | 検証可能な証明バンドル |

**curl 例:**

```bash
curl http://localhost:3000/api/certified/sensor-1
```

---

#### POST /api/certified/verify

証明バンドルを受け取り、Authority 合意の検証結果を返す。
外部クライアントが独立して証明を検証するために使用する。

- **認証**: 不要
- **Content-Type**: `application/json`
- **レスポンス**: `200 OK`
- **前提条件**: このエンドポイントは `ASTEROIDB_BLS_SEED` 環境変数が設定されている必要がある（keyset registry の初期化に使用）。未設定の場合は `500 Internal Server Error`（`"keyset registry not configured; cannot verify proofs"`）を返す

**リクエストボディ:**

```json
{
  "key_range_prefix": "sensor",
  "frontier": {
    "physical": 1700000000000,
    "logical": 5,
    "node_id": "auth-1"
  },
  "policy_version": 1,
  "contributing_authorities": ["auth-1", "auth-2"],
  "total_authorities": 3,
  "certificate": {
    "keyset_version": 1,
    "signatures": [
      {
        "authority_id": "auth-1",
        "public_key": "aabbccdd...",
        "signature": "11223344...",
        "keyset_version": 1
      }
    ]
  },
  "format_version": 1,
  "signature_algorithm": "Ed25519"
}
```

| フィールド | 型 | 必須 | 説明 |
|-----------|-----|------|------|
| `key_range_prefix` | string | はい | 証明のキー範囲プレフィックス |
| `frontier` | FrontierJson | はい | 認証時の majority frontier |
| `policy_version` | u64 | はい | 有効なポリシーバージョン |
| `contributing_authorities` | string[] | はい | 証明に参加した Authority ノード ID |
| `total_authorities` | usize | はい | Authority セット内の総ノード数 |
| `certificate` | CertificateJson | いいえ | 暗号署名付き証明書 |
| `format_version` | u32 | いいえ | 証明書フォーマットバージョン（指定時にバージョン互換チェック実施） |
| `signature_algorithm` | string | いいえ | 署名アルゴリズム: `"Ed25519"` (デフォルト) または `"Bls12_381"` |
| `keyset_version` | u64 | いいえ | BLS 検証で使用する keyset バージョン（省略時は `certificate.keyset_version`、それも無ければ 1） |
| `bls_aggregate_signature` | string | いいえ | hex エンコード BLS 集約署名。`bls_signer_ids` / `bls_public_keys` と共に指定し `signature_algorithm` が `"Bls12_381"` の場合、keyset registry に対する BLS 集約検証を実施 |
| `bls_signer_ids` | string[] | いいえ | BLS 署名者ノード ID（`bls_public_keys` と同順） |
| `bls_public_keys` | string[] | いいえ | hex エンコード BLS 公開鍵（registry 鍵との一致を要求） |

> `GET /api/certified/{key}` の `proof` オブジェクトはこのリクエストボディとして
> そのまま送信できる（round-trip 可能）。署名パイプラインが有効な場合、`proof` には
> `signature_algorithm` / `keyset_version` / BLS フィールドが自動的に含まれる。
> 証明書の `frontier` は Authority が署名したチェックポイント HLC
> （1 秒単位に床丸めされた値、`logical=0`, `node_id=""`）である点に注意。

**レスポンスボディ:**

```json
{
  "valid": true,
  "has_majority": true,
  "contributing_count": 2,
  "required_count": 2
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `valid` | bool | 証明全体の有効性 |
| `has_majority` | bool | 過半数の Authority が参加しているか |
| `contributing_count` | usize | 参加 Authority 数 |
| `required_count` | usize | 過半数に必要な Authority 数 |

**curl 例:**

```bash
curl -X POST http://localhost:3000/api/certified/verify \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "",
    "frontier": {"physical": 1700000000000, "logical": 0, "node_id": "auth-1"},
    "policy_version": 1,
    "contributing_authorities": ["auth-1", "auth-2"],
    "total_authorities": 3
  }'
```

---

### Status API

#### GET /api/status/{key}

指定キーの最新書き込みに対する認証ステータスを返す。

- **認証**: 不要
- **パスパラメータ**: `key` - 対象キー
- **レスポンス**: `200 OK`

**レスポンスボディ:**

```json
{
  "key": "sensor-1",
  "status": "Certified"
}
```

`status` は `"Pending"`, `"Certified"`, `"Rejected"`, `"Timeout"` のいずれか。

**curl 例:**

```bash
curl http://localhost:3000/api/status/sensor-1
```

---

### Control Plane API (読み取り)

以下は全て認証不要の読み取り専用エンドポイント。

#### GET /api/control-plane/authorities

全ての Authority 定義を返す。

- **レスポンス**: `200 OK`

```json
[
  {
    "key_range_prefix": "",
    "authority_nodes": ["auth-1", "auth-2", "auth-3"]
  },
  {
    "key_range_prefix": "user/",
    "authority_nodes": ["auth-1", "auth-4", "auth-5"]
  }
]
```

**curl 例:**

```bash
curl http://localhost:3000/api/control-plane/authorities
```

---

#### GET /api/control-plane/authorities/{prefix}

指定キー範囲プレフィックスの Authority 定義を返す。

- **パスパラメータ**: `prefix` - キー範囲プレフィックス
- **レスポンス**: `200 OK` / `404 Not Found`

```json
{
  "key_range_prefix": "user/",
  "authority_nodes": ["auth-1", "auth-4", "auth-5"]
}
```

**curl 例:**

```bash
curl http://localhost:3000/api/control-plane/authorities/user%2F
```

---

#### GET /api/control-plane/policies

全ての配置ポリシーを返す。

- **レスポンス**: `200 OK`

```json
[
  {
    "key_range_prefix": "",
    "version": 1,
    "replica_count": 3,
    "required_tags": [],
    "forbidden_tags": [],
    "allow_local_write_on_partition": false,
    "certified": false
  }
]
```

**curl 例:**

```bash
curl http://localhost:3000/api/control-plane/policies
```

---

#### GET /api/control-plane/policies/{prefix}

指定キー範囲プレフィックスの配置ポリシーを返す。

- **パスパラメータ**: `prefix` - キー範囲プレフィックス
- **レスポンス**: `200 OK` / `404 Not Found`

```json
{
  "key_range_prefix": "user/",
  "version": 2,
  "replica_count": 3,
  "required_tags": ["region:ap-northeast-1"],
  "forbidden_tags": ["decommissioned"],
  "allow_local_write_on_partition": true,
  "certified": true
}
```

**curl 例:**

```bash
curl http://localhost:3000/api/control-plane/policies/user%2F
```

---

#### GET /api/control-plane/versions

System Namespace のバージョン履歴を返す。

- **レスポンス**: `200 OK`

```json
{
  "current_version": 3,
  "history": [1, 2, 3]
}
```

**curl 例:**

```bash
curl http://localhost:3000/api/control-plane/versions
```

---

### Metrics / SLO / Topology

#### GET /api/metrics

ランタイム運用メトリクスのスナップショットを返す。

- **認証**: 不要
- **レスポンス**: `200 OK`

**レスポンスボディ:**

```json
{
  "pending_count": 5,
  "certified_total": 1200,
  "certification_latency_mean_us": 3456.78,
  "frontier_skew_ms": 15,
  "sync_failure_rate": 0.001,
  "sync_attempt_total": 50000,
  "sync_failure_total": 50,
  "peer_sync": {
    "node-2": {
      "mean_latency_us": 1500.0,
      "p99_latency_us": 5000.0,
      "success_count": 1000,
      "failure_count": 2
    }
  },
  "certification_latency_window": {
    "sample_count": 500,
    "mean_us": 3000.0,
    "p99_us": 8000.0
  },
  "rebalance_start_total": 3,
  "rebalance_keys_migrated": 150,
  "rebalance_keys_failed": 0,
  "rebalance_complete_total": 3,
  "rebalance_duration_sum_us": 5000000,
  "key_rotation_total": 2,
  "key_rotation_last_version": 3,
  "key_rotation_last_time_ms": 1700000000000,
  "write_ops_total": 25000,
  "delta_sync_count": 8000
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `pending_count` | u64 | 現在保留中の認証書き込み数 |
| `certified_total` | u64 | 累計認証済み書き込み数 |
| `certification_latency_mean_us` | f64 | 認証レイテンシの平均値 (マイクロ秒) |
| `frontier_skew_ms` | u64 | Authority スコープ間の最大 frontier スキュー (ミリ秒) |
| `sync_failure_rate` | f64 | 同期失敗率 (0.0 - 1.0) |
| `sync_attempt_total` | u64 | 累計同期試行回数 |
| `sync_failure_total` | u64 | 累計同期失敗回数 |
| `peer_sync` | map | ピアごとの同期統計（スライディングウィンドウ） |
| `certification_latency_window` | object | 認証レイテンシウィンドウ統計 |
| `rebalance_start_total` | u64 | 累計リバランス開始回数 |
| `rebalance_keys_migrated` | u64 | 累計リバランス移行キー数 |
| `rebalance_keys_failed` | u64 | 累計リバランス失敗キー数 |
| `rebalance_complete_total` | u64 | 累計リバランス完了回数 |
| `rebalance_duration_sum_us` | u64 | リバランス所要時間合計 (マイクロ秒) |
| `key_rotation_total` | u64 | 累計鍵ローテーション回数 |
| `key_rotation_last_version` | u64 | 最新 keyset バージョン |
| `key_rotation_last_time_ms` | u64 | 最新ローテーション時刻 (ミリ秒) |
| `write_ops_total` | u64 | 累計書き込みオペレーション数 |
| `delta_sync_count` | u64 | 累計デルタ同期回数 |
| `full_sync_fallback_count` | u64 | 累計フルシンクフォールバック回数 |
| `full_sync_fallback_ratio` | f64 | フルシンク比率 (0.0 - 1.0) |

**curl 例:**

```bash
curl http://localhost:3000/api/metrics
```

---

#### GET /api/slo

全 SLO バジェットのスナップショットを返す。

- **認証**: 不要
- **レスポンス**: `200 OK`

**レスポンスボディ:**

```json
{
  "budgets": {
    "eventual_read_p99": {
      "target": {
        "name": "eventual_read_p99",
        "kind": "LessThan",
        "target_value": 50.0,
        "target_percentage": 99.0,
        "window_secs": 3600
      },
      "total_requests": 10000,
      "violations": 5,
      "budget_remaining": 95.0,
      "is_warning": false,
      "is_critical": false
    },
    "certified_read_p99": {
      "target": {
        "name": "certified_read_p99",
        "kind": "LessThan",
        "target_value": 500.0,
        "target_percentage": 99.0,
        "window_secs": 3600
      },
      "total_requests": 5000,
      "violations": 2,
      "budget_remaining": 96.0,
      "is_warning": false,
      "is_critical": false
    }
  }
}
```

**事前定義 SLO:**

| SLO名 | 基準値 | パーセンタイル | ウィンドウ |
|--------|--------|-------------|----------|
| `eventual_read_p99` | < 50ms | 99% | 1時間 |
| `certified_read_p99` | < 500ms | 99% | 1時間 |
| `replication_convergence` | - | - | 1時間 |
| `authority_availability` | - | - | 1時間 |

**curl 例:**

```bash
curl http://localhost:3000/api/slo
```

---

#### GET /api/topology

クラスタトポロジービューをリージョン別にグループ化して返す。
リージョン間のレイテンシ情報を含む。

- **認証**: 不要
- **レスポンス**: `200 OK`

```json
{
  "regions": [
    {
      "name": "ap-northeast-1",
      "node_count": 2,
      "node_ids": ["node-1", "node-2"],
      "inter_region_latency_ms": {}
    }
  ],
  "total_nodes": 2
}
```

**curl 例:**

```bash
curl http://localhost:3000/api/topology
```

---

### Health Check

#### GET /healthz

ロードバランサーやオーケストレーター向けのヘルスチェックエンドポイント。
認証ミドルウェアの外側に配置されるため、認証なしでアクセス可能。

- **認証**: 不要
- **レスポンス**: `200 OK`

```json
{
  "status": "ok"
}
```

**curl 例:**

```bash
curl http://localhost:3000/healthz
```

---

## Internal API

Internal API はノード間通信に使用される。
`ASTEROIDB_INTERNAL_TOKEN` が設定されている場合、全て Bearer Token 認証が必要。

以下の curl 例では認証ヘッダを省略しているが、トークン設定時は `-H "Authorization: Bearer <token>"` を付与する必要がある。

### Sync

#### POST /api/internal/sync

リモートピアから CRDT 値を受け取り、ローカルの eventual ストアに `merge_remote` でマージする。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json` または `application/octet-stream`
- **Accept**: `application/json` または `application/octet-stream`

**リクエストボディ (JSON):**

```json
{
  "sender": "node-2",
  "entries": {
    "hits": {
      "Counter": {
        "increments": {"node-2": 5},
        "decrements": {}
      }
    }
  }
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `sender` | string | 送信元ノード ID |
| `entries` | map<string, CrdtValue> | キーと CRDT 値のマップ |

**レスポンスボディ:**

```json
{
  "merged": 3,
  "errors": [
    {
      "key": "bad-key",
      "error": "type mismatch"
    }
  ]
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `merged` | usize | 正常にマージされたエントリ数 |
| `errors` | array | マージ失敗したエントリのリスト |

**curl 例:**

```bash
curl -X POST http://localhost:3000/api/internal/sync \
  -H "Content-Type: application/json" \
  -d '{"sender":"node-2","entries":{}}'
```

---

### Delta Sync

#### POST /api/internal/sync/delta

デルタ同期リクエストを受け取り、指定 frontier 以降に変更されたエントリを返す。
インクリメンタルな anti-entropy 同期に使用する。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json` または `application/octet-stream`
- **Accept**: `application/json` または `application/octet-stream`

**リクエストボディ (JSON):**

```json
{
  "sender": "node-2",
  "frontier": {
    "physical": 1700000000000,
    "logical": 5,
    "node_id": "node-2"
  }
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `sender` | string | 送信元ノード ID |
| `frontier` | HlcTimestamp | この HLC 以降の変更を要求 |

**レスポンスボディ:**

```json
{
  "entries": [
    {
      "key": "hits",
      "value": { "Counter": { "increments": {"node-1": 10}, "decrements": {} } },
      "hlc": { "physical": 1700000001000, "logical": 0, "node_id": "node-1" }
    }
  ],
  "sender_frontier": {
    "physical": 1700000001000,
    "logical": 0,
    "node_id": "node-1"
  },
  "applied_origins": {
    "node-1": { "physical": 1700000001000, "logical": 0, "node_id": "node-1" }
  },
  "merge_failed_keys": [],
  "pruned_floor": null,
  "visible_origins": {
    "node-1": { "physical": 1700000001000, "logical": 0, "node_id": "node-1" }
  }
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `entries` | array | frontier 以降に変更されたエントリ |
| `sender_frontier` | HlcTimestamp \| null | 送信元の現在の frontier |
| `applied_origins` | map<string, HlcTimestamp> | 応答側の origin 別適用済みフロンティア（セッション保証のフロンティア養子縁組に使用） |
| `merge_failed_keys` | array<string> | 応答側でマージ失敗（型衝突）したキー |
| `pruned_floor` | HlcTimestamp \| null | 応答側の prune 済みフロンティア。要求 frontier がこれ未満の場合、受信側は `applied_origins` を採用しない |
| `visible_origins` | map<string, HlcTimestamp> | 応答側の origin 別**可視**フロンティア（`applied_origins` の上位集合）。受信側は無条件に max マージし、応答セッショントークンの被覆に使う |

**curl 例:**

```bash
curl -X POST http://localhost:3000/api/internal/sync/delta \
  -H "Content-Type: application/json" \
  -d '{"sender":"node-2","frontier":{"physical":0,"logical":0,"node_id":"node-2"}}'
```

---

### Key Dump

#### GET /api/internal/keys

eventual ストアの全キーバリューペアを、ストアの現在 frontier HLC とともに返す。
リモートピアによるプルベースの anti-entropy 同期（フルシンク）に使用する。

- **認証**: Bearer Token (設定時)
- **Accept**: `application/json` または `application/octet-stream`

**レスポンスボディ:**

```json
{
  "entries": {
    "hits": { "Counter": { "increments": {"node-1": 10}, "decrements": {} } },
    "greeting": { "Register": { "value": "hello", "timestamp": {...} } }
  },
  "frontier": {
    "physical": 1700000001000,
    "logical": 0,
    "node_id": "node-1"
  },
  "timestamps": {
    "hits": { "physical": 1700000001000, "logical": 0, "node_id": "node-1" }
  },
  "applied_origins": {
    "node-1": { "physical": 1700000001000, "logical": 0, "node_id": "node-1" }
  },
  "merge_failed_keys": [],
  "visible_origins": {
    "node-1": { "physical": 1700000001000, "logical": 0, "node_id": "node-1" }
  }
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `entries` | map<string, CrdtValue> | 全キーバリューペア |
| `frontier` | HlcTimestamp \| null | ストアの現在 frontier |
| `timestamps` | map<string, HlcTimestamp> | キーごとの HLC タイムスタンプ |
| `applied_origins` | map<string, HlcTimestamp> | 応答側の origin 別適用済みフロンティア。フルダンプは完全状態のため、受信側は全エントリ適用後に無条件で採用してよい |
| `merge_failed_keys` | array<string> | 応答側でマージ失敗（型衝突）したキー |
| `visible_origins` | map<string, HlcTimestamp> | 応答側の origin 別可視フロンティア（`applied_origins` の上位集合、応答セッショントークンの被覆に使用） |

**curl 例:**

```bash
curl http://localhost:3000/api/internal/keys
```

---

### Frontier

#### POST /api/internal/frontiers

ピアから frontier 更新を受け取り、ローカルの `AckFrontierSet` に適用する。
単調性は `AckFrontierSet::update()` によって保証される。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json` または `application/octet-stream`
- **Accept**: `application/json` または `application/octet-stream`

**リクエストボディ (JSON):**

```json
{
  "frontiers": [
    {
      "authority_id": "auth-1",
      "frontier_hlc": { "physical": 1700000000000, "logical": 0, "node_id": "auth-1" },
      "key_range": { "prefix": "user/" },
      "policy_version": 1,
      "digest_hash": "abc123"
    }
  ]
}
```

**レスポンスボディ:**

```json
{
  "accepted": 1
}
```

**curl 例:**

```bash
curl -X POST http://localhost:3000/api/internal/frontiers \
  -H "Content-Type: application/json" \
  -d '{"frontiers":[]}'
```

---

#### GET /api/internal/frontiers

このノードが現在追跡している全ての frontier を返す。

- **認証**: Bearer Token (設定時)
- **Accept**: `application/json` または `application/octet-stream`

**レスポンスボディ:**

```json
{
  "frontiers": [
    {
      "authority_id": "auth-1",
      "frontier_hlc": { "physical": 1700000000000, "logical": 0, "node_id": "auth-1" },
      "key_range": { "prefix": "" },
      "policy_version": 1,
      "digest_hash": "abc123"
    }
  ]
}
```

**curl 例:**

```bash
curl http://localhost:3000/api/internal/frontiers
```

---

### Join / Leave

#### POST /api/internal/join

新規ノードがシードノードに対して送信し、クラスタに参加する。
シードノードは参加ノードを peer registry に追加し、現在のピアリストと system namespace のスナップショットを返す。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`

**リクエストボディ:**

```json
{
  "node_id": "node-2",
  "address": "10.0.0.2:3000",
  "tags": ["region:ap-northeast-1", "zone:az-1"]
}
```

| フィールド | 型 | 必須 | 説明 |
|-----------|-----|------|------|
| `node_id` | string | はい | 参加ノードの一意識別子 |
| `address` | string | はい | 参加ノードのリッスンアドレス (`host:port`) |
| `tags` | string[] | いいえ | 参加ノードに関連付けるタグ |

**アドレスバリデーション:**
- `host:port` 形式のみ許可（スキーム `http://` 等は不可）
- パス・クエリ文字列を含むアドレスは拒否（SSRF 防止）
- IPv6 は `[::1]:3000` 形式をサポート

**レスポンスボディ:**

```json
{
  "peers": [
    { "node_id": "node-1", "address": "10.0.0.1:3000" },
    { "node_id": "node-2", "address": "10.0.0.2:3000" }
  ],
  "namespace": {
    "authority_definitions": [...],
    "placement_policies": [...],
    "version": 1
  }
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `peers` | PeerInfo[] | 現在のピアリスト（シードノード自身を含む） |
| `namespace` | object | system namespace の JSON スナップショット |

**curl 例:**

```bash
curl -X POST http://localhost:3000/api/internal/join \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-2","address":"10.0.0.2:3000","tags":["region:us-east-1"]}'
```

---

#### POST /api/internal/leave

ノードがグレースフルにクラスタから離脱する。受信ノードは離脱ノードを peer registry から削除する。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`

**リクエストボディ:**

```json
{
  "node_id": "node-2"
}
```

**レスポンスボディ:**

```json
{
  "success": true
}
```

`success` が `false` の場合、指定された node_id が peer registry に存在しなかったことを意味する。

**curl 例:**

```bash
curl -X POST http://localhost:3000/api/internal/leave \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-2"}'
```

---

### Announce

#### POST /api/internal/announce

ピアにメンバーシップの変更を通知する。参加 (`joining: true`) または離脱 (`joining: false`) を全ピアにブロードキャストするために使用する。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`

**リクエストボディ:**

```json
{
  "node_id": "node-2",
  "address": "10.0.0.2:3000",
  "joining": true
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `node_id` | string | アナウンスするノードの ID |
| `address` | string | ノードのリッスンアドレス (`host:port`) |
| `joining` | bool | `true` = 参加、`false` = 離脱 |

**レスポンスボディ:**

```json
{
  "accepted": true
}
```

**curl 例:**

```bash
# ノード参加のアナウンス
curl -X POST http://localhost:3000/api/internal/announce \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-3","address":"10.0.0.3:3000","joining":true}'

# ノード離脱のアナウンス
curl -X POST http://localhost:3000/api/internal/announce \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-3","address":"10.0.0.3:3000","joining":false}'
```

---

### Ping

#### POST /api/internal/ping

軽量な gossip ベースのピアリスト交換エンドポイント。
送信者は自身の既知ピアリスト（ダイジェスト）を送信し、受信者は差分を検出して自身の既知ピアリストを返す。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`

**リクエストボディ:**

```json
{
  "sender_id": "node-2",
  "sender_addr": "10.0.0.2:3000",
  "known_peers": [
    { "node_id": "node-1", "address": "10.0.0.1:3000" },
    { "node_id": "node-2", "address": "10.0.0.2:3000" }
  ]
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `sender_id` | string | 送信者のノード ID |
| `sender_addr` | string | 送信者のリッスンアドレス |
| `known_peers` | PeerInfo[] | 送信者が認識しているピアリスト |

**レスポンスボディ:**

```json
{
  "known_peers": [
    { "node_id": "node-1", "address": "10.0.0.1:3000" },
    { "node_id": "node-2", "address": "10.0.0.2:3000" },
    { "node_id": "node-3", "address": "10.0.0.3:3000" }
  ]
}
```

**セキュリティ制約:**

- 未知の送信者からのピアリスト注入を防止するため、送信者が既知ピアである場合のみピアリストの差分を取り込む
- 1回の ping で追加できる新規ピアは最大 10 件（peer-list poisoning 対策）
- 認証トークンが設定されている場合、認証済みの未知送信者のみ自動追加される

**curl 例:**

```bash
curl -X POST http://localhost:3000/api/internal/ping \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{
    "sender_id": "node-2",
    "sender_addr": "10.0.0.2:3000",
    "known_peers": [{"node_id":"node-1","address":"10.0.0.1:3000"}]
  }'
```

---

## Control Plane API (書き込み)

以下のエンドポイントは全て、Authority ノードの過半数承認 (FR-009) と Bearer Token 認証（設定時）が必要。

### PUT /api/control-plane/authorities

Authority 定義を設定する。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`

**リクエストボディ:**

```json
{
  "key_range_prefix": "user/",
  "authority_nodes": ["auth-1", "auth-2", "auth-3"],
  "approvals": ["auth-1", "auth-2"]
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `key_range_prefix` | string | 対象キー範囲プレフィックス |
| `authority_nodes` | string[] | Authority ノード ID のリスト |
| `approvals` | string[] | この更新を承認したノード ID（過半数必要） |

**レスポンスボディ:**

```json
{
  "key_range_prefix": "user/",
  "authority_nodes": ["auth-1", "auth-2", "auth-3"]
}
```

**curl 例:**

```bash
curl -X PUT http://localhost:3000/api/control-plane/authorities \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "user/",
    "authority_nodes": ["auth-1", "auth-2", "auth-3"],
    "approvals": ["auth-1", "auth-2"]
  }'
```

---

### PUT /api/control-plane/policies

配置ポリシーを設定する。バージョンは自動的にインクリメントされる。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`

**リクエストボディ:**

```json
{
  "key_range_prefix": "user/",
  "replica_count": 3,
  "required_tags": ["region:ap-northeast-1"],
  "forbidden_tags": ["decommissioned"],
  "allow_local_write_on_partition": true,
  "certified": true,
  "approvals": ["auth-1", "auth-2"]
}
```

| フィールド | 型 | 必須 | デフォルト | 説明 |
|-----------|-----|------|-----------|------|
| `key_range_prefix` | string | はい | - | 対象キー範囲プレフィックス |
| `replica_count` | usize | はい | - | レプリカ数（1以上） |
| `required_tags` | string[] | いいえ | `[]` | 必須タグ |
| `forbidden_tags` | string[] | いいえ | `[]` | 禁止タグ |
| `allow_local_write_on_partition` | bool | いいえ | `false` | ネットワーク分断時のローカル書き込み許可 |
| `certified` | bool | いいえ | `false` | 認証が必要なキー範囲かどうか |
| `approvals` | string[] | はい | - | 承認ノード ID（過半数必要） |

**レスポンスボディ:**

```json
{
  "key_range_prefix": "user/",
  "version": 2,
  "replica_count": 3,
  "required_tags": ["region:ap-northeast-1"],
  "forbidden_tags": ["decommissioned"],
  "allow_local_write_on_partition": true,
  "certified": true
}
```

**エラー例:**

```bash
# replica_count が 0 の場合
curl -X PUT http://localhost:3000/api/control-plane/policies \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{"key_range_prefix":"","replica_count":0,"approvals":["auth-1","auth-2"]}'
```

```json
// HTTP 400 Bad Request
{
  "error_code": "INVALID_ARGUMENT",
  "message": "replica_count must be at least 1"
}
```

**curl 例:**

```bash
curl -X PUT http://localhost:3000/api/control-plane/policies \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "",
    "replica_count": 3,
    "required_tags": [],
    "forbidden_tags": [],
    "allow_local_write_on_partition": false,
    "certified": false,
    "approvals": ["auth-1", "auth-2"]
  }'
```

---

### DELETE /api/control-plane/policies/{prefix}

指定キー範囲プレフィックスの配置ポリシーを削除する。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`
- **パスパラメータ**: `prefix` - 削除対象のキー範囲プレフィックス

**リクエストボディ:**

```json
{
  "approvals": ["auth-1", "auth-2"]
}
```

**レスポンスボディ:**

削除されたポリシーの内容を返す:

```json
{
  "key_range_prefix": "user/",
  "version": 2,
  "replica_count": 3,
  "required_tags": [],
  "forbidden_tags": [],
  "allow_local_write_on_partition": false,
  "certified": false
}
```

存在しないプレフィックスを指定した場合は `404 Not Found` + `KEY_NOT_FOUND` エラー。

**curl 例:**

```bash
curl -X DELETE http://localhost:3000/api/control-plane/policies/user%2F \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{"approvals":["auth-1","auth-2"]}'
```

---

## エラーコード一覧

| エラーコード | HTTP ステータス | 説明 | 対処方法 |
|-------------|----------------|------|----------|
| `INVALID_ARGUMENT` | 400 Bad Request | リクエストパラメータが不正 | リクエストボディを確認し、必須フィールドが正しい形式であることを確認する |
| `INVALID_OP` | 400 Bad Request | 指定した CRDT 型に対して無効な操作 | 操作対象のキーの CRDT 型を確認する |
| `TYPE_MISMATCH` | 409 Conflict | 既存キーの CRDT 型と操作の型が不一致 | キーに対して正しい型の操作を使用する（例: Counter キーには `counter_inc`/`counter_dec`） |
| `KEY_NOT_FOUND` | 404 Not Found | 指定キーが存在しない | キー名を確認する。`set_remove`/`map_delete` は事前にキーが存在する必要がある |
| `STALE_VERSION` | 409 Conflict | 古いバージョンでの書き込み | 最新値を再取得してリトライする |
| `POLICY_DENIED` | 403 Forbidden | 配置ポリシーによる拒否 | 対象キー範囲の配置ポリシーを確認する |
| `TIMEOUT` | 504 Gateway Timeout | Authority 合意がタイムアウト | Authority ノードの稼働状況を確認する。`on_timeout=pending` で再試行可能 |
| `SESSION_NOT_SATISFIED` | 412 Precondition Failed | セッショントークンの示す書き込みまでローカルレプリカが追いついていない | `Retry-After` に従いリトライ、`wait_ms` を増やす、または別レプリカに問い合わせる |
| `INCOMPATIBLE_VERSION` | 500 Internal Server Error | データバージョンとコードバージョンの不整合 | AsteroidDB を最新版に更新するか、データマイグレーションを実行する |
| `MIGRATION_FAILED` | 500 Internal Server Error | データマイグレーション失敗 | ログを確認し、データの整合性を検証する |
| `INTERNAL` | 500 Internal Server Error | 内部エラー | サーバーログを確認する |

---

## CRDT 値型

API で扱う CRDT 値は tagged union 形式で表現される。
`type` フィールドで型を判別する。

### Counter (PN-Counter)

増減可能なカウンタ。

```json
{
  "type": "counter",
  "value": 42
}
```

### Set (OR-Set)

要素の追加・削除が可能な集合。Add-wins セマンティクス。

```json
{
  "type": "set",
  "elements": ["alice", "bob", "charlie"]
}
```

`elements` はソート済みで返される。

### Map (OR-Map)

キーバリューペアを持つマップ。

```json
{
  "type": "map",
  "entries": {
    "name": "AsteroidDB",
    "region": "ap-northeast-1"
  }
}
```

### Register (LWW-Register)

Last-Writer-Wins セマンティクスのレジスタ。

```json
{
  "type": "register",
  "value": "hello"
}
```

値が未設定の場合:

```json
{
  "type": "register",
  "value": null
}
```

---

## CLI コマンドとの対応

`asteroidb-cli` の各コマンドは以下の API エンドポイントにマッピングされる:

| CLI コマンド | HTTP メソッド | エンドポイント | 説明 |
|-------------|-------------|---------------|------|
| `asteroidb-cli status` | GET | `/api/metrics` | ノードステータスのサマリ表示 |
| `asteroidb-cli get <key>` | GET | `/api/eventual/{key}` | eventual ストアからの値取得。`--session-token <tok>` / `--session` / `--wait-ms <n>` でセッション保証付き読み取り |
| `asteroidb-cli put <key> <value>` | POST | `/api/eventual/write` | LWW-Register への値書き込み。`OK` の次行に `session_token: <tok>` を表示 |
| `asteroidb-cli metrics` | GET | `/api/metrics` | 詳細ランタイムメトリクス |
| `asteroidb-cli slo` | GET | `/api/slo` | SLO バジェット状況 |

### CLI の接続先設定

```bash
# デフォルト: 127.0.0.1:3000
asteroidb-cli status

# 環境変数で設定
ASTEROIDB_HOST=10.0.0.1:3000 asteroidb-cli status

# コマンドラインオプションで設定
asteroidb-cli --host 10.0.0.1:3000 status
```

### CLI の put コマンドの内部動作

`put` コマンドは内部的に `register_set` 操作を使用する:

```bash
# CLI コマンド
asteroidb-cli put sensor-1 42.5

# 内部的に送信される JSON
# POST /api/eventual/write
# {"type":"register_set","key":"sensor-1","value":"42.5"}
```
