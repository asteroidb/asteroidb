# AsteroidDB API リファレンス

本ドキュメントは AsteroidDB の HTTP API の網羅的なリファレンスである。
全エンドポイントのメソッド、パス、リクエスト/レスポンスの JSON スキーマ、エラーコード、認証方式、Content-Type ネゴシエーションについて記載する。

---

## 目次

1. [概要](#概要)
   - [操作モードと CRDT API](#操作モードと-crdt-api)
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
   - [Certified Replication](#certified-replication)
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
| Public API | `/api/eventual/*`, `/api/certified/*`, `/api/status/*`, `/api/operations/*`（操作モード有効時） | 不要。操作提出は writer 署名を検証 | クライアントからのデータ読み書き |
| Internal API | `/api/internal/*` | Bearer Token (設定時) | ノード間通信 (sync, membership) |
| Control Plane (書き込み) | `PUT /api/control-plane/*`, `DELETE /api/control-plane/*` | Bearer Token (設定時) | Authority 定義・配置ポリシーの変更（Raft リーダーのみ受理 — 下記参照） |

Control Plane の読み取りエンドポイント (`GET`) は認証不要で誰でもアクセス可能。

### 操作モードと CRDT API

`/api/eventual/*` と `/api/certified/*` は従来の CRDT ストア、`/api/operations/*` は署名付きの不変操作ストアを扱う。操作 API は `ASTEROIDB_WRITER_KEYS` と作成時ポリシーを設定したノードだけで利用できる。この操作モードでは旧 `/api/eventual/*`、`/api/certified/*`、`/api/status/*`、`/api/authority/equivocations*` と旧データ用 `/api/internal/frontiers`、`/api/internal/sync*`、`/api/internal/certified/delta`、`/api/internal/keys` は `409 Conflict` を返す。旧ストアの WAL・snapshot は読み込まず、旧データ処理 worker と checkpoint も動かさない。操作 inbox と証拠ストアはそれぞれの永続化処理を使う。制御プレーン、membership、epoch 処理は稼働する。

| CRDT API の機能 | 操作 API の経路 | 意味の違い |
| --- | --- | --- |
| `POST /api/eventual/write` | `POST /api/operations/submit` | CRDT の型付き更新を、namespace・writer・counter で識別される署名付き Create/Add/Delete に変換する。`Add` は算術加算ではなく値と HLC を持つ更新。counter・set・map に同等の自動変換はない。 |
| `GET /api/eventual/{key}` | `GET /api/operations/view/{namespace_id}/{key}?kind=local` | 操作 API は手元の全操作から `Unknown`・`Deleted`・`Value` を返す。従来のセッショントークンや `wait_ms` と同じ読取保証は持たない。 |
| `POST /api/certified/write` | `POST /api/operations/submit-certified` | 従来は CRDT 値と frontier の証明を扱う。新経路は署名済みの一操作を保存し、その操作 ID の Merkle 証明書を待つ。`202` は保存済み・確定待ちであり、取消しではない。 |
| `GET /api/certified/{key}` | `GET /api/operations/view/{namespace_id}/{key}?kind=certified` | 新経路は手元の本体と検証済み操作証明書から値と完全な `basis` を返す。従来の frontier 証明やクラスタ全体の最新値を表さない。 |

操作モードを使う場合は、対象 namespace と key prefix、作成時の確定対象モード、writer 鍵を設定する。操作 ID の `status` と `kind=local|certified` の読みで結果を確認する。旧 CRDT データを操作台帳へ暗黙に取り込むことはない。

---

## 認証

### Bearer Token 認証

環境変数 `ASTEROIDB_INTERNAL_TOKEN` でトークンを設定すると、Internal API と Control Plane 書き込みエンドポイントに Bearer Token 認証が適用される。

```
Authorization: Bearer <token>
```

- トークンが設定されていない場合（または空文字列の場合）、全エンドポイントが認証なしでアクセス可能
- トークンの比較は constant-time 比較 (`subtle::ConstantTimeEq`) で行われ、タイミング攻撃を防止する
- 認証失敗時は `401 Unauthorized` が返る（ボディなし）

**認証が必要なエンドポイント:**

- `/api/internal/*` 全て
- `POST /api/internal/operations/receive`（操作受信を有効化したノードのみ）
- `POST /api/internal/operations/transfer`（操作受信を有効化したノードのみ）
- `POST /api/internal/operations/policy-witness`（policy-only 配送を有効化したノードのみ。Bearer と隣接リンク署名を要求）
- `POST /api/internal/operations/completion`（宛先別 routed custody を有効化したノードのみ）
- `POST /api/internal/operations/retirement`（宛先別 routed custody を有効化したノードのみ）
- `PUT /api/control-plane/authorities`
- `PUT /api/control-plane/policies`
- `DELETE /api/control-plane/policies/{prefix}`

### 操作本体の受信（試験的）

`POST /api/operations/policy/submit` は、完全な署名済み Create packet の作成ポリシーだけを固定宛先へ配送する公開 API。`ASTEROIDB_OPERATION_POLICY_DESTINATIONS` と固定経路が必要。固定経路は操作本体の routed 設定か、独立した `ASTEROIDB_OPERATION_POLICY_ROUTE_EDGES` / `ASTEROIDB_OPERATION_POLICY_ROUTES` で指定する。writer 署名、namespace、作成時設定、担当キーを検査し、Create を操作台帳の外に保存して全初回配送義務を fsync した後に `OperationReceipt` を返す。内部 `POST /api/internal/operations/policy-witness` はリンク署名付きの `PolicyWitnessTransfer { version:1, sender, receiver, destination, packet }` を受け、固定 route を検査する。各 hop は witness を永続化してから ACK する。次 hop の outbox 登録だけが失敗した場合は保存済み witness から再照合で復元する。Certified 読みでは本体未着でも Disabled と分かれば `NotTargeted`、Enabled なら `Unknown` を返す。通常読みと操作 ID status は本体未着のまま `Unknown`。
policy-only 配送が有効なノードでは、通常の操作経路で受けた完全な署名済み Create も ACK 前に policy witness を保存し、設定済み宛先への再送対象にする。`policy/submit` は通常の操作本体を先に送らず作成ポリシーを届ける経路として使える。配送は設定済みの経路と宛先に限る。

操作受信の起動には作成時ポリシー manifest が必要。namespace・キー prefix の最長一致で既定モードを決め、異なるモードはその範囲で明示許可された作成者だけが選べる。manifest は inbox に固定され、途中変更や既存操作への暗黙適用を拒否する。`ASTEROIDB_OPERATION_LEGACY_CREATION_POLICY` は受け付けない。

`POST /api/operations/submit` はこのノードの担当キーへ署名済み `OperationPacket` をローカル提出する。writer と作成操作の署名、namespace 権限、キーの担当を検証し、パケット本体・操作 ledger を永続保存した後に `OperationReceipt` を返す。経路がある固定宛先への配送義務は別の永続ステップで登録し、登録に失敗しても保存済み操作から再試行する。内部 Bearer Token は使わず、署名と writer 登録を認証根拠とする。応答はローカル保存を示し、Certified 到達や全保存先への配送完了を示さない。担当外キーと無効な署名は `403`、同一操作 ID の異なる内容、およびこのノードで削除を保持しているキーへの新しいローカル提出は `409`。HTTP 入力か必要な転送・完了通知の JSON が受信上限を超える場合は、保存前に `413` で拒否する。削除前に保存した操作の同一 ID・同一内容の再試行は受け付ける。ネットワークから届く遅延操作の取り込みにはこのローカル提出制限を適用しない。
routed custody でも必須宛先やローカル経路が未設定なら、ローカル保存後に受領票を返し、送れる宛先だけ配送する。Enabled 操作は必要な証拠が届くまで `pending` のままになる。経路設定の修復は送信履歴がない場合だけ安全に pin を更新できる。送信済みの宛先がある場合や neighbor・Authority 設定の変更を要する場合は、現在の設定のまま残る root と配送義務を保持する。
静的証明書モードでは `POST /api/operations/submit-certified?wait_ms=30000` も使える。同じ署名済みパケットをローカル保存してから、その操作 ID と本体に一致する証明書を最大 `wait_ms` ミリ秒待つ（既定 30000、最大 60000）。到達した場合は `200` と `CertifiedSubmitResponse { receipt, status: "certified", certificate }`、時間切れなら `202` と現在の状態（`"pending"` または `"policy_unknown"`）を返す。`policy_unknown` は compact な作成参照に一致する Create 本体・receipt・完全な署名済み policy witness が未到着で、作成時ポリシーをまだ確定できない状態を示す。ledger 本体が receipt に圧縮済みの操作を再提出した場合は、`certificate` の代わりに値を含まない `compact_certificate` と `operation_receipt` を返す。`202` 後も操作・配送・証明書収集は続き、取消しは行わない。確定対象外、設定 scope 外、待機上限超過は保存前に `422` で拒否する。結果は `GET /api/operations/status/{namespace_id}/{writer}/{counter}` で後から確認できる。

`ASTEROIDB_WRITER_KEYS` を設定したノードは `POST /api/internal/operations/receive` と `POST /api/internal/operations/transfer` を公開する。前者は署名済み `OperationPacket` を直接受信して `OperationReceipt` を返す。隣接 outbox の再送には後者を使う。従来の隣接配送では `OperationTransfer` は `version:1`、送信元 `sender`、送信元ごとの `transfer_id`、受信先 `receiver`、完全な `packet` を含む。宛先別 routed custody を設定した場合は `version:2` とし、最終宛先 `destination`、親転送 `parent`、転送深さ `depth` も含む。最終宛先と経路は namespace ID と key prefix で選択できる。受信ノードは署名付きパケットと転送記録をそれぞれ fsync してから `OperationTransferReceipt` を返す。受領証には同じ version・送信元・転送 ID・受信先と、署名を含むパケット全体の SHA-256 `commitment` を載せる。送信側はすべての項目が outbox 項目と一致する場合にのみ再送を止める。ACK は受信ノードでの永続保存だけを示し、Authority による確定や他ノードへの複製は示さない。

routed custody では `POST /api/internal/operations/completion` で最終取り込みの完了通知を上流へ伝える。最終宛先は転送の取り込みを永続記録してから通知し、中継ノードは子転送の保存 ACK と完了通知が揃った後で親転送の完了を通知する。送信側は通知と自身の転送記録を照合して永続保存し、通知の ACK を返す。完了通知は転送の保存 ACK を代替せず、操作の Certified 成立も示さない。通知は保存済み記録から再起動後に再送される。設定形式と運用上の制限は [運用ガイド](ops-guide.md) を参照。

送信側は保存済みの連続終了境界を `POST /api/internal/operations/retirement` で元の隣接受信先へ通知する。本文は `version:1`、`sender`、`receiver`、排他的 `closed_before`。受信側は単調な境界を fsync してから応答し、その境界より小さい ID の受信転送記録・最終取り込み記録・完了通知応答を回収する。転送 ID と要求の対応は catalog に保持する。証明済みで冗長になった ledger 値本体と全受信転送が終了した操作では、catalog を ID・経路・操作 ID・commitment の記述へ縮め、署名済み packet 本体も回収する。回収済み ID の遅延転送には新しい保存 ACK を返さない。通知は共有内部 token と静的隣接設定の信頼に依存し、HTTP 接続先のノード ID を暗号学的に証明しない。

最終宛先が静的設定上の Authority の場合は、担当キーかどうかに関係なく署名対象の転送を `operation_inbox/signing-inbox` に永続記録してから ACK を返す。担当キーでもあればアプリ取り込み記録も永続化する。Authority worker は routed モードでローカル提出とこの署名処理待ち記録を処理し、通過中のコピーだけでは署名しない。署名用の保持は、そのキーを公開読み取りに適用する担当設定とは独立している。

`GET /api/operations/read/{namespace_id}/{key}` は、このノードの担当 prefix に属するキーについて受信済みの全操作を投影した公開の通常読みを返す。同じ結果を内部 Bearer Token 必須の `GET /api/internal/operations/read/{namespace_id}/{key}` でも取得できる。担当外のキーは配送用コピーを保持していても `Unknown` を返す。応答は `Unknown`、`Deleted`、または `{"Value":{"value_bytes":[...],"timestamp":{...}}}`。`Unknown` はこのノードのアプリ読み取りに情報がないことを示し、クラスタ全体での不存在は示さない。

`GET /api/operations/view/{namespace_id}/{key}?kind=local|certified` は操作 API の統合読み取り口。`kind` は必須で、`local` は全受信済み操作を投影した `{"kind":"local","view":<OperationRead>}` を返す。`certified` は検証済み証明書だけを使い、`{"kind":"certified","view":{"result":<CertifiedReadResult>,"basis":[<OperationMerkleCertificate>],"local_body_seen":<bool>}}` を返す。証明書モードが設定されていないノードでは、確定対象キーの本体と作成根拠があれば `NoCertifiedInformation` と空の `basis` を返す。compact 作成証明を参照する更新・削除が Create 本体より先に届いた場合は `PolicyUnknown` を返す。`local` は担当外キーに `Unknown` を返す。`certified` は担当外でも認証済み Disabled の作成根拠があれば `NotTargeted`、それ以外は `Unknown` を返す。

証明書 scope を設定しない操作ノードでも、公開の `GET /api/operations/status/{namespace_id}/{writer}/{counter}` は保存済みの操作 ID から `unknown`・`policy_unknown`・`not_target`・`pending` を返す。`GET /api/operations/certified-read/{namespace_id}/{key}` は確定対象外キーを `NotTargeted`、確定対象キーに本体があれば `NoCertifiedInformation` として返す。compact 作成証明の参照先 Create をまだ確認できない場合は `PolicyUnknown` と `policy_unknown` になり、Create 本体またはその receipt が届くと確定したモードで再判定する。これらの応答に証明書は含まれない。担当外キーの Certified 読みは認証済み Disabled だけ `NotTargeted`、他は `Unknown` とする。担当外 ID の status は `unknown` とする。

署名、namespace、作成時設定を検証できない要求は `403`、同一操作 ID に異なる内容を送る要求は `409`。この経路は既存の `/api/certified/write` と独立している。固定隣接先の key prefix 設定に従い、受信内容を永続化してから ACK する。後続の送信登録が失敗した場合は保存済み入力から再照合で補い、未 ACK 項目は再起動後も再送する。

静的な操作証明書を設定したノードは `POST /api/internal/operations/supports/receive`、`POST /api/internal/operations/certificates/receive`、`GET /api/internal/operations/certified-read/{namespace_id}/{key}` と `GET /api/internal/operations/status/{namespace_id}/{writer}/{counter}` を公開する。support 受信は `SupportPacket` の操作本体・Authority 署名・包含証明を検証して fsync し、quorum が揃えば証明書を保存して `SupportReceipt` を返す。証明書受信は完成済みの `OperationMerkleCertificate` を検証・永続化してから `CertificateReceipt` を返す。証明書だけが先に届いても保存できる。完成した証明書は宛先別の永続 outbox に登録し、隣接先への配送を再試行する。複数 scope を設定した場合は namespace と最長一致する prefix で検証器を選び、親 scope は子 scope のキーに対する証拠を受け付けない。異なる namespace の同じキー名と操作 dot は独立して扱う。読み取りは担当キーについて、手元の操作本体と一致する証明済み操作だけで値を計算し、`result`、各操作の証明書 `basis`、`local_body_seen` を返す。同じ証明済み読み取りを公開の `GET /api/operations/certified-read/{namespace_id}/{key}` でも取得できる。`result` は `Unknown`（本体なし）、`PolicyUnknown`（本体はあるが認証済み Create 未着）、`NotTargeted`（確定対象外）、`NoCertifiedInformation`（本体はあるが証明済み操作なし）、`Deleted`、または `Value`。操作 ID の状態は `unknown`、`policy_unknown`、`not_target`、`pending`、`certified` で返す。certified で ledger 本体がある場合は `certificate`、本体が receipt に圧縮済みなら `compact_certificate` と `operation_receipt` を付ける。後者の証拠は操作 ID・内容 commitment・署名付き root・包含証明を持ち、値本体を含まない。第三者が元の操作内容まで確認するには、別途保持した元の署名済みパケットと receipt の照合が必要。担当判定は namespace ID とキーの組で行い、担当外キーは本体を配送用に保持していても、認証済み Disabled の作成根拠があるときだけ Certified 読みで `NotTargeted` を返す。他の担当外読みは `Unknown`。読み取りは固定鍵・scope の信頼アンカーを検査し、内部ルートには Bearer Token を要求する。既存の公開 `/api/certified/*` とは独立している。Authority ノードはローカルで発行した support を永続 outbox から固定隣接先へ再送する。

操作 ID の `not_target` は認証済み Create で確定した mode が Disabled のときだけ返す。compact 作成証明の参照先 Create が未着なら、宣言の mode が署名済みでも `policy_unknown` を返す。完全な Create を埋め込む従来の署名証明では追加の Create 配送を待たない。mode が Enabled で、このノードの静的証明書 scope 外にある操作は、作成根拠を確認できれば `pending` と返す。scope は作成時に選ばれた確定対象設定を変更しない。保存済み scope の配置・信頼条件が live 設定と食い違う場合、公開 read/status と内部 certificate/support 受信、`submit-certified` は `503` を返す。証明済み操作を `pending` に降格させない。

証明書が Create より先に届いた場合、`PolicyUnknown` / `policy_unknown` のままでも検証済み `basis` / `certificate` を含み得る。証拠の到着だけでは、このノードの作成ポリシーを学習済みとはみなさない。

**認証が不要なエンドポイント:**

- `/api/eventual/*`, `/api/certified/*`, `/api/status/*`, `POST /api/operations/submit` と `POST /api/operations/submit-certified` と `POST /api/operations/policy/submit`（writer 署名を検証）、`GET /api/operations/read/{namespace_id}/{key}`、`GET /api/operations/view/{namespace_id}/{key}`、`GET /api/operations/certified-read/{namespace_id}/{key}`、`GET /api/operations/status/{namespace_id}/{writer}/{counter}`
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
| `application/json` | JSON | デバッグ用 |
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

**クエリパラメータ:**

| パラメータ | 型 | 必須 | 説明 |
|-----------|-----|------|------|
| `session_token` | string | いいえ | クライアントが保持している直前のセッショントークン。指定すると、この書き込みの HLC 位置を**提示トークンにマージ（折り畳み）した累積トークン**が応答で返る。複数コーディネータに跨って書き込むセッションが read-your-writes を維持するにはこの持ち回りが必須（トークンは不透明であり、クライアント側で複数トークンをマージすることはできない）。空文字列は「トークン無し」と同義。不正な形式・制限超過・サーバ時計 + 60 秒を超えるトークンは `400` で拒否され、**書き込み自体も適用されない** |

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

`session_token` は常に返却される。`session_token` クエリパラメータ無しの
リクエストでは**この書き込みの HLC 位置のみ**を表し、提示ありのリクエスト
では**提示トークンにこの書き込みの位置をマージした累積トークン**を表す。
次の `GET /api/eventual/{key}?session_token=...` に添付すると
read-your-writes が保証される。同一セッションで複数回書き込む場合
（特にコーディネータを跨ぐ場合）は、**毎回の write に直前の応答トークンを
渡して累積させる**こと — 最後の write の応答トークンだけを read に添付する
方式では、先行 write の origin 位置がトークンに含まれず read-your-writes が
保証されない。詳細は [セッショントークン](#セッショントークン) を参照。

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
- 可視性順序の保証であり、耐久性は WAL の fsync モードに従属する。
  `ASTEROIDB_WAL_SYNC=always`（既定）では ack 済み書き込み（= トークンを
  返した書き込み）は再起動・電源断を生き延び、トークンは再起動後も充足
  される。`interval` / `off` では OS クラッシュで直近の ack 済み書き込みが
  ローカル喪失し得るが、その場合も自レプリカは 412 を返すだけで嘘は
  返さない（fail-closed。詳細は ops-guide の耐久性保証表を参照）
- トークンは eventual ストア専用。certified read に流用しても充足しない
  （412 になるだけで害はない）
- 応答トークンはエントリ数上限 64・全長 8192 バイトに収めるため間引かれ得る。
  間引きは HLC の新しい順にソートして**古いエントリから機械的に切り捨てる**
  だけであり、リクエスト由来のエントリや直前に読んだキーの変更位置を優先
  保持する仕組みは**ない**（当該エントリが最古ならそれが落ちる）。落ちた
  origin は以後の判定から消えるため、間引きは単なる偽陰性（保証の弱化）では
  **済まない**: 間引かれた origin の書き込みを持たないレプリカでの次の read
  が誤って成功し、**観測済みの値より古い値が返り得る**（monotonic reads の
  巻き戻り）。ノード ID が長い場合はバイト上限により origin 数が 64 以下でも
  間引きが起き得る。完全な monotonic reads が必要な場合は、書き込み origin
  数を 64 以下に保ち、かつトークンが 8192 バイトに収まるようノード ID を
  短く保つこと
- 応答トークンは applied claim 済みの origin だけでなく**可視状態全体**
  （unclaimed マージで見えるようになった寄与を含む）を被覆する。観測した値を
  被覆しないトークンを発行すると、別レプリカでの巻き戻り（monotonic reads の
  嘘）につながるため。過剰被覆は偽陰性（412）方向にしか作用しない
- トークンは無署名である。偽造・改竄されたトークンは自分の read が 412/400 に
  なるだけで他クライアントやサーバに害を与えない（BFT 拡張時に Ed25519 署名を
  検討予定）

#### 利用例

```bash
# 1. 書き込み → トークンを得る
TOKEN=$(curl -s -X POST http://node-a:3000/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"greeting","value":"hello"}' | jq -r .session_token)

# 2. 続けて書き込む場合（別コーディネータでも可）: 直前のトークンを
#    write に渡して累積させる（渡さないと先行 write の位置が失われる）
TOKEN=$(curl -s -X POST "http://node-c:3000/api/eventual/write?session_token=$TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"register_set","key":"farewell","value":"bye"}' | jq -r .session_token)

# 3. 別ノードでトークン付き read（追いついていなければ 412）
curl "http://node-b:3000/api/eventual/greeting?session_token=$TOKEN"

# 4. 412 の場合: wait_ms 付きでリトライ（複製が届き次第 200）
curl "http://node-b:3000/api/eventual/greeting?session_token=$TOKEN&wait_ms=3000"

# 5. 応答の session_token を次の read に持ち回る（monotonic reads）
```

---

### Certified API

#### POST /api/certified/write

Authority ノード群の過半数合意によって認証が必要な値を書き込む。

- **認証**: 不要
- **Content-Type**: `application/json`
- **レスポンス**: `200 OK` (pending の場合)、`504 Gateway Timeout` (on_timeout=error の場合)、または容量超過時 `503 Service Unavailable`

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

- `"pending"` — その時点で証明が揃わなければ `Pending` を返す（後からポーリング可能）
- `"error"` — その時点で証明が揃わなければ、ローカル WAL の永続化を待ってから `504 Gateway Timeout` + `CERTIFICATION_TIMEOUT` を返す。書き込み自体は保存され、後から認証されうる

受理済み操作は待機時間の経過で取り消されない。同じキーへの新しい書き込みが永続化されると、古い同一キーの追跡項目は置き換わる。古い項目は現在のストア値を証明できないため、容量を占め続けない。追跡容量が満杯で置換可能な同一キー項目もなければ、新しい書き込みは保存前に `503` + `CERTIFICATION_CAPACITY` で拒否される。`Retry-After: 1` に従って再試行する。

**成功レスポンス:**

```json
{
  "status": "Pending"
}
```

`status` は `"Pending"` または `"Certified"` のいずれか。
`on_timeout=error` の `504` はこの成功レスポンスではなくエラーレスポンスになる。
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
| `status` | string | 認証ステータス: `"Pending"`, `"Certified"`, `"Rejected"` のいずれか。`Rejected` は後から certification 評価で拒否された場合にのみ出現する。待機時間の経過は `Pending` を変えない |
| `frontier` | FrontierJson \| null | 読み取り時に観測した過半数 frontier。未署名報告を含み得るため、確定や署名の証拠ではなく、`proof.frontier`（証明書の checkpoint）とも一致するとは限らない |
| `proof` | ProofBundleJson \| null | `Certified` の根拠となる検証可能な証明バンドル。`Pending` では null |

**複製と `status` の関係（重要）**

certified の値は `POST /api/internal/certified/delta` レーンで全ノードに複製される。
これによる可視な変化:

- **非ライターノードでも `value` が返るようになった。** 以前は自分が書いていない
  certified キーは `value: null` だった。今は値が入り、`status` は（そのノードが
  独自に attestation を集めるまで）`"Pending"` になる。
- **`status` はノードローカルな観測であり、値の耐久性とは独立**。複製が運ぶのは値と
  その origin バージョンだけで、認証の「判定」は運ばない。証拠の到着タイミングにより、同じキーが A では `Certified`、B では `Pending` に見えることが
  正常にありうる（fail-closed 方向なので安全側）。
- 未知キーが永遠に `Pending` に見える件（`get_certification_status` の導出化）は
  **この変更では解消していない**。

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
| `certificate` | CertificateJson | 検証成功には必須 | 暗号署名付き証明書。省略したリクエストは無効と判定される |
| `format_version` | u32 | いいえ | 証明書フォーマットバージョン（指定時にバージョン互換チェック実施） |
| `signature_algorithm` | string | いいえ | 署名アルゴリズム: `"Ed25519"` (デフォルト) または `"Bls12_381"` |
| `keyset_version` | u64 | いいえ | BLS 検証で使用する keyset バージョン（省略時は `certificate.keyset_version`、それも無ければ 1） |
| `bls_aggregate_signature` | string | いいえ | hex エンコード BLS 集約署名。`bls_signer_ids` / `bls_public_keys` と共に指定し `signature_algorithm` が `"Bls12_381"` の場合、keyset registry に対する BLS 集約検証を実施 |
| `bls_signer_ids` | string[] | いいえ | BLS 署名者ノード ID（`bls_public_keys` と同順） |
| `bls_public_keys` | string[] | いいえ | hex エンコード BLS 公開鍵（registry 鍵との一致を要求） |

> `GET /api/certified/{key}` の `proof` オブジェクトはこのリクエストボディとして
> そのまま送信できる（round-trip 可能）。`Certified` の `proof` には署名付き
> `certificate` が必ず含まれる。署名が揃う前は `Pending` となり、`proof` はない。
> 署名パイプラインが有効な場合、`proof` には
> `signature_algorithm` / `keyset_version` / BLS フィールドが自動的に含まれる。
> 証明書の `frontier` は Authority が署名したチェックポイント HLC
> （1 秒単位に床丸めされた値、`logical=0`, `node_id=""`）である点に注意。
> 現行証明書の署名対象はキー範囲・チェックポイント・ポリシー版であり、
> 個々の操作 ID や内容を証明するものではない。

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

`status` は `"Pending"`, `"Certified"`, `"Rejected"` のいずれか。待機時間の経過は `Pending` を変えない。

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

#### GET /api/control-plane/raft/status

Control plane Raft コンセンサスの状態を返す（`/api/metrics` と同じく認証
不要の読み取り専用観測エンドポイント）。書き込み前のリーダー特定や、
`NOT_LEADER` 受信後のリトライ先の確認に使う。

- **レスポンス**: `200 OK`

```json
{
  "node_id": "node-1",
  "role": "leader",
  "term": 3,
  "leader_id": "node-1",
  "leader_addr": null,
  "commit_index": 12,
  "last_applied": 12,
  "last_log_index": 12,
  "voters": ["node-1", "node-2", "node-3"]
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `node_id` | string | このノードの ID |
| `role` | string | `"leader"` / `"follower"` / `"candidate"` / `"detached"`（Raft 未構成） |
| `term` | u64 | 現在の Raft term |
| `leader_id` | string \| null | 既知のリーダーのヒント |
| `leader_addr` | string \| null | リーダーの解決済みアドレス（既知の場合） |
| `commit_index` | u64 | commit 済みログの最大 index |
| `last_applied` | u64 | 状態機械へ適用済みの最大 index |
| `last_log_index` | u64 | ローカルログの末尾 index |
| `voters` | string[] | 静的投票者集合（`ASTEROIDB_CONTROL_PLANE_NODES`） |

**curl 例:**

```bash
curl http://localhost:3000/api/control-plane/raft/status
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
  "key_rotation_total": 0,
  "key_rotation_last_version": 0,
  "key_rotation_last_time_ms": 0,
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
| `key_rotation_total` | u64 | 累計鍵ローテーション回数。**自動ローテーション未配線のため現状は常に 0**（`docs/runbook/key-rotation.md` 参照） |
| `key_rotation_last_version` | u64 | 最後にローテーションで適用された keyset バージョン。**自動ローテーション未配線のため現状は常に 0**（メトリクスの初期値。keyset レジストリの初期バージョン 1 とは別物であり、0 は鍵レジストリの初期化不良を意味しない） |
| `key_rotation_last_time_ms` | u64 | 最新ローテーション時刻 (ミリ秒)。**現状は常に 0** |
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

**リージョン名の由来と `unknown`:** リージョン名はノードの `region:` タグから
導出される。ノードのタグはノード間で伝播されない（`PeerInfo` / `AnnounceRequest`
にタグのフィールドが無い）ため、**自ノード以外は常に `unknown` リージョンに
分類される**。`total_nodes` と各ノード ID は正しく、リージョン分類だけが
自ノード分に限られる。タグをワイヤに載せる作業は follow-up
（`docs/followup-plan.md`）。

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
  "durable": true,
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
| `durable` | bool | 応答対象の eventual ストア状態が WAL 同期済み。`false` の応答は push baseline と GC peer ACK の証拠にならない |

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

### Digest Sync

#### POST /api/internal/sync/digest

digest 段階 diff 同期（フルシンクの前段）。要求側の 2 層キー範囲 digest
（ルート + 非空バケット、SHA-256）を受け取り、応答側の単一スナップショットと
比較して 1 往復で応答する: ルート一致なら `root_matched = true`（転送ゼロ）、
不一致なら不一致バケット所属キーの実データを返す。詳細は
`docs/architecture.md` の anti-entropy 節を参照。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json` または `application/octet-stream`
- **Accept**: `application/json` または `application/octet-stream`

**リクエストボディ (JSON):**

```json
{
  "sender": "node-2",
  "scheme_version": 1,
  "root": [18, 52, "..."],
  "buckets": [{ "index": 7, "digest": [171, 205, "..."] }],
  "include_entries": true
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `sender` | string | 送信元ノード ID |
| `scheme_version` | u32 | digest スキームバージョン（現行 1）。不一致は `scheme_ok=false` |
| `root` | bytes (32) | 要求側のルート digest |
| `buckets` | array | 要求側の非空バケット digest のスパース列（不在 = 空バケット） |
| `include_entries` | bool | `true`（pull）: 不一致バケットの実データを返す。`false`（push probe）: 不一致バケット番号のみ |

**レスポンスボディ:**

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `scheme_ok` | bool | `false` = バージョン/形式非対応（要求側は従来フルシンクへ） |
| `durable` | bool | この digest が参照した状態が WAL 同期済み。root 一致や peer-only 差分を GC peer ACK に使うには `true` が必要 |
| `root_matched` | bool | `true` = 状態完全一致、転送ゼロで完了 |
| `mismatched_buckets` | array<u16> | digest が不一致だったバケット番号（双方向差） |
| `entries` | map<string, CrdtValue> | 不一致バケット所属キーの全データ（`include_entries=true` 時のみ） |
| `timestamps` | map<string, HlcTimestamp> | `entries` の per-key HLC |
| `frontier` | HlcTimestamp \| null | 応答側スナップショット時点の frontier |
| `applied_origins` | map<string, HlcTimestamp> | 応答側の適用済みフロンティア（digest 応答はスナップショットに対し完全なので受信側は適用後に無条件採用できる） |
| `visible_origins` | map<string, HlcTimestamp> | 応答側の可視フロンティア（無条件 max マージ） |
| `merge_failed_keys` | array<string> | 応答側の poison 済みキー（採用時に union） |
| `total_keys` | u64 | 応答側スナップショットの総キー数（転送削減量の推定に使用） |

**curl 例（空ストアの digest で全量取得）:**

```bash
curl -X POST http://localhost:3000/api/internal/sync/digest \
  -H "Content-Type: application/json" \
  -d '{"sender":"node-2","scheme_version":1,"root":['"$(python3 -c 'import hashlib;print(",".join(str(b) for b in hashlib.sha256(bytes(8192)).digest()))')"'],"buckets":[],"include_entries":true}'
```

---

### Certified Replication

#### POST /api/internal/certified/delta

**certified** ストアのエントリを、要求側のベースライン以降の分だけ返す。
certified プレーン専用の anti-entropy レーン（pull 専用）。

このレーンが入る前、certified write はそれを受理した 1 ノードのディスクにしか
存在しなかった（`/api/internal/sync`、`/sync/delta`、`/sync/digest`、`/keys` は
すべて **eventual** ストアを配信する）。そのノードを失うと値は恒久的に失われ、
クライアントの手元には「どこにも存在しない値に対する検証可能な証明」だけが残った。
同じキーを eventual で書いていれば失われなかったので、**強整合側が耐久性で劣る**
倒立が起きていた。

- **認証**: Bearer Token (設定時)
- **Accept**: `application/json` または `application/octet-stream`

**リクエストボディ:**

```json
{
  "sender": "node-2",
  "frontier": { "physical": 1700000000000, "logical": 3, "node_id": "node-1" }
}
```

**レスポンスボディ:**

```json
{
  "entries": [
    {
      "key": "user/alice",
      "value": { "type": "counter", "value": 42 },
      "hlc": { "physical": 1700000000001, "logical": 0, "node_id": "node-1" },
      "policy_version": 1
    }
  ],
  "sender_frontier": { "physical": 1700000000001, "logical": 0, "node_id": "node-1" },
  "truncated": false
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `entries[].value` | CrdtValue | そのキーの**全状態**。CRDT の部分デルタではない。certified プレーンには取りこぼしを後から要求する手段が無く、部分ペイロードを土台無しにマージすると「フィールドが静かに欠けた certified マップ」が有効な証明付きで配られてしまうため |
| `entries[].hlc` | FrontierJson | **書き手の** HLC。受信側はこれをそのまま記録する（ローカル時計で再スタンプすると収束後もレーンが静止しない） |
| `entries[].policy_version` | u64 \| null | その書き込みが発行された **origin** ポリシーバージョン。`null` は origin 永続化以前に書かれたキー |
| `sender_frontier` | FrontierJson \| null | 全エントリ適用後に要求側が採用してよいベースライン。**`truncated` のときは「最後に含めたエントリの HLC」**であり、応答側ストア全体の frontier ではない |
| `truncated` | bool | バッチ上限（512 件）で打ち切ったか。要求側はこれが `true` の間、同一サイクル内で最大 8 ラウンドまで続けて引く（ドレイン） |

**`policy_version` が load-bearing である理由**

受信側の `resolve_scope` が返すのは**受信側の現行バージョン**であり、ingest は
書き手の HLC をそのまま保持する。したがって origin をワイヤに載せないと、既に
v2 に上がったノードは v1 で書かれた値を**自分の v2 frontier** で評価して
Certified にしてしまう。`is_version_fenced` は attestation の admission しか
止めないので、これは防げない。これは fence 永続化の修正
(fix/fence-persistence-across-restart) が塞いだ穴そのものであり、複製によって
**全非ライターノードに、クラッシュも再起動もなしに**広がることになる。
`policy_version` はこれを閉じる。

**バッチと帯域**

1 レスポンス最大 512 件、1 サイクル 1 peer あたり最大 8 ラウンド。certified
プレーンには digest 相当の段階的 diff が無いため、新規参加ノードやディスクを
失って復帰したノードは certified キー集合の全量をこのエンドポイント経由で引く
（`sync_interval` のティックに分散される）。

**ベースラインの進め方（要求側の契約）**

このエンドポイントは baseline より**厳密に大きい**エントリしか返さない。
したがって要求側があるエントリを飛ばしてベースラインを進めることは、そのノードに
とって恒久的な取りこぼしになる。要求側は拒否理由で扱いを分けなければならない:

- 一過性（origin が自分より先 / authority 定義がまだ無い = 制御プレーンの適用
  遅れ）: ベースラインを**進めず**、次サイクルで再試行する。
- 恒久（CRDT 型衝突）: 次へ進む。

加えて要求側は定期的に `frontier` をゼロにしてフルリスキャンする。ingest は
書き手の HLC を保持するため、peer は自分の frontier より**下**のキーを正当に
持ちうる——スカラのベースラインではこの取りこぼしを表現できない。

**ingest 側のガード（重要）**

このエンドポイントが供給する ingest は**クライアント面のポリシーゲートを経由しない**。
internal token を持つ者は certified ストアへ値を注入しうる。緩和は次の 3 点:

1. ローカル write と同じ `resolve_scope` を通す（authority 定義の無いキーは拒否）
2. `policy_version` が受信側の現行バージョンより **先** のものは拒否
3. ingest は `Pending` の追跡しか作れず、値が変わった場合は旧 `certified_cache` を
   無効化する。`attestations` / frontier に証拠を追加しないため、**単独で
   `Certified` や proof を捏造できない**

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
| `namespace` | object | system namespace の JSON スナップショット。**参考情報**: 参加ノード側の組み込みクライアントはこのフィールドを自動適用しない（制御プレーン状態は voter なら Raft 複製、非 voter なら observer namespace pull（ops-guide §14.8）で継続的に追随する。join 時の一回きりスナップショットは陳腐化するため採用経路にしていない） |

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

以下のエンドポイントは全て、**control-plane Raft ログでの合意 (FR-009)** と
Bearer Token 認証（設定時）が必要。リクエストを受理できるのは**現在の Raft
リーダーのみ**で、更新はリーダーのログに追記され、静的投票者集合
（`ASTEROIDB_CONTROL_PLANE_NODES`）の過半数で commit されてから応答が返る。

リーダー以外のノードに送ると `503 Service Unavailable` + `error_code:
NOT_LEADER` が返る（`Retry-After: 1` 付き）。応答ヘッダ
`x-asteroidb-leader-id` / `x-asteroidb-leader-addr` にリーダーのヒントが
入るので、クライアントはヒント先へ再送すること（サーバー側転送は行わない）。
リーダーが不明な場合は `GET /api/control-plane/raft/status` で確認できる。

```json
// HTTP 503 Service Unavailable（Retry-After: 1、
// x-asteroidb-leader-id / x-asteroidb-leader-addr ヘッダ付き）
{
  "error_code": "NOT_LEADER",
  "message": "this node is not the control-plane leader; retry against the leader (leader_id: node-2, leader_addr: 10.0.0.2:3000)"
}
```

> **`approvals` フィールドは deprecated**: pre-Raft の承認カウント合意で
> 使われていた自己申告の承認リスト。旧クライアントとのワイヤ互換のため
> 受理はされるが**完全に無視**される — 合意は Raft ログ複製が担う。
> 新規クライアントは送らないこと。

### PUT /api/control-plane/authorities

Authority 定義を設定する。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`
- **受理**: Raft リーダーのみ（それ以外は `503 NOT_LEADER`）

**リクエストボディ:**

```json
{
  "key_range_prefix": "user/",
  "authority_nodes": ["auth-1", "auth-2", "auth-3"]
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `key_range_prefix` | string | 対象キー範囲プレフィックス |
| `authority_nodes` | string[] | Authority ノード ID のリスト。**空配列・重複・空文字列は `400 BAD_REQUEST`**（下記参照） |
| `approvals` | string[] | **Deprecated**。受理されるが無視される（合意は Raft が担う） |

**空の `authority_nodes` は受理されない（400）。** Authority が 0 個の scope は
過半数しきい値が充足不能で何も certify できず、仮に証明を発行しても
`total_authorities: 0` は検証側でも必ず invalid になる。

**重複・空文字列の node ID も受理されない（400）。** 過半数の分母は
`authority_nodes.len()` だが、ack frontier は `(key_range, policy_version,
authority_id)` でキーされるため、同じ ID を N 個並べても frontier は 1 本しか
供給できない（`["n1","n1"]` は 1 本しか出せない集合に 2 本を要求する）。
空文字列 ID はどのノードも指さないので永久に報告されない。どちらも当該
プレフィックスの certified write を恒久的に Pending にする。

この検査は propose の前段で行われるため、拒否されたリクエストは Raft ログに
一切残らない。

**プレフィックスの certified 運用を止める方法は、配置ポリシーの削除
（`DELETE /api/control-plane/policies/{prefix}`）だけ。**
`certified: false` への変更では止まらない: certified 経路のどこも
このフラグを読んでいない（`resolve_scope` は authority 定義と配置ポリシーの
両方が在ることだけを要求する）。また authority 定義を削除する API は存在せず、
`recalculate_authorities` が自動削除するのは `auto_generated` な定義だけなので、
この API で作った手動定義は decertify しても残り、certify し続ける。
なおポリシーを削除すると当該プレフィックスの certified write は
`400 INVALID_ARGUMENT`（ポリシー無し）になる。

当該 scope の authority 定義が（何らかの理由で）空のまま残っている間、
`POST /api/certified/write` は `403 POLICY_DENIED` を返す。
`GET /api/certified/{key}` は 200 のまま `status: "Pending"` /
`frontier: null` を返す（読み取りは scope 解決の失敗を握り潰す）。

**レスポンスボディ:**

```json
{
  "key_range_prefix": "user/",
  "authority_nodes": ["auth-1", "auth-2", "auth-3"]
}
```

**curl 例:**

```bash
curl -X PUT http://<leader-addr>/api/control-plane/authorities \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "user/",
    "authority_nodes": ["auth-1", "auth-2", "auth-3"]
  }'
```

---

### PUT /api/control-plane/policies

配置ポリシーを設定する。バージョンは commit 済みエントリの**適用時**に
複製されたバージョンカウンタから採番される（commit 順が全ノードで同一の
バージョンを決める）。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`
- **受理**: Raft リーダーのみ（それ以外は `503 NOT_LEADER`）

**リクエストボディ:**

```json
{
  "key_range_prefix": "user/",
  "replica_count": 3,
  "required_tags": ["region:ap-northeast-1"],
  "forbidden_tags": ["decommissioned"],
  "allow_local_write_on_partition": true,
  "certified": true
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
| `approvals` | string[] | いいえ | `[]` | **Deprecated**。受理されるが無視される（合意は Raft が担う） |

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
curl -X PUT http://<leader-addr>/api/control-plane/policies \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{"key_range_prefix":"","replica_count":0}'
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
curl -X PUT http://<leader-addr>/api/control-plane/policies \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{
    "key_range_prefix": "",
    "replica_count": 3,
    "required_tags": [],
    "forbidden_tags": [],
    "allow_local_write_on_partition": false,
    "certified": false
  }'
```

---

### DELETE /api/control-plane/policies/{prefix}

指定キー範囲プレフィックスの配置ポリシーを削除する。

- **認証**: Bearer Token (設定時)
- **Content-Type**: `application/json`
- **パスパラメータ**: `prefix` - 削除対象のキー範囲プレフィックス
- **受理**: Raft リーダーのみ（それ以外は `503 NOT_LEADER`）

**リクエストボディ:**

```json
{}
```

（`approvals` フィールドは deprecated — 受理されるが無視される）

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
curl -X DELETE http://<leader-addr>/api/control-plane/policies/user%2F \
  -H "Authorization: Bearer my-token" \
  -H "Content-Type: application/json" \
  -d '{}'
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
| `DOT_COUNTER_EXHAUSTED` | 409 Conflict | OR-Set / OR-Map のローカル dot 採番が `u64` 上限に達し、書き込みを適用していない | 同じノード ID で再試行しない。新しい発行元 ID への移行を計画する |
| `PN_COUNTER_EXHAUSTED` | 409 Conflict | PN-Counter のローカル正または負の成分が `u64` 上限に達し、書き込みを適用していない | 同じ成分への増減を再試行せず、新しい発行元 ID への移行を計画する |
| `POLICY_DENIED` | 403 Forbidden | 配置ポリシーによる拒否 | 対象キー範囲の配置ポリシーを確認する |
| `CERTIFICATION_TIMEOUT` | 504 Gateway Timeout | 書き込みは保存されたが、その応答時点では認証されていない | 状態を再照会する。再書き込みは不要 |
| `CERTIFICATION_CAPACITY` | 503 Service Unavailable | 未認証操作の追跡容量が満杯で、書き込みは保存されていない | `Retry-After` に従い再試行する |
| `SESSION_NOT_SATISFIED` | 412 Precondition Failed | セッショントークンの示す書き込みまでローカルレプリカが追いついていない | `Retry-After` に従いリトライ、`wait_ms` を増やす、または別レプリカに問い合わせる |
| `NOT_LEADER` | 503 Service Unavailable | Control plane 書き込みを Raft リーダー以外のノードが受信した | 応答ヘッダ `x-asteroidb-leader-id` / `x-asteroidb-leader-addr` のヒント先へ再送する（`Retry-After: 1` 付き）。リーダー不明なら `GET /api/control-plane/raft/status` で確認 |
| `STORAGE_UNAVAILABLE` | 503 Service Unavailable | 書き込みの WAL 追記に失敗し耐久性を確立できない（例: ディスクフル） | ディスク容量・I/O 状態を確認してリトライする。読み取りは継続可能 |
| `INCOMPATIBLE_VERSION` | 500 Internal Server Error | データバージョンとコードバージョンの不整合 | AsteroidDB を最新版に更新するか、データマイグレーションを実行する |
| `MIGRATION_FAILED` | 500 Internal Server Error | データマイグレーション失敗 | ログを確認し、データの整合性を検証する |
| `INTERNAL` | 500 Internal Server Error | 内部エラー | サーバーログを確認する |

---

## CRDT 値型

API で扱う CRDT 値は tagged union 形式で表現される。
`type` フィールドで型を判別する。

### Counter (PN-Counter)

増減可能なカウンタ。正負の成分は合計してから差を計算し、結果だけを `i64` の範囲に収める。

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
| `asteroidb-cli operation-sign-create <namespace_id> <writer> <counter> <key> --value-file <file> --key-file <secret.hex> --creation-policy-file <policy.json> --mode enabled\|disabled --output <packet.json>` | ローカル | — | 作成ポリシーを検証し、その digest を含む作成証拠と操作に署名してパケットを書き出す |
| `asteroidb-cli operation-sign-add <create.json> <writer> <counter> --value-file <file> --key-file <secret.hex> --output <packet.json>` | ローカル | — | 作成宣言を引き継ぎ、値の追加操作に署名する |
| `asteroidb-cli operation-sign-delete <create.json> <writer> <counter> --key-file <secret.hex> --output <packet.json>` | ローカル | — | 作成宣言を引き継ぎ、削除操作に署名する |
| `asteroidb-cli operation-submit <packet.json>` | POST | `/api/operations/submit` | 署名済み `OperationPacket` のローカル保存と受領証取得 |
| `asteroidb-cli operation-submit-certified <packet.json> [--wait-ms <n>]` | POST | `/api/operations/submit-certified` | 同じ署名済みパケットを提出し、操作証明書を待つ。`202` は保存済み・確定待ち |
| `asteroidb-cli operation-read <namespace_id> <key>` | GET | `/api/operations/read/{namespace_id}/{key}` | 手元の全操作による通常読み |
| `asteroidb-cli operation-certified-read <namespace_id> <key>` | GET | `/api/operations/certified-read/{namespace_id}/{key}` | 証明済み操作と証拠による読み取り |
| `asteroidb-cli operation-view <namespace_id> <key> --kind local\|certified` | GET | `/api/operations/view/{namespace_id}/{key}?kind=...` | 通常読みまたは証明付き読みを明示して取得 |
| `asteroidb-cli operation-status <namespace_id> <writer> <counter>` | GET | `/api/operations/status/{namespace_id}/{writer}/{counter}` | 操作 ID ごとの確定状態 |
| `asteroidb-cli metrics` | GET | `/api/metrics` | 詳細ランタイムメトリクス |
| `asteroidb-cli slo` | GET | `/api/slo` | SLO バジェット状況 |

`operation-sign-*` は `OperationPacket` JSON をローカルで生成する。`--key-file` は 32 バイトの Ed25519 秘密鍵を 64 桁の hex で記したファイル、`--value-file` は値の生バイト列。Create の `--creation-policy-file` は提出先ノードに固定された `ASTEROIDB_OPERATION_CREATION_POLICY` と同じ JSON manifest を指定する。CLI は署名前に作成宣言を検証し、manifest の digest を署名済み作成証拠に含める。Create/Add は `--timestamp-ms` と `--logical` を省略でき、省略時の時刻は現在時刻、logical は 0 になる。Add/Delete の `<create.json>` には Create 時のパケットを指定し、同じ宣言と署名済み作成証拠を引き継ぐ。作成者と更新者は別の writer でもよいが、両方の公開鍵と namespace 許可をノードの `ASTEROIDB_WRITER_KEYS` に登録する必要がある。入力ファイルの作成者署名と公開鍵許可は提出先ノードで検証する。

例: `asteroidb-cli operation-sign-create example writer-1 1 orders/1 --value-file value.bin --key-file writer.secret.hex --creation-policy-file creation-policy.json --mode enabled --output create.json` の後に `asteroidb-cli operation-submit-certified create.json --wait-ms 5000`。更新時は `operation-sign-add create.json writer-1 2 --value-file next.bin --key-file writer.secret.hex --output add.json` で新しい操作 ID を使う。`operation-submit` 系はこの JSON を入力として受け取る。操作コマンドは操作 API を有効にしたノードが対象で、既存の `get` / `put` とストア・確定の意味を共有しない。

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
