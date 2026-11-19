# AsteroidDB 運用ガイド

本ドキュメントは AsteroidDB クラスタの運用・監視・トラブルシューティングに関する包括的なガイドです。
既存の Runbook（`docs/runbook/`）を補完し、監視設定・アラート基準・容量計画・パフォーマンスチューニングをカバーします。

---

## 目次

1. [デプロイメント構成](#1-デプロイメント構成)
2. [環境変数リファレンス](#2-環境変数リファレンス)
3. [監視・アラート設定](#3-監視アラート設定)
4. [SLO メトリクスの解釈とアラート基準](#4-slo-メトリクスの解釈とアラート基準)
   - [4.5 certified replication レーンの運用](#45-certified-replication-レーンの運用)
5. [バックアップ・リストア手順](#5-バックアップリストア手順)
6. [ログ設定とログレベル](#6-ログ設定とログレベル)
7. [パフォーマンスチューニング](#7-パフォーマンスチューニング)
8. [ネットワーク設定](#8-ネットワーク設定)
9. [ノード追加・削除手順](#9-ノード追加削除手順)
10. [配置ポリシー変更手順](#10-配置ポリシー変更手順)
11. [容量計画](#11-容量計画)
12. [トラブルシューティング](#12-トラブルシューティング)
13. [障害復旧手順](#13-障害復旧手順)
14. [Control plane Raft 運用](#14-control-plane-raft-運用)

---

## 1. デプロイメント構成

### 1.1 Docker Compose（推奨：開発・テスト環境）

プロジェクト付属の `docker-compose.yml` を使って 3 ノードクラスタを起動できます。

```bash
# 内部認証トークンを設定（省略可、省略時は認証なし）
export ASTEROIDB_INTERNAL_TOKEN="my-secret-token"

# クラスタ起動
docker compose up -d

# ポートマッピング
#   node-1: localhost:3001 -> container:3000
#   node-2: localhost:3002 -> container:3000
#   node-3: localhost:3003 -> container:3000
```

各ノードの設定ファイルは `configs/node-{1,2,3}.json` に配置されています。
設定ファイルの構造:

```json
{
  "node": {
    "id": "node-1",
    "mode": "Both",
    "tags": []
  },
  "bind_addr": "0.0.0.0:3000",
  "peers": {
    "self_id": "node-1",
    "peers": {
      "node-2": {
        "node_id": "node-2",
        "addr": "asteroidb-node-2:3000"
      },
      "node-3": {
        "node_id": "node-3",
        "addr": "asteroidb-node-3:3000"
      }
    }
  }
}
```

ノードモードは以下の 3 種類:
- `Store`: データストアのみ（レプリケーション対象）
- `Subscribe`: 読み取り専用レプリカ
- `Both`: ストア + Authority 合意に参加（デフォルト）

### 1.2 Docker 単体デプロイ

```bash
# イメージビルド
docker build -t asteroidb:latest .

# シングルノード起動
docker run -d \
  --name asteroidb-node-1 \
  -p 3000:3000 \
  -e ASTEROIDB_NODE_ID=node-1 \
  -e ASTEROIDB_BIND_ADDR=0.0.0.0:3000 \
  -e ASTEROIDB_ADVERTISE_ADDR=10.0.1.1:3000 \
  -e ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3 \
  -e ASTEROIDB_DATA_DIR=/data \
  -v /var/lib/asteroidb/node-1:/data \
  asteroidb:latest
```

### 1.3 systemd デプロイ（本番推奨）

```ini
# /etc/systemd/system/asteroidb.service
[Unit]
Description=AsteroidDB Node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=asteroidb
Group=asteroidb
ExecStart=/usr/local/bin/asteroidb-poc
Restart=on-failure
RestartSec=5

# 環境変数
Environment=ASTEROIDB_NODE_ID=node-1
Environment=ASTEROIDB_BIND_ADDR=0.0.0.0:3000
Environment=ASTEROIDB_ADVERTISE_ADDR=10.0.1.1:3000
Environment=ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3
Environment=ASTEROIDB_DATA_DIR=/var/lib/asteroidb
Environment=RUST_LOG=asteroidb_poc=info

# セキュリティ設定（トークンは EnvironmentFile で管理）
EnvironmentFile=/etc/asteroidb/env
# /etc/asteroidb/env に ASTEROIDB_INTERNAL_TOKEN=xxx を記載

# リソース制限
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

```bash
# サービス管理
sudo systemctl enable asteroidb
sudo systemctl start asteroidb
sudo systemctl status asteroidb
sudo journalctl -u asteroidb -f
```

### 1.4 Bare Metal デプロイ

```bash
# ビルド
cargo build --release

# バイナリ配置
sudo cp target/release/asteroidb-poc /usr/local/bin/

# データディレクトリ作成
sudo mkdir -p /var/lib/asteroidb
sudo chown asteroidb:asteroidb /var/lib/asteroidb

# 起動
ASTEROIDB_NODE_ID=node-1 \
ASTEROIDB_BIND_ADDR=0.0.0.0:3000 \
ASTEROIDB_ADVERTISE_ADDR=10.0.1.1:3000 \
ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3 \
ASTEROIDB_DATA_DIR=/var/lib/asteroidb \
RUST_LOG=asteroidb_poc=info \
/usr/local/bin/asteroidb-poc
```

---

## 2. 環境変数リファレンス

| 変数名 | 必須 | デフォルト | 説明 |
|--------|------|-----------|------|
| `ASTEROIDB_NODE_ID` | いいえ | `node-1` | ノードの一意識別子 |
| `ASTEROIDB_BIND_ADDR` | いいえ | `127.0.0.1:3000` | HTTP サーバのバインドアドレス |
| `ASTEROIDB_ADVERTISE_ADDR` | いいえ | `BIND_ADDR` と同じ | 他ノードからの接続先アドレス |
| `ASTEROIDB_CONFIG` | いいえ | なし | JSON 設定ファイルパス |
| `ASTEROIDB_AUTHORITY_NODES` | いいえ | `auth-1,auth-2,auth-3` | Authority ノード ID（カンマ区切り） |
| `ASTEROIDB_DATA_DIR` | いいえ | `./data` | データ永続化ディレクトリ |
| `ASTEROIDB_INTERNAL_TOKEN` | いいえ | なし | 内部 API 認証用 Bearer トークン |
| `ASTEROIDB_WRITER_KEYS` | いいえ | なし | 操作受信を有効化する writer 公開鍵と namespace 許可の JSON 配列。設定時は内部 token、操作 namespace、固定隣接先 manifest が必須。操作モードでは旧 eventual/certified の公開書き込みは `409` で拒否する |
| `ASTEROIDB_OPERATION_NAMESPACE_ID` | 条件付き | なし | 操作受信を許す単一 namespace。`ASTEROIDB_WRITER_KEYS` を設定した場合に必須 |
| `ASTEROIDB_OPERATION_CREATION_POLICY` | 条件付き | なし | 操作の作成時モードを検証する不変 JSON manifest。`ASTEROIDB_WRITER_KEYS` 設定時に必須。Create の CLI 署名にも同じ manifest を渡す |
| `ASTEROIDB_OPERATION_NEIGHBORS` | 条件付き | なし | 操作配送の固定隣接先 JSON 配列。`key_prefixes` は全 namespace に適用し、任意の `scoped_key_prefixes:[{"namespace_id":"orders","key_prefix":"item/"}]` は指定 namespace だけに適用する。`ASTEROIDB_WRITER_KEYS` を設定した場合に必須。末端ノードは `[]` |
| `ASTEROIDB_OPERATION_ROUTE_EDGES` | 任意・3変数同時 | なし | 許可隣接辺 JSON 配列。各辺の任意の `namespace_id` で適用先を絞れる。routed custody を有効にする場合に必須 |
| `ASTEROIDB_OPERATION_ROUTES` | 任意・3変数同時 | なし | 宛先・namespace・key prefix ごとの経路と減少 rank。行の `namespace_id` 省略時は全 namespace に適用 |
| `ASTEROIDB_OPERATION_REQUIRED_DESTINATIONS` | 任意・3変数同時 | なし | namespace・key prefix ごとに初回提出が必要とする最終 NodeId の配列。行の `namespace_id` 省略時は全 namespace に適用 |
| `ASTEROIDB_OPERATION_LINK_SEED` | routed 時に必須 | なし | 操作隣接リンク専用 Ed25519 秘密 seed（32 バイト hex）。Authority 鍵とは別にする |
| `ASTEROIDB_OPERATION_LINK_KEYS` | routed 時に必須 | なし | 隣接 NodeId と固定 Ed25519 公開鍵（32 バイト hex）の JSON 配列。`NEIGHBORS` と完全一致させる |
| `ASTEROIDB_OPERATION_OWNED_PREFIXES` | 条件付き | なし | このノードのアプリ読み取りに適用する担当範囲の JSON 配列。文字列は全 namespace に適用し、`{"namespace_id":"orders","key_prefix":"item/"}` はその namespace だけに適用する。`ASTEROIDB_WRITER_KEYS` 設定時に必須。中継専用は `[]`、全キー担当は `[""]` |
| `ASTEROIDB_OPERATION_CERT_SCOPE_PREFIX` | いいえ | なし | 静的な操作証明書受信を有効化するキー prefix。設定時は同 prefix の certified 配置ポリシー、Authority 定義、その Authority ID に対応する公開鍵、`ASTEROIDB_OPERATION_CERT_FAULTS` が必要 |
| `ASTEROIDB_OPERATION_CERT_FAULTS` | 条件付き | なし | 操作証明書で許容する Authority 故障数。静的な証明書受信を有効化した場合に必須 |
| `ASTEROIDB_OPERATION_CERT_SCOPES` | いいえ | なし | 証明書 scope を指定する JSON 配列。例: `[{"key_prefix":"orders/","faults":1},{"namespace_id":"other-ns","key_prefix":"orders/","faults":0}]`。`namespace_id` 省略時は `ASTEROIDB_OPERATION_NAMESPACE_ID` を使う。同じ namespace と prefix の重複は不可。キーには最長一致する prefix の Authority 定義と配置ポリシーを適用する。上の単一 scope 用 2 変数とは併用不可 |
| `ASTEROIDB_OPERATION_ASSIGNMENTS` | いいえ | なし | 操作用の namespace 別 Authority・配置設定を指定する JSON 配列。各項目は `namespace_id`、`policy`（`PlacementPolicy`）、`authority`（`AuthorityDefinition`）を持ち、両者の key prefix を一致させる。同じ namespace と prefix の重複は不可。未指定 scope は従来の prefix 設定を使う |
| `ASTEROIDB_BLS_SEED` | いいえ | なし | 署名鍵生成用 hex シード（32 バイト）。Ed25519 署名鍵と（`native-crypto` ビルドでは）BLS 鍵ペアの両方をこのシードから導出し、frontier 報告への署名（FR-008）を有効化する。`native-crypto` 無効ビルドでは Ed25519 のみで署名する |
| `ASTEROIDB_AUTHORITY_KEYS` | いいえ | なし | ピア Authority の公開鍵（`<node-id>=<ed25519 hex 64 文字>[/<bls hex 96 文字>/<pop hex 192 文字>]` をカンマ区切り）。ピアの署名付き frontier を検証するために必須。第 3 セグメントは BLS 鍵の **Proof-of-Possession（PoP）**——公開鍵そのものへの署名で秘密鍵の所有を証明し、BLS 集約検証に対する rogue-key 攻撃を防ぐ（draft-irtf-cfrg-bls-signature §3.3）。各ノードは起動ログに自身の配布用エントリ（`Authority key entry for ASTEROIDB_AUTHORITY_KEYS distribution: <node-id>=<ed>/<bls>/<pop>`）を出力するので、そこから PoP 付きのエントリをコピーする。BLS 部を省略したピア、または PoP を欠く 2 セグメント旧形式（`<ed>/<bls>`）は BLS レーンが破棄され Ed25519 のみで検証される（degrade、下記のローリングアップグレード手順を参照）。PoP の検証に失敗したエントリは（lenient モードでは）警告付きでスキップされる。`ASTEROIDB_BLS_SEED` 未設定でも本変数のみで検証専用のキーセットレジストリが構築される |
| `ASTEROIDB_REQUIRE_SIGNED_FRONTIERS` | いいえ | `false` | `1`/`true` で無署名 frontier 報告の受理を拒否（strict モード）。**全ノードへの鍵配布（`ASTEROIDB_AUTHORITY_KEYS`）完了後に有効化する運用切替**。署名付きで検証に失敗した報告はこの設定に関わらず常に拒否される。strict モードでは加えて `ASTEROIDB_AUTHORITY_KEYS` に PoP 無し／不正な PoP を持つ BLS 鍵エントリがあると起動時にエラー終了する（`native-crypto` 無効ビルドは PoP を暗号検証できないため hex 長などの構文検査のみを行う）。キーセットレジストリを構築できない構成（`ASTEROIDB_BLS_SEED` と `ASTEROIDB_AUTHORITY_KEYS` の両方が未設定）で有効化した場合も、ノードは起動時にエラー終了する |
| `ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES` | いいえ | `false` | `1`/`true` で、equivocation 証拠が記録された Authority の attestation を**証明書組み立てから除外**する（frontier の前進自体は許容——frontier 値は単調 max 情報で毒性が低い）。除外は 3 点で強制される: (1) HTTP 受信経路での検証時＋**apply 時の再チェック**（同一リクエスト内で告発が後続しても前方の attestation がすり抜けない）、(2) **告発時の attestation pool purge**（告発前にプール済みの最大 128 checkpoint 分の attestation を除去。`attestation_purged_total` で観測可能）、(3) **自己報告経路**（自ノードが告発された場合、自身の attestation も pool へ入れず、既存分を purge する）。過半数のしきい値の分母は縮まないため除外は常に安全側（証明を難しくする方向）にしか働かないが、除外により当該 scope が過半数割れすると **certificate 生成が停止する可用性コスト**がある。デフォルトは検知のみ（警告ログ＋証拠保存＋メトリクス）で、除外は運用者の明示的な opt-in。**`0`（既定）では purge は発動せず、告発済み Authority の attestation は detect-only 契約どおり証明書に混入し続ける**。なお、告発**前**に組み立て済み・キャッシュ済みの証明書の遡及失効は行わない（証明書失効プロトコルは将来課題、`docs/followup-plan.md` 参照） |
| `ASTEROIDB_DIGEST_SYNC_DISABLED` | いいえ | `false` | `1`/`true` で digest 段階 diff 同期（フルシンク前のキー範囲 digest 比較）を無効化し、従来のフルシンクのみのフォールバック動作へ切り戻す（ops キルスイッチ。3.6 を参照） |
| `ASTEROIDB_CERTIFIED_SYNC_DISABLED` | いいえ | `false` | `1`/`true` で certified replication レーン（P0-1、同期 tick ごとの certified pull）を無効化し、レーン導入前の挙動へ切り戻す。pull は**一切発行されない**（結果を捨てるのではなくリクエスト自体を出さない）。有効化中は certified write がそれを受理したノードの喪失で失われうる（ops キルスイッチ、要再起動。4.5 を参照） |
| `ASTEROIDB_FRONTIER_STORE_DIGEST` | いいえ | `true` | `0`/`false` で frontier 報告の `digest_hash` を M-12 以前のプレースホルダ形式（`{node}-{physical}-{logical}`）へ切り戻す（ops キルスイッチ、要再起動）。既定の有効時は eventual store の M-7 root digest（`sd2:<hex64>`）を束縛し、データ内容の split-view 検知が働く。有効化には data dir（ReportClockFloor の永続化先 `frontier_report_clock.json`）が必要で、floor が構成できない場合は自動的にプレースホルダ形式へ fail-safe する。floor ファイルが無い起動（初回起動・floor 喪失）は 180 秒の activation grace の間 **frontier 報告そのものを停止**し（前世代がどの形式で署名していたか不明なため、無署名だけが両形式方向に衝突フリー）、grace 明けに `sd2:` 形式で報告を再開する（「Equivocation / split-view 検知」節を参照） |
| `ASTEROIDB_GC_HOLE_JUMP` | いいえ | `false` | `1`/`true` は起動時に拒否する。現在の peer registry だけでは、除外後に再参加する peer が保持する live dot の不在を証明できない（3.7 を参照） |
| `RUST_LOG` | いいえ | `info` | ログレベル（tracing-subscriber 形式）。**未設定・空文字列の場合は `info` 相当**（6.1 を参照）。値を明示した場合はその指定がそのまま尊重される（`RUST_LOG=error` で ERROR のみに絞ることもできる） |

操作受信の設定例（公開鍵は Ed25519 の 32 バイトを hex で指定）:

```sh
export ASTEROIDB_WRITER_KEYS='[{"writer_id":"writer-1","public_key_hex":"<64桁のhex>","namespaces":["example"]}]'
export ASTEROIDB_OPERATION_NAMESPACE_ID=example
export ASTEROIDB_OPERATION_CREATION_POLICY='{"version":1,"ranges":[{"namespace_id":"example","key_prefix":"","default_mode":"Disabled","override_writers":[]},{"namespace_id":"example","key_prefix":"orders/","default_mode":"Enabled","override_writers":["writer-1"]}]}'
export ASTEROIDB_OPERATION_NEIGHBORS='[{"node_id":"neighbor-1","addr":"127.0.0.1:3001","key_prefixes":["orders/"]}]'
export ASTEROIDB_OPERATION_OWNED_PREFIXES='["orders/"]'
export ASTEROIDB_INTERNAL_TOKEN='<既存クラスタと共有するトークン>'
```

操作モードでは、全操作ノードに同じ `ASTEROIDB_OPERATION_CREATION_POLICY` を配布する。例: `{"version":1,"ranges":[{"namespace_id":"example","key_prefix":"","default_mode":"Disabled","override_writers":[]},{"namespace_id":"example","key_prefix":"orders/","default_mode":"Enabled","override_writers":["writer-1"]}]}`。namespace 内の最長一致 prefix が既定モードを決める。署名済み作成宣言が既定値と異なる場合は、その作成者が該当範囲の `override_writers` に明示されているときだけ受け付ける。更新・削除も元の署名済み作成を照合する。該当範囲がなければ拒否する。manifest の digest は `operation_inbox/.creation-policy-manifest` と Authority worker の `operation_authority/creation-policy-anchor.bin` に固定される。`operation-sign-create --creation-policy-file` にも同じ manifest を渡し、CLI で宣言を検証して digest を署名する。操作状態がある inbox への後付け、設定変更、設定削除は起動時に拒否する。動的な版切替は未実装。Enabled の作成宣言が証明書を得るには、別途 certified 配置ポリシーと Authority scope が必要。

CLI の Create packet は、作成者が宣言と Create 本体の commitment に署名する値なし作成証拠を持つ。Add/Delete packet は Create の初期値を埋め込まない。Authority は後続操作を署名する前に、一致する Create 本体・receipt、または同じ inbox にある完全な署名済み policy witness を要求する。先に届いた Add/Delete は作成根拠の到着後に再照合する。

この設定で署名済み操作の受信ルートを起動し、受信パケットを `ASTEROIDB_DATA_DIR/operation_inbox` に永続化する。受信側は完全な署名付きパケットを先に fsync し、担当キーなら操作 ledger も fsync してから ACK する。後続の outbox 登録だけが失敗した場合も ACK を返し、保存済み入力から稼働中または再起動後の再照合で登録を補う。パケット保存後・ledger 保存前に停止した場合は、再起動時に検証済みパケットから ledger を復元する。署名付きパケットのない ledger 本体は起動時に拒否する。`key_prefixes` が操作キーに一致する固定隣接先へ配送し、全キーを許可する隣接先には `key_prefixes:[""]` を明示する。保存済み送信項目は再起動後も再試行し、inbox の全パケットと固定隣接先を照合して不足した項目を補う。設定不足、不正な manifest、復旧できない inbox/outbox は起動時にエラーとする。全ノードで隣接先 ID と接続先を管理し、変更時は古い outbox 送信先も残ることに注意する。操作モードでは旧 `/api/eventual/*`、`/api/certified/*`、`/api/status/*` と旧内部データ同期 API を `409 Conflict` で拒否する。旧ストアの WAL・snapshot は読み込まず、旧データ処理 worker・checkpoint も動かさない。制御プレーン、membership、epoch 処理は稼働し、操作の証明書経路は次段の静的設定で有効にする。

隣接 ACK は署名付きパケットと送信元・転送 ID・受信先の転送記録が受信側に保存されたことだけを示す。隣接 outbox は `operation_outbox` に version 2 の送信記録を保存し、`operation_inbox/transfers` に受信転送記録を保存する。異なる隣接先や転送 ID の ACK は再送停止に使えない。完了通知は同じパケットが最終宛先へ永続取り込みされたことを示し、Certified の成立や現在値の有効性は示さない。固定 prefix は配送先選択であり、担当外ノードのアプリへの適用可否や Authority の署名許可を代替しない。

`operation_inbox`、`operation_outbox`、support/certificate outbox、Authority と操作台帳の各保存先は一つのプロセスだけが開ける。二重起動は復旧や採番の前に排他エラーで停止する。不確定な書き込みの後は、受信処理や再送ワーカーが保存済み記録を再読込・検証してから処理を再開する。再検証に失敗している間は古いメモリ状態から ACK・署名・配送完了を返さず、公開 read/status は HTTP 503 を返す。

inbox や evidence への保存後に outbox 登録だけが失敗した場合、定期ワーカーは保存済みの操作・support・certificate から配送義務を再発見する。稼働中の復旧に再起動を要求しない。公開の操作提出は inbox 保存が完了すれば受領票を返し、outbox 登録の失敗は警告として記録する。受領票はローカル保存を示すもので、配送完了や Certified 到達を示さない。

Authority worker は初回起動時に配置と入れ子の規則、writer 権限、Authority 鍵集合の指紋を保存し、再起動時に照合する。署名済み状態があるのにこの信頼アンカーがない旧 Authority ディレクトリは起動を拒否する。既存ディレクトリの自動移行は未実装。

routed モードのローカル提出は、完全な署名付き本体を `operation_inbox/roots` に先に fsync し、packet・ledger・outbox の順に保存する。途中で停止しても起動時に root の本体から packet と ledger を復元し、配送義務を再登録する。

古いローカル root の署名付き本体は、その操作の Certified 証拠に基づく ledger receipt が保存され、必要な全最終宛先への root 配送に ACK と完了通知が永続記録され、ローカル Authority の署名待ち義務がない場合に回収する。先に `operation_inbox/root-tombstones` へ操作 ID・キー・作成モード・パケットと操作の commitment を fsync し、その後 `roots` と共有 packet を削除する。再起動時は tombstone を ledger receipt と outbox の完了済み配送記録に照合する。照合できなければ起動を拒否する。同じ署名付き操作の再提出には保存済み receipt を返し、本体は復元しない。

宛先別 routed custody を使う場合、上記3変数と `OPERATION_LINK_SEED`、`OPERATION_LINK_KEYS` を設定する。`ROUTE_EDGES` は `[{"source":"a","receiver":"b","key_prefixes":[""]}]`、`ROUTES` は `[{"source":"a","destination":"c","key_prefix":"","next_hop":"b","rank":2}, {"source":"b","destination":"c","key_prefix":"","next_hop":"c","rank":1}]`、`REQUIRED_DESTINATIONS` は `[{"key_prefix":"","destinations":["c"]}]` の形。各ノードの `NEIGHBORS` には、そのノードから出る route の next hop とアドレスを登録する。完了通知を返すため、受信側には上流の送信元 ID とアドレスも登録する。routed モードは最終宛先、親の送信元・転送 ID、depth を version 2 転送に保存する。受信側は全体経路と最終宛先を検証してから ACK し、親を持つ子配送だけを登録する。最終宛先は受信済み転送と署名付き本体を `operation_inbox/terminal` に結び付けて保存し、元の送信者に完了通知を再送する。中継元は完全な転送記述を照合して `operation_outbox/*.done` に保存し、子配送の保存 ACK も揃った後で親へ通知する。完了通知だけでは子配送の ACK 待ちは解除されない。通知元は上流の保存応答を `operation_inbox/report-acks` に記録するまで再送する。再起動時は `operation_inbox/roots` のローカル提出と `operation_inbox/transfers` の受信記録から義務を再構築する。全 inbox パケットを隣接先へ配る legacy モードは3変数を設定しない場合に継続する。`operation_outbox/.mode` はモードの混在を、`.routing` は経路・必要宛先・ローカル隣接設定の暗黙の変更を拒否する。既存の配送ディレクトリを routed モードへそのまま切り替えることはできない。

`LINK_KEYS` は `[{"node_id":"b","public_key":"<64桁hex>"}]` の形で、各ノードの隣接先を過不足なく列挙する。NodeId は ASCII 英数字、`.`、`_`、`-` のみを使う。各ノードの seed は固有にし、公開鍵を相手側 manifest に固定する。転送、完了通知、回収境界に加え、Authority support と証明書の要求と保存 ACK も専用 Ed25519 鍵で署名し、送信元・受信先・HTTP メソッドとパス・本文 SHA-256・元要求の digest に結び付ける。同一の署名済み要求の再送は保存済み転送の照合と境界の単調性で扱う。鍵やアドレスの暗黙変更は `operation_inbox/.link-manifest` と `operation_outbox/.link-manifest` で拒否する。署名無しで処理した既存 routed 配送記録の移行も拒否し、空の配送ディレクトリからのみリンク認証を有効化できる。support と証明書の既存 outbox も署名無し ACK を再利用しないよう起動時に確認する。

`REQUIRED_DESTINATIONS` にはアプリ担当ノードに加え、操作の署名を求める Authority ノードも指定する。routed モードの非担当 Authority は、静的 Authority 定義・scope・有効な作成宣言に一致する操作だけを `operation_inbox/signing-inbox` に保存してから最終受け渡し ACK を返す。署名 worker はローカル提出とこの処理待ち記録だけを対象にし、中継中のコピーからは署名しない。Authority の `OWNED_PREFIXES` は `[]` にでき、配送用・署名用の本体保持だけで公開読み取りを有効にしない。
Enabled 操作も、担当 Authority への経路や必要宛先が欠けていてもローカル root を保存し、送れる宛先だけ配送登録する。署名・作成根拠・所有キーの検査は維持する。証明書を集めるには担当 Authority へ届く経路を設定する必要があり、欠けたままなら操作は Pending に残る。起動時には certificate scope の信頼済み名簿と `n > 3f` を検査し、経路の欠落だけで起動を拒否しない。後から routing pin を変更できるのは転送 ID を一度も発行せず、inbox に受信転送・退役記録・回収済み root がない場合だけ。neighbor・link・Authority の pin 変更と、既存転送を持つノードでの経路修復は未対応。
操作本体の `ROUTES` は support・証明書の配送経路を設定しない。証拠は各ノードの `NEIGHBORS` に沿って転送するため、Authority から担当ノードまでの証拠経路に入る中継ノードにも、その namespace/key を覆う `ASTEROIDB_OPERATION_CERT_SCOPES`、Authority 鍵と配置・Authority 定義を設定する。中継専用ノードの `OWNED_PREFIXES` は `[]` のままでよい。たとえば A→B→C しか通信できず A が Authority、C が担当なら、B に証明書 scope がないと操作本体は C に届いても A の support は B の受信経路で 404 となり、C は証拠を得られない。現在の起動時検査はデータ経路だけを確認し、証拠経路の成立までは検査しない。
最終受け取り Authority の scope、配置・Authority 定義、ローカル NodeId は `operation_inbox/.authority-terminal-manifest` に固定する。再起動時の暗黙の担当変更を拒否し、未固定の既存 inbox 状態に後から担当を付けることも拒否する。担当変更には保存済み配送・署名義務を引き継ぐ明示的な移行が必要。
policy 付き routed relay では、提出用の配置・Authority 定義と namespace 別 assignment も同じ pin に含める。再起動時に異なる設定を渡すと root の配送再登録より前に起動を拒否する。稼働中の control-plane 変更後は新しい Enabled 提出を拒否し、変更前に保存した root は mount 時の設定で配送を再登録する。
この提出用 policy を含まない旧形式の Authority pin がある既存 inbox は自動移行しない。保存済み配送・署名義務を調べた上で明示的な移行が必要。

送信側は ACK と完了通知の両方を fsync した配送 ID だけを終了とする。`operation_outbox/.index` は連続して終了した ID の排他的境界と、穴より後の終了 ID を保持する。更新中は `*.closed` を回復用記録として残し、境界が永続化された後に個別の `*.ack` と `*.done` を消す。routed モードでは終了済み `*.send` の署名付き本体を回収し、転送 ID・送信元・宛先・親配送・操作 ID・packet commitment の記述を残す。未終了の `*.send` は再送用の完全な本体を保持する。残した記述は再起動時の配送照合と子配送の完了判定に使う。`operation_outbox/.next-id` は次の採番位置を永続化し、再起動時にも終了済み ID を再利用しない。保存済みの連続境界だけを元の隣接受信先に繰り返し通知し、受信側は送信元別の単調な境界を `operation_inbox/retirement-fences` に fsync してから詳細を削除する。受信転送と完了通知応答は境界未満だけ回収し、`operation_inbox/transfer-catalog` は配送 ID と要求の対応を保持する。ledger の値本体が証明済みの冗長データとして receipt に置き換わり、同じ操作の全受信転送が終了境界未満になった場合は、catalog の完全な転送を操作 ID・署名付き packet commitment・ledger 本体 commitment・宛先・親配送の記述へ置き換え、`operation_inbox/packets` の署名付き本体を回収する。ローカル提出、Authority 署名待ち、未終了の受信転送に必要な本体は保持する。compact 記述から元の packet を復元できるとは扱わず、再送・競合判定は SHA-256 commitment の一致に依存する。別の送信元から同じ操作が新しい配送 ID で届けば、その署名付き本体を再び保存して未終了配送に使う。終了境界未満の最終取り込み記録も回収する。Authority の署名処理待ち記録は、ローカル root 発行、検証済み証拠による ledger receipt 化、送信側の退役境界が揃うまで保持し、その後は完全な転送を削除する。root log は発行済み操作の同一性を、転送 catalog は配送 ID と packet commitment を保持する。記録削除と packet 回収の間で停止しても、再起動時に回収を再試行する。遅延した古い転送には新しい ACK を返さない。境界より後の配送は通常どおり進む。routed モードの配送・完了・境界通知は隣接先の固定公開鍵で署名を確認する。legacy モードは共有内部 token に依存する。

証明済みの冗長な ledger 本体を receipt に置き換えた後、`operation_evidence.log` も値を含まない証拠レコードへ原子的に書き換える。必要な隣接先への完全な証明書が certificate outbox に保存済みでない操作は、この書き換えの対象にしない。未完了の配送コピーは値を保持し得るため、古い値のバイト列をノード全体から完全に消す GC は未実装。保持先と残る接続は [操作値の物理回収](design/operation-value-retention.md) を参照。

旧 version 1 の `operation_outbox/*.send` がある状態では起動を拒否する。アップグレード時はノードを停止し、`operation_outbox` ディレクトリを別名に退避してから起動する。起動時に署名済み `operation_inbox` の全パケットから現在の固定隣接先への送信義務を再登録するため、旧 ACK 済み送信も再配送される。退避したディレクトリは復旧確認まで保管する。元の署名済み inbox が欠けている場合はこの再登録に頼れないため、先に保存状態を修復する。
`ASTEROIDB_OPERATION_OWNED_PREFIXES` はそれとは独立した、このノードの読み取り対象である。中継専用ノードも署名付き本体と証拠を配送用に保持するが、公開の通常読みと対応する内部読みは担当外キーに `Unknown` を返す。Certified 読みは担当外でも認証済み Disabled の作成根拠があれば `NotTargeted`、それ以外は `Unknown` を返す。操作 ID 状態も担当外では `unknown` とし、配送用コピーだけでアプリに適用されたとは報告しない。担当 prefix は operation inbox の信頼アンカーに固定し、再起動時の変更を拒否する。既存状態があるのにアンカーがない inbox は起動を拒否する。現行の担当 prefix は静的な運用入力で、配置変更時の自動移管は未実装。
担当ノードは `POST /api/operations/submit` で署名済み操作を受け付ける。writer と作成操作の署名・namespace 権限を検証し、ローカル保存と隣接配送義務を永続登録した後で応答する。認証には writer 署名を使い、内部 token は要求しない。中継専用ノードでのローカル提出は拒否する。受信済み操作の公開通常読みは `GET /api/operations/read/{namespace_id}/{key}` で行い、証明書モードでは `GET /api/operations/certified-read/{namespace_id}/{key}` が検証済み証明書を各操作の根拠として返す。これらの公開読みは内部 token を要求しない。
静的証明書モードでは `POST /api/operations/submit-certified?wait_ms=30000` で Certified 到達を待てる。待機が時間切れになった場合も操作は保持・配送され続ける。`202 pending` の後は、提出したパケットの操作 ID を `GET /api/operations/status/{namespace_id}/{writer}/{counter}` で再確認する。`wait_ms` の既定値は 30000、上限は 60000。

操作証明書の静的受信を有効にする場合、`ASTEROIDB_OPERATION_CERT_SCOPE_PREFIX` と `ASTEROIDB_OPERATION_CERT_FAULTS` を設定する。起動時に同 prefix の certified 配置ポリシーと Authority 定義を照合し、より狭い prefix による上書きがないこと、定義の Authority ID と現在配布されている Ed25519 鍵集合が一致することを確認する。鍵集合、scope、ポリシー版、epoch 設定、故障許容数を `operation_evidence` の信頼アンカーへ固定し、変化した場合は support・証明書の受信と読み取りを拒否する。Authority ノードには登録済みの署名鍵を設定する。受信した操作を専用 ledger に保存し、発行済み root を `operation_authority` に fsync してから、自分の support を `operation_supports` と隣接先別の `operation_support_outbox` に登録する。中継ノードも受信した support を検証・永続化してから固定隣接先へ転送し、ACK がない項目は再送する。起動時には support log から証明書と outbox の不足項目を補う。このモードでは操作証明書を固定 epoch 0 で扱い、鍵・scope の運用中ローテーションは対象外。ローテーションには信頼できる履歴を別途実装する必要がある。

同じノードに複数の証明書 scope がある場合は `ASTEROIDB_OPERATION_CERT_SCOPES` を使用する。各項目で独立に配置ポリシー、Authority 定義、故障許容数を検証し、support・証明書・Authority の状態と再送キューを `operation_certificate_scopes/<scope hash>/` 以下に保存する。単一 scope の旧変数で起動する場合は従来の `operation_evidence` などの保存先を維持する。scope hash は固定のドメイン文字列、namespace ID と key prefix の長さ・バイト列から SHA-256 で決定する。旧変数から複数 scope 変数への切替だけでは従来の保存済み証拠は新しい保存先へ移らない。保存済み scope を設定から外す場合や証明書設定を解除する場合も起動を拒否するため、既存ノードの切替には保存先の明示的な移行が必要となる。

異なる namespace の同じ key prefix に異なる Authority 集合や配置ポリシーを割り当てる場合は、各ノードに同じ `ASTEROIDB_OPERATION_ASSIGNMENTS` を設定する。この設定は操作用のローカル静的設定であり、既存 Raft の prefix 単位の設定には追加されず、SystemNamespace の保存 JSON にも書かれない。再起動時にも必ず指定する。操作では namespace と key prefix の最長一致を使い、同じ prefix 長では namespace 指定行を優先する。設定を変更すると保存済み証明書の信頼アンカーが一致せず、起動を拒否する。既存の prefix 単位の設定を使う scope の digest は変更しない。

各 scope はその Authority 定義に含まれる ID とグローバルな信頼済み公開鍵から検証用 keyset を作るため、scope ごとに異なる Authority 集合を設定できる。包含関係のある prefix では、子 scope のキーを親 scope の Authority が署名・検証しない。追加した namespace の署名済み操作も同じ inbox で受け付ける。その namespace は writer の許可一覧と作成ポリシーにも登録する必要がある。既存 inbox の作成ポリシー manifest は固定されるため、運用中に namespace 範囲を追加するには状態移行が必要となる。担当範囲、隣接先、最終宛先、経路は namespace ごとに設定できる。namespace 指定のない行は全 namespace に適用し、key prefix の最長一致を優先して、同じ長さなら namespace 指定行を選ぶ。操作用の namespace 別 Authority・配置設定は静的入力であり、Raft による配布は未実装。Authority が個別 packet を拒否した場合は警告を記録して次の packet を処理する。admission ledger の I/O 異常や署名 root の永続化異常では worker を停止する。

routed custody と上記の操作証明書受信が両方有効な場合、30 秒ごとに操作 ledger の値本体を回収する。証明書を現在の固定信頼設定で再検証し、削除済みまたは認証済みの勝者に吸収された値だけを対象にする。対象操作の署名付き packet と、全必須宛先に対する永続送信登録を照合する。Enabled のローカル root は、必須宛先が担当 Authority 全員を含むことも確認し、欠ける間は root と値本体を回収しない。送信 ACK は不要で、未完了の送信は packet と outbox の記録から引き続き再試行する。ledger は ID receipt と checkpoint を永続化して旧 WAL の本体を削除する。署名付き packet、転送カタログ、outbox の送信本体は残るため、この処理だけでは操作データ全体のディスク容量は回収されない。

同じ周期処理で、receipt 化済み操作の証明書を値なしの署名・包含証拠へ圧縮し、ACK 済みの support・証明書送信を値なしの識別記録へ置換する。support ストアの本体は、証明書の compact 証拠を再検証でき、各 support の必要な隣接先から ACK が永続化された場合に限って圧縮する。Authority の発行 root と admission ledger も、対応する検証済み証拠と全必要先の support ACK が揃った後に値なしの root 識別記録と admission receipt へ圧縮する。root 書き換えを先に永続化し、途中停止後は admission ledger の checkpoint を再実行する。最初の support または root 圧縮時に `ASTEROIDB_OPERATION_NEIGHBORS` の配送先と適用範囲を永続的に固定する。圧縮後にその設定を変更したノードは再起動を拒否するため、変更には未完了の過去の配送を維持する状態移行が必要となる。

quorum が揃って完成した証明書も、受信済み証明書も、検証して `operation_evidence` に fsync した後、キーを許可する固定隣接先ごとに `operation_certificate_outbox` へ登録して再送する。受信側は証明書を検証・永続化してから証明書内容の commitment を ACK する。次ホップの送信登録だけが失敗した場合は、保存済み証明書から再照合で不足義務を補う。本体より証明書が先に到着しても証明書は保持され、担当ノードの Certified 読みには本体が届いてから反映される。証明書配送には support と同じ固定隣接設定を使う。隣接先の網羅性、通信回復、接続先 ID の認証はこの設定と運用に依存する。

### Control plane Raft コンセンサス

ポリシー / Authority 定義の変更は内蔵 Raft で合意される（運用手順は
[14. Control plane Raft 運用](#14-control-plane-raft-運用) を参照）。

| 変数名 | 必須 | デフォルト | 説明 |
|--------|------|-----------|------|
| `ASTEROIDB_CONTROL_PLANE_NODES` | いいえ | 自ノード ID のみ | Raft **投票者集合**（ノード ID をカンマ区切り、**静的**）。既定では単一ノードクラスタとして起動直後に自身がリーダーになる。**全ノードで同一の値**を設定すること。gossip の PeerRegistry からは導出しない（ping 失敗による自動 evict で過半数の定義が変わり選挙安全性が壊れるため） |
| `ASTEROIDB_RAFT_PEERS` | 複数ノード時は推奨 | なし | Raft ピアの静的アドレスマップ（`id=host:port` をカンマ区切り、例 `cp-1=10.0.0.1:3000,cp-2=10.0.0.2:3000`）。未設定のピアは gossip（join 済みの PeerRegistry）でアドレス解決される——コールドスタート時は join 完了まで選挙が始まらない可能性がある |
| `ASTEROIDB_RAFT_ELECTION_TIMEOUT_MIN_MS` | いいえ | `5000` | 選挙タイムアウトの下限。**対象リンクの実測 RTT（p99）から逆算して設定する**こと。論文の推奨値 150–300ms は broadcast ≈15ms の DC 内前提であり、高遅延リンクにそのまま使うと選挙嵐になる。目安: heartbeat 間隔の 5 倍以上、かつリンク RTT p99 の 10 倍以上 |
| `ASTEROIDB_RAFT_ELECTION_TIMEOUT_MAX_MS` | いいえ | `10000` | 選挙タイムアウトの上限（この範囲でランダム化される）。split vote を減らすため MIN の 1.5〜2 倍を推奨 |
| `ASTEROIDB_RAFT_HEARTBEAT_MS` | いいえ | `1000` | リーダーのハートビート（AppendEntries）周期。`heartbeat * 3 > election_timeout_min` の場合は起動時に警告が出る |
| `ASTEROIDB_RAFT_PROPOSE_TIMEOUT_MS` | いいえ | `30000` | ポリシー変更 1 件が commit されるまでの待ち時間。超過時は HTTP 504（少数派側の分断中はこのタイムアウトで失敗する） |
| `ASTEROIDB_RAFT_LOG_MAX` | いいえ | `4096` | ログ tail がこのエントリ数を超えると、適用済みエントリをスナップショットへ畳み込む（コンパクション）。無制限成長の防止線 |
| `ASTEROIDB_OBSERVER_NS_PULL_MS` | いいえ | `5000` | **非 voter（observer）ノードのみ**: voter から committed 済み制御プレーン状態を pull する周期（M-17）。observer の namespace（＝その authority 署名が乗せる policy_version）はこの pull だけで policy bump に追随する。`0` で無効（テスト・再現用。無効化すると observer namespace は凍結し、observer authority は次の bump で **無音で** certification 定足数から脱落する——§14.8）。voter では無視される |

**Raft 永続化は `ASTEROIDB_PERSISTENCE=off` でも常時有効**:
`currentTerm` / `votedFor` / ログの fsync（`$ASTEROIDB_DATA_DIR/raft/` 配下）は
選挙安全性の要件であり、無効化すると再起動後の二重投票（split-brain）を許してしまう。
書き込みパスで同期 fsync するため、遅いディスクはハートビートを遅らせ偽選挙を誘発し得る
（保守的な既定タイムアウトはこれを織り込んでいる）。

### 永続化（WAL・スナップショット）

| 変数名 | 必須 | デフォルト | 説明 |
|--------|------|-----------|------|
| `ASTEROIDB_PERSISTENCE` | いいえ | `on` | `off` で永続化を完全に無効化（従来の純メモリ動作。スナップショットのロードも WAL も行わない）。開発・ベンチ用エスケープハッチ |
| `ASTEROIDB_WAL_SYNC` | いいえ | `always` | WAL の fsync ポリシー: `always` / `interval` / `off`（下記の耐久性保証表を参照） |
| `ASTEROIDB_WAL_SYNC_INTERVAL_MS` | いいえ | `100` | `interval` モードの fdatasync 周期（ミリ秒）。`0` は 1 ms に補正する |
| `ASTEROIDB_SNAPSHOT_INTERVAL_SECS` | いいえ | `300` | チェックポイント（スナップショット保存 + 封印済み WAL セグメント削除）の周期。`0` で周期チェックポイント無効（graceful shutdown 時の最終チェックポイントは行う） |
| `ASTEROIDB_WAL_SEGMENT_BYTES` | いいえ | `67108864` (64 MiB) | WAL セグメントのローテーション閾値 |
| `ASTEROIDB_WAL_RECOVER_TRUNCATE` | いいえ | `0` | `1` で、mid-log corruption 検出時に最初の無効レコード以降を**明示的に切り捨てて**起動する（既定は fail-stop。13.6 の runbook を参照） |

**ディスクレイアウト**（`$ASTEROIDB_DATA_DIR` 直下）:

```
eventual.snapshot.bin        eventual ストアのスナップショット (bincode)
certified.snapshot.bin       certified ストアのスナップショット
wal/eventual/wal-<seq>.log   eventual ストアの WAL セグメント
wal/certified/wal-<seq>.log  certified ストアの WAL セグメント
```

**fsync ポリシーと耐久性保証**:

| モード | ack のタイミング | 保証 |
|--------|-----------------|------|
| `always`（既定） | WAL append 後、group-commit fdatasync の完了を待って ack | ack 済み書き込みはプロセス/OS クラッシュ・電源断で失われない。fdatasync 失敗は fail-stop（プロセス abort） |
| `interval` | append 直後に ack。背景タスクが N ms ごとに fdatasync | プロセスクラッシュ単体では損失ゼロ（ページキャッシュ生存）。OS クラッシュ / 電源断では最大 N ms ぶんの ack 済み書き込みを**ローカルでは**喪失し得る（ピアへ複製済みの分は anti-entropy で回復） |
| `off` | append 直後に ack。明示 fsync なし | プロセスクラッシュでは損失ゼロ。OS クラッシュ時の耐久性は OS のライトバック（数十秒）依存。開発・ベンチ用 |

内部の full-state push と digest の応答は、応答が参照した eventual ストア状態の
WAL 同期完了を `durable` で示す。`interval` では内部応答が同期完了を待つ。
`off` または永続化無効の受信ノードは通常の同期に応答するが `durable=false` を返し、
送信ノードはその応答で push の確定済み進捗や tombstone GC の peer ACK を更新しない。
したがってその構成では、該当 peer を含む GC gate は通らない。

**ディスクフル時の挙動**: WAL append の失敗は書き込み API に HTTP 503
（`STORAGE_UNAVAILABLE`）を返す degrade 動作で、読み取りは継続する。
スナップショット保存が失敗すると封印済み WAL セグメントは削除されず
**WAL が伸び続ける**ため、データディレクトリのディスク使用量は必ず監視
すること。ENOSPC は自己回復しない——容量を確保して次のチェックポイント
成功を待つ。fdatasync 自体の失敗（`always`/`interval`）は fail-stop。

ローテーション途中で ENOSPC 等により作成が失敗して残る orphan セグメント
（ヘッダ長以下の torn create）は、次のローテーションが**再起動なしで自動
回収**する（容量回復後の append / チェックポイントはそのまま成功する）。
例外として、rotate が `refusing to reclaim segment` を含む `InvalidData`
で失敗し続ける場合（通常運用では発生しない不変条件違反）、当該ファイルは
frame を含む可能性があるため自動では削除・切り詰めされない。プロセスを
再起動した際の挙動は衝突ファイルの内容に依存する:

- 内容が正規の WAL セグメントとして解釈できる場合（多重 writer が作った
  active セグメント等）は、起動時回復が torn tail / 空セグメントとして
  処理して復旧する。
- 内容が WAL として解釈できない場合（ヘッダ不正や不正 frame を含む異物
  ファイル）は、起動は `WAL ... is corrupted mid-log` で fail-stop する。
  この場合は 13.6 症状 B の runbook に従うこと。なお当該ファイルは常に
  最大 seq のセグメントであり ack 済みレコードを一切含まないため、この
  ケースに**限って**は、内容を保全・確認したうえでファイルを退避（移動）
  するか、`ASTEROIDB_WAL_RECOVER_TRUNCATE=1` で切り捨てて起動しても
  ack 済みデータの損失はない。

いずれの場合も、根本原因として同一データディレクトリに対する多重プロセス
起動（または外部ツールによるファイル混入）を疑うこと。

**注意事項**:

- eventual / certified の 2 系統はそれぞれ独立にチェックポイントされる
  ため、バックアップ内で両者の「整合時点」は厳密には一致しない（CRDT
  マージにより復元後は自動収束する）
- WAL セグメントのフォーマットを変更するリリースは、**新バイナリ側が旧
  フォーマットを読める（versioned decode arm を実装済みである）ことが前提**
  （`src/store/wal.rs` の MAINTAINER WARNING を参照）。graceful shutdown は
  WAL リプレイ量を最小化するが、ローテーションが作る**ヘッダのみの
  アクティブセグメントは必ず残る**（チェックポイントは封印済みセグメント
  しか削除しない）うえ、シャットダウン中の in-flight リクエストが最終
  チェックポイント後にレコードを追記し得るため、「WAL が空になるから
  旧フォーマット読み取りは不要」という運用は成立しない
- btrfs / ZFS などチェックサム付き FS 上では、FS 層の corruption 検出が
  I/O エラーとして現れ、アプリ層の CRC 検出（torn-tail 判定）より先に
  fail-stop を誘発することがある。FS 選定時に error / corruption の扱い
  分けを確認すること

> **証明範囲の注意**: majority certificate は「過半数の Authority が当該チェックポイントまで frontier を進めたこと」を暗号学的に証明する。frontier 報告の `digest_hash` は eventual store 全体の M-7 root digest（scheme v2、`sd2:<hex64>` 形式。warm-up 未完了 tick は `sd2:cold`、eventual store 未接続は `sd2:unavailable` のセンチネル）であり、report 署名がこの内容主張を反証不能に束縛する——同一 Authority が同一 checkpoint HLC に異なるストア内容を主張すれば equivocation 証拠になる（M-12）。ただし **majority certificate 自体は digest を集約しない**。これは意図的な設計である: eventual モードでは正当な複製遅延により Authority ごとにストア内容（= digest）が異なるのが常態であり、「共通 digest 値の過半数合意」は意味論的に成立しない（延期ではなく原理的却下）。また frontier は certified レーンの主張だが digest は eventual store の内容を束縛する——検知は同一 Authority の自己主張同士の比較なので、このレーン差は健全性に影響しない。frontier 報告全体への署名により報告単位の改竄・なりすましは防止される。

### Frontier 追跡と attestation pool の資源上限（M-4）

frontier 追跡（`AckFrontierSet`——scope ごとに 1 エントリで固有の上限を持たず、スナップショットにも載る）と、署名検証済み attestation を保持する pool（証明書組み立ての材料置き場、非永続）は、登録済み Authority 1 台が `policy_version` / `key_range` を回転させるだけでメモリを増やせないよう、二層で防御される:

- **入口検証（一次防衛）**: frontier 報告は「定義済み range と完全一致 ∧ 報告者がその range の authority set メンバー ∧ placement policy が存在 ∧ `policy_version` が現行版の `-2..=+1` ウィンドウ内」の場合のみ受理される。拒否された報告は **frontier 追跡にも attestation pool にも一切入らず**、`attestation_rejected_unknown_range_total` / `attestation_rejected_version_window_total` に計上される。拒否される scope は certification（`resolve_scope`）が要求するのと同じ「定義＋policy」条件を満たさないため、いかなる write の証明にも使われ得ない——正当損失ゼロ。ウィンドウ幅は frontier GC の保持幅（`frontier_gc_max_retained_versions` 既定 2）＋先行 1 版に一致し、ラグ中／先行中の正当な報告者を落とさない。なお placement policy を持たない range（自動シードされる catch-all `""` 定義など）は報告側（`FrontierReporter`）がそもそも報告対象にしない——受信側全ノードで admission 拒否になるだけの報告を毎 tick 生成して、警告ログと flood 信号カウンタを恒常汚染しないため。policy を後から作成すれば、次の policy 変更検知で自動的に報告対象へ昇格する。
- **pool 内ハード上限（バックストップ）**: グローバル 1024 scope・Authority あたり 64 scope（いずれも `equivocation` 検知器の上限と同値。スケール前提を変える場合は両方を同時に引き上げる）。上限到達時は**新規 scope の作成を拒否**する（既存 scope への参加は常に受理——組み立て進行中の定足数を絶対に妨げない。resident の追い出しも行わない——過去 checkpoint の attestation は再取得不能なため）。拒否時は 1 秒間隔にスロットルされた stale scope 掃除（現行版ウィンドウ外の scope を一括除去）が走り 1 回だけ再試行する。
- 正常運用では拒否カウンタは全てゼロが期待値。`attestation_rejected_scope_cap_total` / `attestation_rejected_authority_cap_total` の増加は「同時有効 scope 数が運用規模の想定（range 数 × 4 版）を超えた」ことを意味し、定数（`MAX_POOL_SCOPES` / `MAX_POOL_SCOPES_PER_AUTHORITY`）の引き上げ検討シグナルとなる。

### Equivocation / split-view 検知

同一 Authority が矛盾する frontier 報告を（同一ピアまたは別ピアへ）署名付きで送った場合、受信ノードはこれをローカルで検知し、否認不能な証拠を保存する。

- **判定条件**: 同一 `(authority_id, key_range, policy_version, frontier_hlc)` に対して `digest_hash` が異なる、**署名検証済み** attestation のペア。report 署名は frontier の全フィールド（digest 含む）を束縛するため、このペアは 2 つの相異なる署名済みメッセージそのものであり、第三者がレジストリ鍵で再検証できる **proof of misbehaviour（POM）** になる。同一チェックポイント内での frontier_hlc の前進や、鍵ローテーション中の同一 digest 再署名は定義上検知対象外。無署名報告・検証に失敗した報告は証拠にならない。
- **誤検知ゼロの根拠（M-12 以降）**: digest が実ストア内容（tick 間で正当に変化する値）になったため、誤検知ゼロは「digest が HLC の決定的関数であること」ではなく次の機構で保証される: (1) digest は **tick ごとに一度だけ**計算され、report 署名で凍結される（全 scope・全ピア・自己観測に同一バイト）。(2) report HLC はプロセス内で厳密単調。(3) 再起動を跨ぐ単調性は **ReportClockFloor**（`<data_dir>/frontier_report_clock.json`、10 秒幅リースの write-ahead fsync。fsync 失敗 tick は報告ごとスキップ = `frontier_report_skipped_floor_total`）が保証し、起動時にリース値から clock を seed する。(4) floor ファイルが無い起動（初回起動・floor 喪失）では **activation grace（180 秒 = 観測ヘッド保持 120 秒 + skew 前提 60 秒）** の間 **frontier 報告そのものを完全停止**する（形式を問わず何も署名しない）。前世代の報告形式は floor 喪失後には判別できず、プレースホルダを出すと「旧世代の `sd2:` ヘッドをピアが保持したまま、壁時計逆行（60 秒以内でも）で同一 HLC を再発行」した場合に placeholder-vs-sd2 の偽証拠が成立し得るため、無署名だけが両形式方向に安全。grace 中は floor ファイルも作成されない（grace 中にクラッシュしても次回起動は grace を最初からやり直す——floor ファイルの存在は常に「リースが全署名済み report を覆う」ことの証拠であり続ける）。grace 明けに `sd2:` 形式で報告を再開する。**grace の安全根拠（M-14 で改訂）**: 観測ヘッドは中継（frontier gossip + sync 相乗り）により hop ごとに保持期限が更新され、クラスタ全体では 120 秒を超えて生存し得るため、「grace 明けにはピア上の旧世代ヘッドが全て期限切れ」という旧論証には**依存しない**。安全性はヘッド寿命と無関係なクロック算術で成立する: ピアが索引し得る旧世代ヘッドの HLC は クラッシュ時壁時計 + 60 秒（受信側 HLC 受理境界と検知器の future-skew ガード）を超えず、壁時計逆行が skew 予算（60 秒）以内なら grace（180 秒）明けの初回報告 HLC は クラッシュ時壁時計 + 120 秒 以上——旧世代ヘッドを厳密に上回る。不変量 **grace > 2 × MAX_CLOCK_SKEW_MS（180 秒 > 120 秒、マージン 60 秒）** はテストで固定されている（`digest_activation_grace_covers_clock_swing_budget`）——**厳密不等号**であることが本質で、両側の境界は包含的（受理は「境界超え」のみ拒否）なため、等号（grace = 2 × skew）では旧世代ヘッドと同一 physical（= 同一 HLC になり得る）の報告が排除できない。retention 120 秒は grace 値の構成要素として残るが、いまや検知窓の定義であって安全性の前提ではない。コスト: floor 喪失・初回起動時のみ、当該 authority の frontier 報告が約 3 分停止する。ノード間のストア内容差（複製遅延・GC タイミング差・anti-entropy 途中）は **authority_id が異なるため構造上比較されない**——split-view 検知は「同一 Authority が同一 checkpoint に異なる内容を主張すること」の検出であり、ノード間差異の検出ではない。
- **運用前提（M-12）**: (a) authority ノードは floor の永続化のため書き込み可能な data dir が必須（floor path 未構成なら `sd2:` 形式は決して有効化されず、プレースホルダのまま fail-safe）。(b) クロックの逆行はクラスタ前提の skew 上限 60 秒（`MAX_CLOCK_SKEW_MS`）以内であること——activation grace の幅はピア側壁時計がこの前提内であることに依存する。(c) **`frontier_report_clock.json` をバックアップから復元しないこと**。復元された floor のリースはバックアップ時点の値であり、バックアップ以降クラッシュまでに署名した report を覆わない（= 単調性証拠として偽）。しかも staleness はローカルで判別不能なため、ノードはそれを信頼して即時 `sd2:` 署名を再開してしまう。data dir をバックアップから復元する場合はこのファイルを**削除してから**起動する（削除すれば grace が適用され安全に劣化する。下の「データ復旧」表も参照）。切替・ロールバックは `ASTEROIDB_FRONTIER_STORE_DIGEST=0` + 再起動（floor により再起動後 HLC は常に新規なので、新旧形式が同一ヘッドで衝突することはない）。ローリングアップグレードに順序制約はなく（`sd2:` 文字列は旧ノードにも不透明な digest として検証・比較・中継される）、旧バイナリへのダウングレードも安全（floor ファイルは旧バイナリには読まれない無害な JSON として残るだけ）。
- **split-view 検知（CT-gossip Protocol 2 型）**: 各ノードは自身が観測した署名付き attestation の要約を **2 本のレーン**で交換する。(1) 既存の frontier push への相乗り（`observed` レーン、authority 発。旧ノードは無視して decode 可能）。(2) **全ノード共通の anti-entropy 同期リクエスト（delta / digest）への相乗り（M-14）**——frontier push レーンだけでは、悪意 Authority が**非 authority ノードだけ**を狙って矛盾ヘッドを配ると（非 authority は frontier reporter を持たないため観測を送出せず）矛盾が出会わなかった。sync レーンは全ノードが周期実行し、中継されたヘッドは受信側の索引に入って次ホップのサンプルに再流入するため、**伝播は多段（推移的）**で分断治癒後にも再拡散する。帯域・CPU は有界: 送信は 1 サイクル・ピアごとに最大 1 リクエストへの添付（サンプル上限 64 件 ≈ 最大 ~80KB）で、**未変化サンプルは送出抑止（定常時 0 バイト）**——ただし配達済み記録は観測ヘッド保持窓（120 秒）で失効し、同一サンプルでも 1 窓に 1 回は再送される（受信側の観測索引はメモリのみで再起動・age-out で消えるため、抑止を無期限にすると中継経路が恒久的に沈黙し得る。再送は受信側で検証前に dedupe され CPU 追加コストなし）。受信は従来どおりリクエストあたり 64 件で打ち切り・既知エコーは署名検証前に dedupe。悪意 Authority がピアごとに異なる digest を報告しても、gossip 交換後に受信側が矛盾を検知する。検知済み証拠のペアは両レーンで全ピアへ能動的に伝播する。**混在期の注意（ローリングアップグレード）**: 旧ノード → 新ノード方向の delta/digest リクエストは bincode の位置依存 decode に失敗して 400 になり、送信側の既存 JSON 再送で成功する（M-8 の `compaction_floor` 追加と同一機構）。アップグレード期間中、internal sync 経路の 4xx とペイロード再送（実質倍増）は**想定内**であり、4xx ベースのアラートの誤報要因になる。新 → 旧方向は旧ノードが末尾の `observed` を無視して 200 を返す（observed が運ばれないだけで同期本体は無劣化）。旧ノードのみを経由する経路では中継が塞がるが、アップグレード完了で解消する（旧ノードは末尾バイトを黙って捨てつつ 200 を返すため送信側は「配達済み」と記録するが、その記録も 120 秒で失効するので、アップグレード後 1 窓以内に再配達される）。**ビルド構成混在（native-crypto / stub）の注意**: stub ビルドは BLS フィールドを検証せずに索引し得るため、中継サンプル生成時（`gossip_summaries`）に BLS レーンを剥離して送出する——native 受信側は BLS フィールドをデシリアライズ時に厳格検証するため、不正な BLS 文字列を 1 件でも中継すると carrier リクエスト全体がデコード不能になる。証拠の採否は全ビルドで Ed25519 レーンのみで決まるため、剥離による検知能力の低下はない（native ビルド同士は検証済み BLS レーンをそのまま中継する）。sync レーン経由の中継量は `observed_relay_sync_requests_total` / `observed_relay_sync_accepted_total` で観測できる。
- **検知時の動作**: `EQUIVOCATION DETECTED` の警告ログ（構造化フィールド付き）、メトリクス計上（`equivocation_detected_total` ほか）、証拠のデータディレクトリへの永続化（`equivocation_evidence.json`、再起動後も保持）。**自動隔離は行わない**——Authority の排除には合意が必要であり、検知レイヤに強制を混ぜない設計（除外の opt-in は `ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES` を参照）。証拠は `GET /api/authority/equivocations` で取得できる。対応手順は `docs/runbook/troubleshooting.md` の「Authority Equivocation Detected」を参照。
- **検知可能になったもの（M-12）**: 同一 Authority が同一 `(key_range, policy_version, frontier_hlc)` に**異なるストア内容**を主張する data-content split view。root digest は全キーの正準 CRDT ストリーム digest（D(k)）を集約するため、任意の 1 キーの差異が root を変える（SHA-256 衝突を除く）。比較は文字列等価なので**旧ノードの検知器も新形式の split view をそのまま検知できる**（ロールアウト即時に全観測者へ波及）。同一署名鍵を共有する重複プロセス（誤構成・鍵漏洩）も、ストア内容が分岐した時点で自己検知される。
- **限界（重要・脅威モデル上の正直な注記）**:
  1. 検知は**事後的・確率的**であり防止ではない（reactive security）。矛盾する報告が一時的に受理・配布される時間窓を許す。
  2. **結託した過半数**が全ノードに一貫して同じ嘘をつく非分岐型の不正は、本機構（および fork*/ハッシュチェーン系一般）では検知できない。対抗手段は Authority 群の独立配置のみ。また digest は自己申告であり、実際にサービングしている内容との照合（PeerReview 型監査）は未実装——「全員に一貫した嘘」は digest が実体化しても不可視。さらに**内容束縛は報告側の正直な実装パスのみが強制**する: 悪意 Authority はプレースホルダ形式（HLC の決定的関数なので split の両側が常にバイト一致）や固定センチネル文字列を署名し続けることで、data-content split-view 検知から**恒久的に離脱**できる。これは検知器では不正として扱われず（digest は意図的に不透明文字列比較）、受信側メトリクス `frontier_nonbinding_digest_total` の恒常増加と `GET /api/internal/frontiers` の目視でのみ観測できる。
  3. 悪意 Authority がピアごとに frontier_hlc を僅かにずらして報告すると、完全一致ベースの検知確率は下がる（その場合も certificate は checkpoint 単位で束縛される）。CT 型 consistency proof は将来課題。
  4. 検知遅延 ≈ frontier 報告周期 + push 伝搬遅延 + クロックスキュー。honest なピアが定期的に push し合うことが前提で、長期分断では検知が遅れる。
  5. 観測索引は scope あたり 128 件（報告周期 1 秒でおよそ 2 分）に prune されるため、それより古い時点との矛盾はローカルでは検知できない（記録済みの証拠自体は消えない）。保持窓は **hop ごと**（各中継先で受信時刻から再計時）であり end-to-end ではないため、M-14 の sync 相乗り中継はこの窓を跨いで多段伝播できる（`sync_interval` が保持窓 120 秒以上に設定されていると中継の実効性が失われる——起動時に WARN を出す）。
  5b. 中継の残余盲点: 恒久分断・全経路支配（eclipse）・keyset registry を持たないノードのみの経路・混在期の旧ノードのみ経路（アップグレード完了で解消）では矛盾ヘッドが出会わない。また digest は不透明文字列比較のため、frontier HLC が完全一致しない一過性 split-view は検知対象外（項目 3 と同じ制約。CT 型 consistency proof は将来課題）。検知は本質的に確率的・遅延 SLA 型である。
  6. `observed` レーンは 1 件あたり Ed25519 検証コストがかかる（リクエストあたり 64 件で打ち切り）。内部 API には `ASTEROIDB_INTERNAL_TOKEN` の設定を推奨する。
  7. digest は per-key HLC・LWW タイムスタンプ等のメタデータを意図的に含まない（`src/store/digest.rs` の設計判断）ため、**メタデータのみの分岐**は不可視。
  8. digest cache が cold の tick（`sd2:cold`、`frontier_digest_cold_total` で観測）と eventual store 未接続（`sd2:unavailable`）は内容を束縛しない。floor 無し起動の activation grace 中（最初の 180 秒）は frontier 報告そのものが停止する（内容束縛どころか報告が無い——安全性のための意図的な空白）。非束縛 tick は受信側でも `frontier_nonbinding_digest_total` で観測できる。
  9. root digest は全ストア粒度であり、証拠は**どの key range が分岐したか**までは示さない（per-scope digest への細分化は followup M-12b）。
  10. compaction checkpoint の digest（`src/compaction/engine.rs` 系統、FR-010）は**別系統で依然プレースホルダのまま**（未解決）。
  11. **鍵配布そのものは検知の対象外（脅威モデル境界）**: この透明性レイヤが覆うのは frontier 報告の一致性のみであり、keyset 履歴に STR（Signed Tree Root）チェーンは無い。**鍵配布・ローテーション自体の split-view**——異なるノードに異なる `ASTEROIDB_AUTHORITY_KEYS` を配る攻撃——は検知できず、keyset の完全性は env 配布経路（構成管理・シークレット配布）の運用信頼に依存する。STR チェーンの導入は次期課題（`SECURITY.md`「既知の制限事項」および `docs/runbook/key-rotation.md` を参照）。

### BLS 鍵配布のローリングアップグレード手順（PoP 導入）

`ASTEROIDB_AUTHORITY_KEYS` の BLS 部は 2 セグメント形式（`<ed>/<bls>`）から PoP 付き 3 セグメント形式（`<ed>/<bls>/<pop>`）へ拡張された。**旧バイナリは 3 セグメント形式を解釈できない**——旧パーサは `<bls>/<pop>` 全体を不正な BLS hex とみなし、**Ed25519 鍵を含むエントリ全体をスキップ**する。env を先に切り替えると、未更新ノードがピアの Ed25519 鍵まで失い、strict モード（`ASTEROIDB_REQUIRE_SIGNED_FRONTIERS=1`）ではピアの署名付き frontier が unknown authority として拒否され certification が停滞する。したがって以下の順序を厳守すること:

1. **全ノードのバイナリを新版へ更新**する（env は旧 2 セグメント形式のままでよい。新版は lenient で BLS 部を破棄し Ed25519-only に degrade して動作する）。
2. 各ノードの起動ログから **PoP 付き配布用エントリ**（`Authority key entry for ASTEROIDB_AUTHORITY_KEYS distribution: ...`）を収集する。
3. 全ノードの `ASTEROIDB_AUTHORITY_KEYS` を **3 セグメント形式へ一斉に更新して再起動**する。
4. 必要なら `ASTEROIDB_REQUIRE_SIGNED_FRONTIERS=1` を有効化する。

strict モードで運用中のクラスタは、手順 (1) の前に一時的に strict を外すか、手順 (3) 完了まで strict 化を遅らせること（strict のまま旧 2 セグメント形式でアップグレードすると、新版が PoP 欠落を検出して起動エラーになる）。

---

## 3. 監視・アラート設定

### 3.1 メトリクスエンドポイント

AsteroidDB は JSON 形式でメトリクスを提供します。

```bash
# メトリクス取得
curl -s http://localhost:3000/api/metrics | jq .

# ヘルスチェック
curl -s http://localhost:3000/healthz
# => {"status":"ok"}

# SLO ステータス
curl -s http://localhost:3000/api/slo | jq .
```

### 3.2 利用可能なメトリクス一覧

`GET /api/metrics` が返す `MetricsSnapshot` のフィールド:

| メトリクス | 型 | 説明 |
|-----------|------|------|
| `pending_count` | u64 | 現在の保留中 Certified Write 数 |
| `certified_total` | u64 | Certified Write 累計 |
| `certified_sync_attempt_total` | u64 | certified 複製レーンの pull リクエスト数（1 ドレインラウンドにつき 1。1 サイクル 1 peer あたり最大 8） |
| `certified_sync_success_total` | u64 | 適用まで完了した pull |
| `certified_sync_failed_total` | u64 | ネットワーク/デコード失敗、または適用中のストレージエラー |
| `certified_sync_unsupported_total` | u64 | レーン以前のノードからの 404/405。ローリングアップグレード**中**は正常。完了後も増え続けるなら未アップグレードノードがあり、そのノードの certified write は複製されていない |
| `certified_sync_entries_applied_total` | u64 | ローカル certified ストアへ取り込んだエントリ数 |
| `certified_sync_entries_skipped_total` | u64 | RR ゲートで落ちた冗長エントリ。収束済みレーンの定常値であり、増加は仕事ではない |
| `certified_sync_origin_unknown_total` | u64 | origin バージョンを持たずに届いたエントリ（origin 永続化以前のキー）。受信側の現行バージョンにフォールバックする。全キーが書き直されれば頭打ちになる |
| `certified_sync_origin_ahead_rejected_total` | u64 | origin が自ノードの現行バージョンより先だったため拒否。自ノードの namespace が書き手より遅れている。**一過性であり、レーンは baseline を進めずに再試行する**（下の `baseline_held` を参照）。継続的な増加は namespace の追随が止まっていることを意味する |
| `certified_sync_type_mismatch_total` | u64 | ローカル値との CRDT 型衝突で拒否。非ゼロは実際の乖離なので要調査（恒久的な乖離なので、レーンは停止せず次へ進む） |
| `certified_sync_policy_denied_total` | u64 | 受信側に当該キーの authority 定義がまだ無く拒否。ポリシー変更の伝播中は正常で一過性。**一過性なのでレーンは baseline を進めずに再試行する** |
| `certified_sync_baseline_held_total` | u64 | 一過性の拒否（origin ahead / policy denied）により peer baseline を**進めずに保留**したラウンド数。定常的に 0 でなければ、その peer との間で namespace が揃っていない |
| `certified_sync_forced_skip_total` | u64 | 保留を 12 ラウンド続けても解消せず、やむなく**そのエントリを飛ばした**回数。非ゼロは要調査（prefix に authority 定義が無い等の運用レベルの原因）。飛ばしたエントリは下記の定期リスキャンでのみ回収される |
| `certified_sync_rescan_total` | u64 | peer の certified ストアを `baseline=0` から読み直した回数（30 分ごと）。certified プレーン唯一の修復パス |
| `certified_origins_len` | u64 | ゲージ: 保持している per-key origin ピン数。certified キー数に比例して増える。RSS と併せて監視する |
| `certification_latency_mean_us` | f64 | 証明レイテンシ平均 (us) |
| `frontier_skew_ms` | u64 | Authority frontier 最大スキュー (ms) |
| `sync_failure_rate` | f64 | 同期失敗率 (0.0-1.0) |
| `sync_attempt_total` | u64 | 同期試行累計 |
| `sync_failure_total` | u64 | 同期失敗累計 |
| `delta_sync_count` | u64 | デルタ同期累計 |
| `full_sync_fallback_count` | u64 | フルシンクフォールバック累計（実際に全キー push が走った回数） |
| `full_sync_fallback_ratio` | f64 | フルシンク比率 (0.0-1.0) |
| `digest_sync_attempt_total` | u64 | digest 同期 pull 試行累計 |
| `digest_sync_root_match_total` | u64 | ルート digest 一致（転送ゼロ完了）累計 |
| `digest_sync_partial_total` | u64 | 不一致バケットのみの部分転送累計 |
| `digest_sync_unsupported_total` | u64 | digest 非対応ピア（404 / スキーム不一致）による従来フルシンク移行累計 |
| `digest_sync_failed_total` | u64 | digest 交換のネットワーク/デコード失敗累計 |
| `digest_sync_keys_transferred_total` | u64 | digest pull で実際に転送したキー数累計 |
| `digest_sync_keys_skipped_total` | u64 | digest 同期で転送を回避したキー数累計（フルダンプ比の帯域削減量の推定） |
| `digest_push_probe_total` | u64 | push 側 digest probe 試行累計 |
| `digest_push_match_total` | u64 | probe 一致によるフル push スキップ累計 |
| `digest_push_keys_pushed_total` | u64 | digest subset push で送ったキー数累計 |
| `gc_floor_stalled_hole_dots` | u64 (gauge) | 直近に**実行された** GC sweep で hole に停止した floor 走査数。mark のみ・ゲート不合格の tick では上書きされず、次の sweep 実行まで値を保持する。恒常的に非ゼロなら peer の全状態同期と tombstone の再配送を調べる（3.7） |
| `gc_floor_stalled_uncandidated_dots` | u64 (gauge) | 直近に実行された GC sweep で mark 後 tombstone に停止した floor 走査数（一時的。次サイクルで解消。非 sweep tick では保持） |
| `gc_floor_rejected_dots_total` | u64 | compaction floor に覆われた受信 tombstone の不採用累計。ローリングアップグレード混在期の v1 再注入圧の観測 = 全ノード更新完了の判定材料 |
| `gc_floor_killed_by_floor_total` | u64 | floor により抑止された stale live dot 累計（未知レプリカ・遅延 push 由来の削除済み dot の棄却/殺） |
| `gc_last_sweep_wall_ms` | u64 (gauge) | 実際に sweep した最後の GC パスのローカル壁時計 ms。**0 = 起動以来一度も sweep していない**。tombstone GC が生きているかの最上位シグナル（3.7.1）。「進まないこと」に対してアラートする |
| `gc_gate_blocked_authority_total` | u64 | GC 二重ゲートの authority 側で不合格になった tick 累計。lagging cluster では正常に非ゼロなので、単体ではアラートにしない（`gc_last_sweep_wall_ms` の停止原因の切り分けに使う） |
| `gc_gate_blocked_peer_total` | u64 | 同 peer 側で不合格になった tick 累計。registry に残った dead peer が典型 |
| `gc_gate_peer_population` | u64 (gauge) | peer ゲートが最後に評価した登録ピア数。単一ノードでは 0 が正常。**多ノード構成で 0 かつ `gc_last_sweep_wall_ms` が進んでいる場合、peer ゲートが空真のまま sweep している**（3.7.1） |
| `compaction_unattributed_write_ops_total` | u64 | どの certifiable レンジ（authority 定義と placement policy の両方を持つ prefix）にも一致せず、compaction の ops カウンタに計上されなかった書き込み ops 累計。増え続ける場合は policy 未設定のキー空間に書き込みが集中している |
| `sync_redundant_merge_skips_total` | u64 | RR ゲート（M-6）が抑止した冗長リモート merge 累計（no-op merge かつ delta 可視済みキー → 再スタンプ/変更ログ/WAL 書き込みをスキップ）。GC tick ごとに `EventualApi` の in-memory カウンタから反映（再起動でリセット）。アイドルクラスタで緩やかに増えるのは正常（収束済みキーのエコー吸収）。収束済みストアの双方向 sync 下で 0 に張り付く場合はピンポン回帰を疑う |
| `write_ops_total` | u64 | 書き込み操作累計 |
| `rebalance_start_total` | u64 | リバランス開始累計 |
| `rebalance_keys_migrated` | u64 | リバランス移行キー累計 |
| `rebalance_keys_failed` | u64 | リバランス失敗キー累計 |
| `rebalance_complete_total` | u64 | リバランス完了累計 |
| `rebalance_duration_sum_us` | u64 | リバランス所要時間合計 (us) |
| `key_rotation_total` | u64 | 鍵ローテーション累計。**自動ローテーション未配線のため現状は常に 0**（`docs/runbook/key-rotation.md` 参照） |
| `key_rotation_last_version` | u64 | 最新 keyset バージョン。**現状は初期値のまま変化しない** |
| `key_rotation_last_time_ms` | u64 | 最新ローテーション時刻 (ms)。**現状は常に 0** |
| `equivocation_detected_total` | u64 | equivocation 証拠ペア検知累計 |
| `equivocation_last_detected_ms` | u64 | 最終 equivocation 検知時刻 (ms、0=なし) |
| `equivocation_accused_authorities` | u64 | 証拠が存在する Authority 数（ゲージ） |
| `split_view_observations_total` | u64 | gossip 経由で検証・照合した観測数累計（frontier push・sync 相乗りの両レーン共通、M-14 以前からの連続性維持） |
| `observed_relay_sync_requests_total` | u64 | 非空の observed レーンを載せて処理された delta/digest sync リクエスト数（M-14。registry 無しノードは全捨てのため計上しない） |
| `observed_relay_sync_accepted_total` | u64 | sync 相乗りレーン経由で署名検証を通過し検知器へ投入された観測数（M-14。既知エコー・range 非メンバーは含まない） |
| `attestation_pool_scopes` | u64 | attestation pool が追跡中の scope 数（ゲージ。正常時は「定義 range 数 × 高々 4 バージョン」以下） |
| `attestation_rejected_unknown_range_total` | u64 | 入口検証での frontier 報告拒否累計（frontier 追跡・pool の両方に不採用）：未定義 range・authority set 非メンバー報告者・placement policy なし。**正常時ゼロ**、増加は flood または設定不整合の一次信号 |
| `attestation_rejected_version_window_total` | u64 | 入口検証での frontier 報告拒否累計（frontier 追跡・pool の両方に不採用）：policy_version が現行版の `-2..=+1` ウィンドウ外。**正常時ゼロ** |
| `attestation_stale_version_total` | u64 | 現行版より**古い** policy_version を報告してきた frontier の受理累計（M-17。ウィンドウ内なので受理はされるが、報告者の namespace が制御プレーンに追随できていない一次信号——凍結 observer や分断少数派 voter は **policy bump 1 回目からここに現れる**）。**正常時ゼロ**、継続増加はアラート推奨（§14.8） |
| `attestation_rejected_fenced_total` | u64 | fence 済み `(range, version)` scope への frontier 報告の破棄累計（M-17。frontier は前進せず attestation も pool されない = 当該 authority はその scope の certification に寄与していない確証）。**正常時ゼロ**。M-17 以前この破棄は完全無音だった |
| `attestation_rejected_scope_cap_total` | u64 | pool のグローバル scope 上限（1024）による insert 拒否累計。**正常時ゼロ** |
| `attestation_rejected_authority_cap_total` | u64 | pool の per-authority scope 上限（64）による insert 拒否累計。**正常時ゼロ** |
| `attestation_purged_total` | u64 | 告発された Authority の pool 済み attestation の purge 除去累計（`ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES=1` 時のみ増加し得る） |
| `frontier_digest_cold_total` | u64 | digest cache が cold のまま `sd2:cold` センチネルで報告した frontier tick 累計。再起動・大量書き込み直後の散発は正常、恒常増加は warm-up 不収束（当該 tick は内容を束縛しない） |
| `frontier_report_skipped_floor_total` | u64 | ReportClockFloor の fsync 失敗により**報告ごとスキップ**した frontier tick 累計。正常時ゼロ。増加は data dir 書き込み不能を意味し、この Authority は報告を停止している（certification 可用性への影響大、即調査） |
| `frontier_nonbinding_digest_total` | u64 | 受信・受理したリモート Authority の frontier 報告のうち、`digest_hash` が内容を束縛しない形式（プレースホルダ / `sd2:cold` / `sd2:unavailable` / 不明形式）だった件数の累計。ローリングアップグレード中や cold cache 窓では一時的な増加が正常。**特定 Authority に帰属する恒常増加**（debug ログ `accepted frontier report binds no store content` で帰属確認）は、その Authority が内容束縛から離脱している唯一のシグナル（限界 2 を参照） |
| `peer_sync` | map | ピアごとの同期統計（60 秒スライディングウィンドウ） |
| `certification_latency_window` | object | 証明レイテンシウィンドウ統計（60 秒） |

`peer_sync` の各ピアエントリ:

| フィールド | 説明 |
|-----------|------|
| `mean_latency_us` | ウィンドウ内平均同期レイテンシ (us) |
| `p99_latency_us` | ウィンドウ内 P99 同期レイテンシ (us) |
| `success_count` | 累計成功数 |
| `failure_count` | 累計失敗数 |

`certification_latency_window`:

| フィールド | 説明 |
|-----------|------|
| `sample_count` | ウィンドウ内サンプル数 |
| `mean_us` | ウィンドウ内平均レイテンシ (us) |
| `p99_us` | ウィンドウ内 P99 レイテンシ (us) |

### 3.3 Prometheus 連携

AsteroidDB はネイティブの Prometheus エクスポーターを持ちませんが、JSON メトリクスを Prometheus 形式に変換できます。

**方法 1: json_exporter を使用**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'asteroidb'
    metrics_path: /probe
    params:
      module: [asteroidb]
    static_configs:
      - targets:
          - http://node-1:3000/api/metrics
          - http://node-2:3001/api/metrics
          - http://node-3:3002/api/metrics
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: json-exporter:7979
```

**方法 2: スクリプトで変換**

```bash
#!/bin/bash
# asteroidb-metrics-exporter.sh
# cron で定期実行し、Prometheus textfile collector で収集
NODES="node-1:3000 node-2:3001 node-3:3002"
OUTPUT="/var/lib/prometheus/node-exporter/asteroidb.prom"

for node in $NODES; do
  METRICS=$(curl -s "http://${node}/api/metrics")
  NODE_ID=$(echo "$node" | cut -d: -f1)

  echo "asteroidb_pending_count{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.pending_count')"
  echo "asteroidb_certified_total{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.certified_total')"
  echo "asteroidb_sync_failure_rate{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.sync_failure_rate')"
  echo "asteroidb_frontier_skew_ms{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.frontier_skew_ms')"
  echo "asteroidb_write_ops_total{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.write_ops_total')"
  echo "asteroidb_full_sync_fallback_ratio{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.full_sync_fallback_ratio')"
  echo "asteroidb_certification_latency_p99_us{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.certification_latency_window.p99_us')"
  echo "asteroidb_equivocation_detected_total{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.equivocation_detected_total')"
  echo "asteroidb_equivocation_accused_authorities{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.equivocation_accused_authorities')"
  echo "asteroidb_split_view_observations_total{node=\"${NODE_ID}\"} $(echo $METRICS | jq '.split_view_observations_total')"
done > "$OUTPUT"
```

### 3.4 Grafana ダッシュボード推奨パネル

| パネル | クエリ対象 | 閾値 |
|--------|-----------|------|
| Certification Latency P99 | `certification_latency_window.p99_us` | Warning: 300ms, Critical: 500ms |
| Sync Failure Rate | `sync_failure_rate` | Warning: 5%, Critical: 10% |
| Frontier Skew | `frontier_skew_ms` | Warning: 5000ms, Critical: 10000ms |
| Pending Writes | `pending_count` | Warning: 100, Critical: 1000 |
| Full Sync Fallback Ratio | `full_sync_fallback_ratio` | Warning: 30%, Critical: 50% |
| Write Throughput | `write_ops_total` (rate) | 情報表示のみ |
| Rebalance Progress | `rebalance_keys_migrated` vs `rebalance_keys_failed` | Failed > 0 で Warning |
| Authority Equivocation | `equivocation_detected_total` | **> 0 で即時対応（P1）**——runbook「Authority Equivocation Detected」参照 |

### 3.5 アラートルール例

```yaml
# alertmanager rules
groups:
  - name: asteroidb
    rules:
      - alert: HighSyncFailureRate
        expr: asteroidb_sync_failure_rate > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Sync failure rate exceeds 10%"

      - alert: CriticalSyncFailureRate
        expr: asteroidb_sync_failure_rate > 0.3
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Sync failure rate exceeds 30%"

      - alert: HighFrontierSkew
        expr: asteroidb_frontier_skew_ms > 10000
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "Frontier skew exceeds 10 seconds"

      - alert: HighCertificationLatency
        expr: asteroidb_certification_latency_p99_us > 500000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Certification P99 latency exceeds 500ms"

      - alert: PendingWriteBacklog
        expr: asteroidb_pending_count > 1000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Large pending write backlog"

      - alert: HighFullSyncRatio
        expr: asteroidb_full_sync_fallback_ratio > 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Over 50% of syncs are full-sync fallbacks"
```

### 3.6 digest 段階 diff 同期の運用

フルシンク相当の状況（高変更率・prune 済み change log・デコード失敗・連続
ネットワーク障害・長期分断後の再接続）では、全キーダンプの前にキー範囲
digest を比較し、不一致バケットのみを転送します（設計は
`docs/architecture.md` の「Anti-Entropy: 3 段構えの同期と digest 段階 diff」
を参照）。

**メトリクスの読み方**:

- `digest_sync_root_match_total` の増加 = 分断後再接続やアイドル時の
  フォールバックが**転送ゼロ**で完了している（帯域節約が効いている）。
- `digest_sync_keys_skipped_total` / (`digest_sync_keys_skipped_total` +
  `digest_sync_keys_transferred_total`) がフルダンプ比の削減率の推定値。
- `digest_sync_unsupported_total` が増え続ける = 旧バージョン（digest
  エンドポイント未実装）またはスキームバージョン不一致のノードが残存。
  ローリングアップグレードの完了を確認する。非対応ピアは 10 分 TTL で
  キャッシュされ、TTL 経過後に自動で再 probe される。
- `digest_sync_partial_total`: GC 直後の不一致はバケット転送が compaction
  floor を伝播して **1 往復で自己治癒**する（scheme v2、3.7 を参照）ため、
  一時的な増加は正常。**恒常的な増加が続く場合**は、旧ノード混在
  （`gc_floor_rejected_dots_total` の増加を併確認）、hole 停滞
  （`gc_floor_stalled_hole_dots`）、dead peer を疑う。
  **歴史的注記（scheme v1 クラスタ向け）**: v1 では「GC 非対称の偽不一致は
  帯域のみのコスト」と記載していたが、これは誤りだった。実際にはバケット
  転送のマージが旧ピアの stale tombstone を再注入して GC を巻き戻すため、
  持続的フォールバック下では tombstone GC がクラスタ全体で収束しない
  ライブロックがあった（M-8）。v1 のまま運用する場合の暫定緩和は
  `ASTEROIDB_DIGEST_SYNC_DISABLED=1`（本節冒頭のキルスイッチ）。
- `digest_sync_failed_total` の増加はネットワーク/デコード失敗。従来フル
  シンクで救済されるため正しさは不変だが、リンク品質を確認する。

**コスト特性（M-7 増分 digest キャッシュ導入後）**: digest 交換は発信側・
応答側とも、定常状態（書き込みが少なくクラスタが収束済み）ではストアの
クローンもキー再ハッシュも行わない（O(1)）。再起動直後の数サイクルだけは
キャッシュが cold のため従来同様のスナップショット経路（ロック外ハッシュ）
を通り、sync サイクル冒頭の warm-up が完了すると増分経路に移行する。
digest 値・応答内容はどちらの経路でもビット同一なので、運用上の観測点は
従来どおり上記メトリクスのみでよい。注意点が 1 つ: 書き込みが持続して
いるノードでは push probe と subset push の間の並行書き込みにより
push 証拠（`push_acked_wall_ms`、tombstone GC のピアゲート）が意図的に
据え置かれることがある。これは安全方向（GC が待つだけ）で、書き込みが
静まった後の root 一致 probe で自動的に前進する。tombstone GC の停滞を
調査する際は、まず対象ピアへの書き込みレートを確認すること。

HLC index の `pruned_floor` より古い push baseline から delta scan した場合は、prune 済みのキーを送れていない可能性を peer ごとに記憶する。その delta が成功して baseline が進んでも、次の空 scan だけでは tombstone GC の push 証拠を更新しない。次周期に digest 省略の完全状態 push を試み、全キーにエラーのない応答があった時だけ記憶を消して証拠を更新する。失敗や一部キーのエラーでは次周期も再送する。再起動時には push 証拠と baseline も初期化される。

**切り戻し手順**（digest 同期に起因する問題を疑う場合）:

1. 対象ノードに `ASTEROIDB_DIGEST_SYNC_DISABLED=1` を設定して再起動。
   同期は従来のフルシンクのみのフォールバック動作（digest 導入前と同一）に
   戻る。ノードごとに独立して切り戻せる（受信側エンドポイントは残るが、
   digest を無効化したノードは probe を送らなくなるだけで無害）。
2. `digest_sync_attempt_total` と `digest_push_probe_total` が増加しなく
   なったことを確認。
3. 問題が解消しない場合、digest 同期は原因ではない。

**版の切り替え**: AsteroidDB は実験段階で、異なる版のノードの混在運用や
旧 snapshot / WAL の読み込みを保証しない。永続形式を変える版への切り替えでは、
対象版の形式で新しいデータ領域を用意して同じ版のノードを起動する。
保存済みデータを引き継ぐ必要がある場合は、その版に対応した明示的な移行手順を
先に用意する。現行コードは旧形式を暗黙に復元しない。

### 3.7 tombstone GC（compaction floor）と Stage 2 hole-jump

トゥームストーン GC は certified sweep（mark → retention → 二重ゲート）
通過後、tombstone を per-(key, writer) の **compaction floor** に畳み込んで
物理削除する。floor はマージで pointwise max 継承され（撤回不能）、stale
tombstone / stale live dot をマージ時に棄却するため、GC はクラスタ全体で
単調に収束する（v1 のライブロックの修正、M-8）。

- 通常運用（Stage 1）で必要な操作はない。`gc_floor_stalled_uncandidated_dots`
  は mark 後に増えた tombstone による一時停滞で、次サイクルで解消する。
- `gc_floor_stalled_hole_dots` が **soak 後も恒常的に非ゼロ**の場合、旧方式
  sweep が痕跡なく削除した dot（legacy hole）で floor が停止している。
  クラスタのどこかに tombstone が残っていれば anti-entropy が hole を埋めて
  自己治癒するため、まず数 GC サイクル分待つこと。
- Stage 2 hole-jump は停止中。mark 後に peer が ping 失敗で registry から
  除外されると、現在の registry に対する inbound ゲートが空集合として通る。
  除外された peer が hole に対応する live dot を保持して再参加すれば、進んだ
  floor がその値を誤って消す。`ASTEROIDB_GC_HOLE_JUMP=1` は起動時に拒否し、
  直接構築した `NodeRunner` でも hole-jump を実行しない。再開には、mark 時点と
  それ以前に存在した replica の保管責任を永続的に追跡し、除外・再参加を含む
  inbound 証拠の健全性を確認する仕組みが必要。
- Authority の frontier が全員分揃っていても、現行名簿の過半数が同じ
  checkpoint に署名し、受理済み full frontier と一致するまで sweep は進まない。
  `ASTEROIDB_REQUIRE_SIGNED_FRONTIERS=false` は未署名報告の受信を許すだけで、
  未署名報告を GC の認証済み floor として扱わない。
- `gc_interval` の短縮は hole 停滞・ゲート不合格には**効かない**（12.3 も
  参照）。停滞の原因はゲート（dead peer / 分断 / hole）であり周期ではない。

#### 3.7.1 「GC が一度も動いていない」の診断（D1 回帰防止）

最上位の生存シグナルは **`gc_last_sweep_wall_ms`**（実際に sweep した最後の
パスのローカル壁時計 ms、**0 = 起動以来一度も sweep していない**）。他の
`gc_floor_*` ゲージは sweep したパスしか書かないため、これが無い状態では
「ゲートが恒久的に閉じているノード」と「健全で回収対象が無いノード」の
メトリクス出力が完全に同一だった。アラートはこのゲージが**進まないこと**に
対して張り、原因の切り分けは以下の順で行う。

1. `gc_last_sweep_wall_ms` が 0 のまま、または retention の数倍を超えて
   進まない → GC は実質停止している。
2. `gc_gate_blocked_authority_total` / `gc_gate_blocked_peer_total` の
   どちらが増えているかで、閉じているゲートの半分を特定する。
3. 該当ノードのログで `tombstone GC gate blocked` の WARN（10 分に 1 行に
   スロットル）を見る。`reason` が原因種別、`prefix` / `peer` が対象、
   `reported` / `required` が authority 側の充足数、`peer_lane` が
   ピアレーンの状態を示す。
   - `authority_under_reported`: その prefix の authority 全員分の frontier
     報告が揃っていない（分断・停止ノード）。
   - `frontier_behind_mark`: 報告は揃ったがデータ時刻が mark 未満。
   - `report_not_advanced`: mark 以降にローカルで前進した報告が無い
     （永続化から復元しただけの frontier を含む。fail-closed）。
   - `signed_quorum_missing`: 現行 Authority 名簿の署名付き checkpoint が
     mark を覆わない、または署名対象の full frontier が受理済み報告と一致しない。
   - `peer_evidence_missing_or_stale`: レジストリに残った dead peer が
     典型。9.2 のノード削除手順でレジストリから外す。

**注意（無防備な sweep の検出）**: `gc_gate_peer_population` は peer ゲートが
最後に評価したときの登録ピア数。単一ノード構成では 0 が正常だが、**多ノード
構成でこれが 0 かつ `gc_last_sweep_wall_ms` が進んでいる場合、peer ゲートが
空真のまま sweep している**（＝復活防止が効いていない）。sync レイヤの設定と
ピアレジストリを確認すること。

**`""`（catch-all）prefix に placement policy を作らないこと。** GC/compaction
の母集合は「authority 定義 **と** placement policy の両方がある prefix」で、
`""` に policy を置くとストア全体が単一レンジとして compaction 対象になり、
`prune_timestamps_before("")` がストア全域に及ぶ。catch-all の authority 定義
自体は（policy が無い限り）無害で、メンバーシップ判定にのみ寄与する。

---

## 4. SLO メトリクスの解釈とアラート基準

### 4.1 事前定義 SLO

AsteroidDB は以下の 4 つの SLO をデフォルトで追跡します（1 時間ウィンドウ）:

| SLO 名 | 条件 | 目標値 | 目標達成率 |
|--------|------|--------|-----------|
| `eventual_read_p99` | レイテンシ < 50ms | 50.0 | 99% |
| `certified_read_p99` | レイテンシ < 500ms | 500.0 | 99% |
| `replication_convergence` | 収束時間 < 5000ms | 5000.0 | 95% |
| `authority_availability` | 可用性 > 99% | 99.0 | 99.9% |

### 4.2 エラーバジェットの解釈

`GET /api/slo` の応答例:

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
      "violations": 50,
      "budget_remaining": 50.0,
      "is_warning": false,
      "is_critical": false
    }
  }
}
```

**エラーバジェット計算式:**

```
許容エラー率 = 1 - (target_percentage / 100)
実際のエラー率 = violations / total_requests
バジェット消費率 = 実際のエラー率 / 許容エラー率
バジェット残量 = (1 - バジェット消費率) * 100%
```

**例**: 99% SLO（許容エラー率 1%）で 10,000 リクエスト中 50 違反:
- 実エラー率 = 50/10000 = 0.5%
- バジェット消費 = 0.5% / 1% = 50%
- バジェット残量 = 50%

### 4.3 SLO アラート基準

| 状態 | バジェット残量 | アクション |
|------|--------------|-----------|
| 正常 | >= 50% | 監視のみ |
| Warning (`is_warning`) | < 50% | 調査開始、原因特定 |
| Critical (`is_critical`) | < 20% | 即時対応、オンコール通知 |
| 枯渇 | 0% | インシデント宣言 |

**推奨アラート設定:**

```yaml
- alert: SloWarning
  expr: asteroidb_slo_budget_remaining < 50
  for: 5m
  labels:
    severity: warning

- alert: SloCritical
  expr: asteroidb_slo_budget_remaining < 20
  for: 2m
  labels:
    severity: critical
```

---

## 4.5 certified replication レーンの運用

certified プレーンは専用の pull 型 anti-entropy レーン
(`POST /api/internal/certified/delta`) を持つ。これが入る前、certified write は
それを受理した 1 ノードのディスクにしか存在せず、そのノードを失うと値は恒久的に
失われた（クライアントには「どこにも存在しない値に対する検証可能な証明」だけが
残った）。同じキーを eventual で書いていれば失われなかったため、**強整合側が
耐久性で劣る**という倒立が起きていた。

**保証の範囲**: このレーンが提供するのは eventual プレーンと同じ
**anti-entropy 耐久性**であって、同期クォーラム耐久性ではない。ack は依然として
「このノードで durable」を意味し、pull が届く前にそのノードが死ねば値は失われうる。

### 容量への影響

- 各ノードの certified ストアは**クラスタ全体の certified キー集合**になる。
  certified 側のディスク使用量とメモリが相応に増える（`certified_origins_len`
  ゲージで per-key ピン数を監視する）。
- 全ノードが新版になった直後、各ノードは baseline=0 から全量転送を行う。
  1 レスポンス 512 件の上限があり、1 サイクル 1 peer あたり最大 8 ラウンドまで
  連続して引く（ドレイン）ので、N キーの追いつきは `ceil(N/4096)` ティックで
  完了する。この期間はレーンのトラフィックが跳ねる。
- **エントリはキーの全状態を運ぶ**（CRDT の部分デルタではない）。certified
  プレーンには digest 相当の段階的 diff が無く、取りこぼしたエントリを後から
  要求する手段が無いため、部分ペイロードを土台無しにマージすると
  「フィールドが静かに欠けた certified マップ」が有効な証明付きで配られてしまう。
  全状態を運ぶことで、以降のどの更新でも穴が修復される。転送量はその分増える。
- certified プレーンには digest 相当の段階的 diff が無い。ディスクを失って
  復帰したノードの修復は **certified 全件転送**になる。

### 拒否されたエントリの扱いと定期リスキャン

`Store::delta_entries_since` は baseline より**厳密に大きい**エントリしか返さない。
したがって「あるエントリを飛ばして baseline を進める」ことは、そのノードにとって
**恒久的な取りこぼし**を意味する（そのキーが再度書かれない限り二度と提供されない）。
レーンはこれを踏まえて拒否理由を 2 つに分ける:

- **一過性**（`origin ahead` / `policy denied` = 制御プレーンの適用遅れ）:
  baseline を直前の成功エントリで**保留**し、次サイクルで再試行する。
  12 ラウンド連続で解消しない場合のみ、**そのエントリ 1 件だけ**を飛ばして
  `certified_sync_forced_skip_total` を上げる（1 キーが peer 全体を止めないため）。
- **恒久**（CRDT 型衝突）: 再試行しても解決しないので次へ進む。

さらに **30 分ごとに peer ごとの `baseline=0` フルリスキャン**が走る。これが
certified プレーンの唯一の修復パスであり、(a) 上で飛ばしたエントリ、(b) スカラ
baseline では表現できない取りこぼし（ingest は書き手の HLC を保持するため、peer は
自分の frontier より**下**のキーを正当に持ちうる）の両方を回収する。収束済みのキーは
RR ゲートで落ちるだけなので、コストは転送量と受信側の軽い照合に限られる。

### ローリングアップグレード

レーンを持たないノードは 404 を返し、`Unsupported` として扱われる（backoff の
失敗として数えない）。アップグレード**中**に
`certified_sync_unsupported_total` が増えるのは正常。**完了後も増え続ける場合は
未アップグレードノードが残っており、そのノードの certified write は複製されて
いない。**

### キルスイッチ

`NodeRunnerConfig::certified_sync_enabled`（既定 `true`）を `false` にすると、
certified の pull を**一切発行しない**（結果を捨てるのではなく、リクエスト自体を
出さない）。挙動はレーン導入前と完全に一致する。
出荷バイナリでは `ASTEROIDB_CERTIFIED_SYNC_DISABLED=1`（または `true`）を設定して
再起動することで切り替える（`ASTEROIDB_DIGEST_SYNC_DISABLED` と同じ形式）。

### セキュリティ上の注意

このレーンの ingest は**クライアント面のポリシーゲートを経由しない**。internal
token を持つ者は certified ストアへ値を注入しうる。したがって `/api/internal/*`
の Bearer token を必ず設定すること（レーンのルートは他の internal ルートと同じ
ミドルウェア配下にある）。緩和策は 3 点:

1. ローカル write と同じ `resolve_scope` を通す（authority 定義の無いキーは拒否）
2. origin が受信側の現行ポリシーバージョンより**先**のものは拒否
3. **ingest は `Pending` の追跡しか作れない。** `certified_cache` / `attestations`
   / frontier のいずれにも触れないため、**単独で `Certified` や proof を捏造できない**

### origin バージョンについて

複製されるエントリは、書き手のノードでの**書き込み時ポリシーバージョン（origin）**を
ワイヤで運ぶ。受信側はこれをピン留めする。これは必須であって最適化ではない:
origin が無いと、既に v2 に上がったノードが v1 で書かれた値を**自分の v2 frontier**で
評価して Certified にしてしまう（`is_version_fenced` は attestation の admission
しか止めないので防げない）。

origin 永続化以前に書かれたキーは `policy_version: null` で送られ、受信側は現行
バージョンにフォールバックする（復旧時の既存挙動と同値で、確定タイミングが早まる
だけ）。`certified_sync_origin_unknown_total` で可視化される。

---

## 5. バックアップ・リストア手順

### 5.1 バックアップ対象

| ファイル/ディレクトリ | 場所 | 説明 |
|---------------------|------|------|
| データディレクトリ | `$ASTEROIDB_DATA_DIR/` | ストアデータ、永続化状態 |
| ストアスナップショット | `$ASTEROIDB_DATA_DIR/eventual.snapshot.bin`, `certified.snapshot.bin` | eventual / certified ストアのチェックポイント |
| WAL | `$ASTEROIDB_DATA_DIR/wal/` | スナップショット以降の書き込みログ（スナップショットとセットでバックアップすること） |
| ピアレジストリ | `$ASTEROIDB_DATA_DIR/peers.json` | クラスタメンバーシップ |
| システム名前空間 | `$ASTEROIDB_DATA_DIR/system_namespace.json` | 配置ポリシー、Authority 定義 |
| 設定ファイル | `$ASTEROIDB_CONFIG` (任意) | ノード設定 JSON |

> **注意**: eventual / certified のスナップショットは独立にチェック
> ポイントされるため、ライブバックアップ内で両者の整合時点は厳密には
> 一致しない（リストア後の CRDT マージ / anti-entropy で収束する）。

### 5.2 バックアップ手順

```bash
# === 定期バックアップスクリプト ===
#!/bin/bash
BACKUP_DIR="/backup/asteroidb/$(date +%Y%m%d_%H%M%S)"
DATA_DIR="${ASTEROIDB_DATA_DIR:-./data}"
NODE_ID="${ASTEROIDB_NODE_ID:-unknown}"

mkdir -p "$BACKUP_DIR"

# データディレクトリのスナップショット
# 注意: AsteroidDB は CRDT ベースなので、ライブバックアップでも整合性は保たれる
cp -r "$DATA_DIR" "$BACKUP_DIR/data-${NODE_ID}"

# メタデータのバックアップ
curl -s "http://localhost:3000/api/metrics" > "$BACKUP_DIR/metrics-${NODE_ID}.json"
curl -s "http://localhost:3000/api/slo" > "$BACKUP_DIR/slo-${NODE_ID}.json"

# 圧縮
tar czf "$BACKUP_DIR.tar.gz" -C "$(dirname $BACKUP_DIR)" "$(basename $BACKUP_DIR)"
rm -rf "$BACKUP_DIR"

echo "Backup saved to ${BACKUP_DIR}.tar.gz"
```

**推奨バックアップ頻度:**

| 環境 | 頻度 | 保持期間 |
|------|------|---------|
| 開発 | 日次 | 7 日 |
| ステージング | 12 時間ごと | 14 日 |
| 本番 | 6 時間ごと | 30 日 |

### 5.3 リストア手順

```bash
# 1. ノード停止
sudo systemctl stop asteroidb

# 2. 既存データのバックアップ（念のため）
mv $ASTEROIDB_DATA_DIR ${ASTEROIDB_DATA_DIR}.old

# 3. バックアップからリストア
tar xzf /backup/asteroidb/20260310_060000.tar.gz -C /tmp
cp -r /tmp/data-node-1 $ASTEROIDB_DATA_DIR

# 4. ノード再起動
sudo systemctl start asteroidb

# 5. 収束を確認
# CRDT マージにより他ノードのデータが自動的に同期される
asteroidb-cli --host localhost:3000 slo
```

---

## 6. ログ設定とログレベル

### 6.1 ログフレームワーク

AsteroidDB は `tracing` + `tracing-subscriber` を使用した構造化ログを出力します。
ログレベルは `RUST_LOG` 環境変数で制御します。

**`RUST_LOG` を設定しない場合の既定は `info`** です。`Dockerfile` にも
`docker-compose.yml` / `docker-compose.scale.yml` にも `RUST_LOG` は書かれていないため、
出荷構成ではこのコード側の既定がそのまま効きます（既定が ERROR だと full sync の 413、
署名不正、checkpoint 失敗、GC ゲート阻止、ping 失敗といった WARN が 1 行も出ず、
他の障害が観測できなくなるため）。`RUST_LOG` を明示した場合は既定は注入されず、
指定した内容だけが有効になります。

### 6.2 ログレベル設定例

```bash
# 全体を info、AsteroidDB モジュールは debug
RUST_LOG=info,asteroidb_poc=debug

# 同期処理のみ詳細ログ
RUST_LOG=asteroidb_poc::network::sync=debug,asteroidb_poc=info

# 全体を warn、compaction と authority は info
RUST_LOG=warn,asteroidb_poc::compaction=info,asteroidb_poc::authority=info

# 本番環境推奨
RUST_LOG=asteroidb_poc=info

# 障害調査時
RUST_LOG=asteroidb_poc=debug

# 詳細なトレース（パフォーマンスに影響あり）
RUST_LOG=asteroidb_poc=trace
```

### 6.3 主要なログメッセージ

| モジュール | ログ内容 | レベル |
|-----------|---------|-------|
| `runtime::node_runner` | sync cycle 結果、certification 結果 | info |
| `network::sync` | peer sync 成功/失敗、delta/full sync 選択 | info/warn |
| `network::membership` | join/leave イベント、ping 結果 | info |
| `compaction::engine` | checkpoint 作成、compaction 実行 | info |
| `authority::certificate` | 証明書検証、keyset ローテーション | info |
| `placement::rebalance` | rebalance 開始/完了、キー移行 | info |

### 6.4 ログローテーション

systemd 環境では journald が自動的にログを管理します。
Docker 環境では Docker のログドライバを設定してください。

```yaml
# docker-compose.yml に追加
services:
  node-1:
    logging:
      driver: json-file
      options:
        max-size: "50m"
        max-file: "5"
```

---

## 7. パフォーマンスチューニング

### 7.1 Compaction チューニング

Compaction エンジンはチェックポイント方式でログ圧縮を行います (FR-010)。

**デフォルト設定:**

| パラメータ | デフォルト値 | 説明 |
|-----------|------------|------|
| `time_threshold_ms` | 30,000 (30 秒) | 時間ベースのチェックポイント閾値 |
| `ops_threshold` | 10,000 | 操作数ベースのチェックポイント閾値 |

いずれかの閾値に先に到達した時点でチェックポイントが作成されます。

**アダプティブ Compaction:**

アダプティブモード有効時、書き込みレートと Authority frontier lag に基づいてチューニングが自動調整されます:

| 条件 | 動作 | 範囲 |
|------|------|------|
| 書き込みレート > 750 ops/sec | ops_threshold を半減 | 最小 1,000 |
| 書き込みレート < 30 ops/sec | ops_threshold を倍増 | 最大 50,000 |
| Frontier lag > 15 秒 | time_threshold を 50% 増加 | 最大 120,000ms |
| Frontier lag < 1 秒 | time_threshold を 25% 減少 | 最小 10,000ms |

チューニング間隔はデフォルト 30 秒です。書き込みレートは 60 秒のスライディングウィンドウで計測されます。

**Compaction が実行される条件:**
1. チェックポイント閾値（時間 or 操作数）に到達
2. 同じキー範囲・ポリシーバージョンの Authority 過半数がチェックポイント以降の更新を取り込み済み

Compaction が進まない場合は Authority ノードの可用性を確認してください。

**チェックポイント履歴の保持:**
- アダプティブモード時: デフォルト 10 世代まで保持（`max_checkpoint_history`）
- 非アダプティブ時: 無制限

### 7.2 同期 (Sync) チューニング

**NodeRunner デフォルト設定:**

| パラメータ | デフォルト値 | 説明 |
|-----------|------------|------|
| `sync_interval` | 2 秒 | anti-entropy 同期間隔 |
| `ping_interval` | 10 秒 | メンバーシップ gossip 間隔 |
| `certification_interval` | 1 秒 | 保留書き込みの certification 評価間隔 |
| `compaction_check_interval` | 10 秒 | compaction チェック間隔 |
| `frontier_report_interval` | 1 秒 | Authority frontier 報告間隔 |
| `epoch_check_interval` | 60 秒 | epoch 境界チェック（epoch カウンタ更新のみ。keyset 失効判定用。**鍵ローテーションは発火しない** — `docs/runbook/key-rotation.md` 参照）|
| `gc_interval` | 60 秒 | トゥームストーン GC 間隔 |
| `frontier_gc_interval` | 60 秒 | ack-frontier GC 間隔 |

**デルタ同期 vs フルシンク:**

デルタ同期は前回の frontier からの差分のみを送信します。以下の条件でフルシンクにフォールバックします:

- 変更キー数 / 全キー数 > `full_sync_threshold` (デフォルト 0.5 = 50%)
- デルタ同期のペイロードが `MAX_DELTA_PAYLOAD_BYTES` (512 KiB) を超過
- ピアとの frontier 情報が未初期化

**バックオフ:**

同期失敗時は指数バックオフが適用されます:
- 初回失敗: 即座にリトライ
- 連続失敗: `min(INITIAL * 2^failures, MAX_BACKOFF)` で待機
- 最大バックオフ: 2 秒
- 成功時: バックオフリセット

**チューニング指針:**

| 状況 | 推奨調整 |
|------|---------|
| 低レイテンシ環境（同一DC内） | `sync_interval` を 1 秒に短縮 |
| 高レイテンシ環境（WAN） | `sync_interval` を 5-10 秒に延長 |
| 書き込み負荷が高い | `full_sync_threshold` を 0.3 に下げて早めにフルシンク |
| メモリ使用量が多い | `gc_interval` を 30 秒に短縮（ただしゲート不合格・hole 停滞による tombstone 蓄積には効かない — 3.7 / 12.3 を参照） |
| フルシンク比率が高い | `sync_interval` を短縮して差分を小さく保つ |

### 7.3 バッチサイズ

同期データは `DEFAULT_BATCH_SIZE` (100 エントリ) 単位でバッチ送信されます。
大量のキーがある環境では、ネットワーク帯域とレイテンシのトレードオフを考慮してください。

---

## 8. ネットワーク設定

### 8.1 ポート構成

| ポート | プロトコル | 用途 |
|--------|----------|------|
| 3000 (デフォルト) | HTTP | クライアント API + 内部 API |

AsteroidDB は単一ポートで全トラフィック（クライアント/内部通信）を処理します。

### 8.2 API エンドポイント一覧

**クライアント API（認証不要）:**

| メソッド | パス | 説明 |
|---------|------|------|
| GET | `/healthz` | ヘルスチェック |
| GET | `/api/metrics` | メトリクス取得 |
| GET | `/api/slo` | SLO ステータス |
| POST | `/api/eventual/write` | Eventual 書き込み |
| GET | `/api/eventual/{*key}` | Eventual 読み取り |
| POST | `/api/certified/write` | Certified 書き込み |
| GET | `/api/certified/{*key}` | Certified 読み取り |
| GET | `/api/status/{*key}` | Certification ステータス |
| POST | `/api/certified/verify` | 証明検証 |
| GET | `/api/control-plane/versions` | バージョン履歴 |
| GET | `/api/topology` | トポロジー情報 |

**内部 API（トークン認証が有効時は要認証）:**

| メソッド | パス | 説明 |
|---------|------|------|
| POST | `/api/internal/sync` | フルシンク |
| POST | `/api/internal/sync/delta` | デルタシンク |
| POST | `/api/internal/certified/delta` | certified 複製レーン（pull） |
| GET | `/api/internal/keys` | 全キーダンプ |
| POST/GET | `/api/internal/frontiers` | Frontier 送受信 |
| POST | `/api/internal/join` | ノード参加 |
| POST | `/api/internal/leave` | ノード離脱 |
| POST | `/api/internal/announce` | メンバーシップ通知 |
| POST | `/api/internal/ping` | ピアリスト交換 |
| POST | `/api/internal/raft/vote` | Control plane Raft: RequestVote |
| POST | `/api/internal/raft/append` | Control plane Raft: AppendEntries |
| POST | `/api/internal/raft/snapshot` | Control plane Raft: InstallSnapshot |

**Control Plane API（トークン認証が有効時は要認証）:**

| メソッド | パス | 説明 |
|---------|------|------|
| PUT | `/api/control-plane/policies` | 配置ポリシー設定（Raft リーダーのみ） |
| DELETE | `/api/control-plane/policies/{prefix}` | 配置ポリシー削除（Raft リーダーのみ） |
| GET | `/api/control-plane/policies` | 配置ポリシー一覧 |
| GET | `/api/control-plane/policies/{prefix}` | 配置ポリシー取得 |
| PUT | `/api/control-plane/authorities` | Authority 定義設定（Raft リーダーのみ） |
| GET | `/api/control-plane/authorities` | Authority 一覧 |
| GET | `/api/control-plane/authorities/{prefix}` | Authority 取得 |
| GET | `/api/control-plane/raft/status` | Raft コンセンサス状態（認証不要の読み取り専用） |

### 8.3 内部認証トークン

ノード間通信を保護するために Bearer トークン認証を設定できます。

```bash
# トークン生成（全ノードで同一トークンを使用）
export ASTEROIDB_INTERNAL_TOKEN=$(openssl rand -hex 32)
```

トークン設定時:
- `/api/internal/*` エンドポイントに `Authorization: Bearer <token>` ヘッダが必要
- Control Plane の変更 API（PUT/DELETE）にも認証が必要
- 読み取り専用 API（GET /api/metrics, GET /healthz など）は認証不要

**重要:** 空文字列のトークンは「未設定」として扱われます（認証なし）。
Docker Compose で `${ASTEROIDB_INTERNAL_TOKEN}` が未定義の場合に空文字列が代入される問題を防ぐためです。

### 8.4 TLS 設定

AsteroidDB 自体は TLS 終端機能を持ちません。本番環境では以下のいずれかで TLS を終端してください:

**方法 1: リバースプロキシ**

```nginx
# nginx.conf
upstream asteroidb {
    server 127.0.0.1:3000;
}

server {
    listen 443 ssl;
    ssl_certificate /etc/ssl/certs/asteroidb.crt;
    ssl_certificate_key /etc/ssl/private/asteroidb.key;

    location / {
        proxy_pass http://asteroidb;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

**方法 2: サイドカープロキシ（Envoy, Linkerd など）**

サービスメッシュ環境では mTLS を自動的に処理できます。

### 8.5 ファイアウォール設定

```bash
# クライアントアクセス（外部から）
ufw allow 3000/tcp comment "AsteroidDB client API"

# ノード間通信（クラスタ内部のみ）
# 同一トークンによる認証が有効な場合、ポート制限は任意
ufw allow from 10.0.1.0/24 to any port 3000 proto tcp comment "AsteroidDB internal"
```

---

## 9. ノード追加・削除手順

### 9.1 ノード追加

1. **設定準備:**

   ```bash
   # 新ノードの環境変数を設定
   export ASTEROIDB_NODE_ID=node-4
   export ASTEROIDB_BIND_ADDR=0.0.0.0:3000
   export ASTEROIDB_ADVERTISE_ADDR=10.0.1.4:3000
   export ASTEROIDB_AUTHORITY_NODES=node-1,node-2,node-3
   export ASTEROIDB_INTERNAL_TOKEN=<既存クラスタと同じトークン>
   ```

2. **設定ファイル作成（任意）:**

   ```json
   {
     "node": {
       "id": "node-4",
       "mode": "Both",
       "tags": ["region:ap-northeast-1"]
     },
     "bind_addr": "0.0.0.0:3000",
     "peers": {
       "self_id": "node-4",
       "peers": {
         "node-1": {"node_id": "node-1", "addr": "10.0.1.1:3000"}
       }
     }
   }
   ```

   少なくとも 1 つのシードピアを指定すれば、残りのピアは gossip で自動発見されます。

3. **ノード起動:**

   ```bash
   asteroidb-poc
   ```

   起動時に fan-out join が実行され、全既知ピアに参加を通知します。

4. **確認:**

   ```bash
   # 新ノードのステータス確認
   asteroidb-cli --host 10.0.1.4:3000 status

   # 既存ノードから新ノードが見えることを確認
   curl -s http://10.0.1.1:3000/api/internal/ping \
     -H 'Content-Type: application/json' \
     -d '{"sender_id":"probe","sender_addr":"","known_peers":[]}'

   # データ同期の進捗確認
   asteroidb-cli --host 10.0.1.4:3000 metrics
   # peer_sync にエントリが増えていることを確認
   ```

5. **Authority に追加する場合:**

   Raft リーダー宛てに送る（リーダー以外は `503 NOT_LEADER` +
   `x-asteroidb-leader-id` / `x-asteroidb-leader-addr` ヒントヘッダを返す。
   §14 参照）:

   ```bash
   curl -X PUT http://<leader-addr>/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{
       "key_range_prefix": "",
       "authority_nodes": ["node-1", "node-2", "node-3", "node-4"]
     }'
   ```

### 9.2 ノード削除

1. **グレースフルシャットダウン（推奨）:**

   ```bash
   # Ctrl-C でプロセスを停止
   # → fan-out leave が自動実行され、全ピアに離脱を通知
   # → システム名前空間がディスクに保存される
   ```

2. **手動削除:**

   ```bash
   curl -X POST http://10.0.1.1:3000/api/internal/leave \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{"node_id": "node-4"}'
   ```

3. **Authority から削除する場合:**

   Authority 定義を更新して対象ノードを除外（Raft リーダー宛て。§14 参照）:

   ```bash
   curl -X PUT http://<leader-addr>/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{
       "key_range_prefix": "",
       "authority_nodes": ["node-1", "node-2", "node-3"]
     }'
   ```

   **注意:** Authority ノード数が過半数を下回る変更は避けてください。

4. **削除後の確認:**

   ```bash
   # 残りのノードで SLO を確認
   asteroidb-cli --host 10.0.1.1:3000 slo
   ```

---

## 10. 配置ポリシー変更手順

### 10.1 配置ポリシーの構造

配置ポリシーはキー範囲（プレフィクス）ごとにレプリカ配置を制御します:

- `replica_count`: レプリカ数
- `required_tags`: 必須タグ（全レプリカが持つべきタグ）
- `forbidden_tags`: 禁止タグ（レプリカに含めないタグ）
- `on_partition`: 分断時の挙動

### 10.2 ポリシー変更手順

1. **現在のポリシーを確認:**

   ```bash
   curl -s http://localhost:3000/api/control-plane/policies | jq .
   ```

2. **Raft リーダーを確認してポリシーを設定:**

   ポリシー変更は **Raft リーダーのみ**が受理する。リーダーは
   `/api/control-plane/raft/status` で確認できる:

   ```bash
   curl -s http://localhost:3000/api/control-plane/raft/status | jq '{role, leader_id, leader_addr}'
   ```

   ```bash
   curl -X PUT http://<leader-addr>/api/control-plane/policies \
     -H 'Content-Type: application/json' \
     -H 'Authorization: Bearer <token>' \
     -d '{
       "key_range_prefix": "user/",
       "replica_count": 3,
       "required_tags": ["region:ap-northeast-1"],
       "forbidden_tags": []
     }'
   ```

   リーダー以外のノードへ送ると `503` + `error_code: NOT_LEADER` が返り、
   応答ヘッダ `x-asteroidb-leader-id` / `x-asteroidb-leader-addr` に
   リーダーのヒントが入る（`Retry-After: 1` 付き）。クライアントは
   ヒント先へ再送すること（サーバー側転送は行わない）。
   旧クライアントの `approvals` フィールドは受理されるが**無視**される
   （deprecated——合意は Raft ログ複製が担う）。

3. **リバランスの自動実行を監視:**

   ポリシー変更後、NodeRunner がリバランスプランを自動計算し、キーの移行を開始します。

   ```bash
   # リバランス進捗の確認
   curl -s http://localhost:3000/api/metrics | jq '{
     rebalance_start_total,
     rebalance_keys_migrated,
     rebalance_keys_failed,
     rebalance_complete_total
   }'
   ```

4. **注意事項:**
   - ポリシー変更は Control Plane Raft の**過半数 commit** が必要（分断中の
     少数派側では 503/504 で失敗する——14.3 を参照）
   - ポリシーの `version` はコミット順に全ノードで同一の値が採番される
   - リバランス中もサービスは継続
   - `rebalance_keys_failed` が 0 でない場合は失敗キーの調査が必要

---

## 11. 容量計画

### 11.1 ストレージ見積もり

```
ストレージ = キー数 x 平均値サイズ x レプリカ数 x (1 + オーバーヘッド)
```

| コンポーネント | 見積もり方 |
|--------------|-----------|
| CRDT メタデータ | 値あたり約 100-200 バイト（HLC タイムスタンプ、ドット情報） |
| トゥームストーン | OR-Set/OR-Map の場合、削除操作 1 件あたり約 50 バイト。certified sweep 到達後は (key, writer) ペアあたり約 50 バイト定数の compaction floor（NodeId 文字列 + カウンタ。tombstone 1 件と同程度のフットプリント）に圧縮される。優位性は 1 件あたりのサイズではなく「削除件数に比例して無制限に増える tombstone → writer 数に対して定数の floor」という件数の圧縮にある |
| チェックポイント | キー範囲あたり最大 10 世代（アダプティブモード時） |
| ピアレジストリ | ノード 1 台あたり約 200 バイト |
| システム名前空間 | ポリシー数 x 約 500 バイト |

**計算例:**
- 100 万キー、平均値 1KB、レプリカ数 3、オーバーヘッド 30%
- ストレージ = 1,000,000 x 1KB x 3 x 1.3 = 約 3.9 GB/ノード

### 11.2 メモリ見積もり

```
メモリ = アクティブキー数 x 平均値サイズ + 同期バッファ + メトリクスウィンドウ
```

| コンポーネント | 見積もり |
|--------------|---------|
| KV ストア | キー数 x (キーサイズ + 値サイズ + 200B メタデータ) |
| 同期バッファ | ピア数 x `MAX_DELTA_PAYLOAD_BYTES` (512 KiB) |
| メトリクスウィンドウ | ピア数 x 約 10 KiB（60 秒ウィンドウ） |
| SLO トラッカー | 4 SLO x 観測数 x 約 20 バイト |
| Compaction エンジン | キー範囲数 x チェックポイント履歴数 x 約 200 バイト |

### 11.3 ネットワーク帯域見積もり

```
帯域 = (sync_interval あたりの変更量) x ピア数
```

| シナリオ | 見積もり |
|---------|---------|
| デルタ同期（通常時） | 変更キー数 x 平均値サイズ x ピア数 / sync_interval |
| フルシンク（フォールバック時） | 全キー x 平均値サイズ x ピア数 |
| Frontier 報告 | ピア数 x 1KB / frontier_report_interval |
| Ping/Gossip | ピア数 x 500B / ping_interval |

**帯域削減のポイント:**
- `sync_interval` を適切に設定してデルタを小さく保つ
- `full_sync_threshold` を調整してフルシンク頻度を制御
- bincode シリアライゼーション（内部通信で自動使用）は JSON より約 30-50% 小さい

---

## 12. トラブルシューティング

### 12.1 同期遅延（Sync Lag）

**症状:**
- Eventual 読み取りが古いデータを返す
- `sync_failure_rate` > 0.1 (10%)
- `peer_sync` の `p99_latency_us` が SLO 目標を超過

**診断:**

```bash
# メトリクスで同期状態を確認
curl -s http://localhost:3000/api/metrics | jq '{
  sync_failure_rate,
  sync_attempt_total,
  sync_failure_total,
  delta_sync_count,
  full_sync_fallback_count,
  full_sync_fallback_ratio,
  peer_sync
}'

# SLO バジェットを確認
asteroidb-cli --host localhost:3000 slo
```

**対処:**

| 原因 | 対処 |
|------|------|
| ネットワーク障害 | ピア間の接続を確認、ファイアウォールルールの検証 |
| ピアの過負荷 | 負荷分散、ノード追加を検討 |
| デルタが大きすぎる | `sync_interval` を短縮してデルタサイズを小さく保つ |
| フルシンク頻度が高い | `full_sync_threshold` を下げてフルシンクを先行実行 |
| バックオフ中 | 最大 2 秒で回復するため一時的には待機で可 |

**手動フルシンク:**

```bash
# 正常ノードから遅延ノードへ全データを強制同期
curl -s http://good-node:3000/api/internal/keys | \
  curl -X POST http://lagging-node:3000/api/internal/sync \
    -H 'Content-Type: application/json' -d @-
```

### 12.2 Split-Brain

**症状:**
- 異なるノードが同一キーに対して異なる値を返す
- `frontier_skew_ms` が異常に高い（> 10,000ms）
- 同期失敗率の急増

**診断:**

```bash
# 全ノードの frontier skew を比較
for node in node-1:3000 node-2:3001 node-3:3002; do
  echo "=== $node ==="
  curl -s "http://$node/api/metrics" | jq '.frontier_skew_ms'
done

# ピア間の接続性を確認
curl -s http://node-1:3000/api/internal/ping \
  -H 'Content-Type: application/json' \
  -d '{"sender_id":"probe","sender_addr":"","known_peers":[]}'
```

**対処:**

1. **ネットワーク分断が原因の場合:**
   - 接続を復旧すれば CRDT マージにより自動収束
   - `replication_convergence` SLO で収束を監視

2. **クロックドリフトが原因の場合:**
   - 各ノードの NTP 同期を確認
   - HLC は中程度のドリフトを補償するが、10 秒以上のドリフトは順序異常を引き起こす可能性あり

3. **手動介入が必要な場合:**

   ```bash
   # 正常ノードのデータを信頼ソースとして他ノードに同期
   curl http://trusted-node:3000/api/internal/keys > /tmp/dump.json
   curl -X POST http://diverged-node:3001/api/internal/sync \
     -H 'Content-Type: application/json' -d @/tmp/dump.json
   ```

### 12.3 メモリ増大

**症状:**
- プロセスの RSS が時間とともに増加
- OOM Killer によるプロセス終了

**診断:**

```bash
# メトリクスで状態サイズを推定
curl -s http://localhost:3000/api/metrics | jq '{
  write_ops_total,
  pending_count,
  rebalance_keys_migrated
}'

# キー数の確認（注意: 全キーを転送するため大規模環境では負荷が高い）
curl -s http://localhost:3000/api/internal/keys | jq '.entries | length'
```

**対処:**

| 原因 | 対処 |
|------|------|
| トゥームストーン蓄積 | まず `gc_last_sweep_wall_ms` を見て「一度でも sweep したか」を切り分ける（0 のまま＝GC 実質停止。3.7.1 の手順で `gc_gate_blocked_authority_total` / `gc_gate_blocked_peer_total` と `tombstone GC gate blocked` WARN へ）。その上で原因を分類する: GC ゲート不合格（dead peer が registry に残存・分断・lagging authority）と hole 停滞（`gc_floor_stalled_hole_dots`、3.7 を参照）には `gc_interval` 短縮は**効かない**。ゲートが通っていて単に回収周期が長いだけの場合のみ `gc_interval` を短縮（デフォルト 60 秒） |
| ack-frontier エントリの蓄積 | `frontier_gc_interval` を短縮、`frontier_gc_max_retained_versions` を縮小 |
| Compaction が進まない | Authority 可用性を確認（過半数必要） |
| pending_count が高い | Certified Write のタイムアウト設定を見直す |
| 大量のキーデータ | ノード追加によるデータ分散を検討 |

### 12.4 ディスク使用量増大

**症状:**
- データディレクトリのサイズが想定を超えて増大
- Compaction の完了数がゼロ

**診断:**

```bash
# データディレクトリサイズ
du -sh $ASTEROIDB_DATA_DIR

# Compaction メトリクス
curl -s http://localhost:3000/api/metrics | jq '{
  rebalance_start_total,
  rebalance_complete_total,
  rebalance_keys_migrated,
  rebalance_keys_failed,
  write_ops_total
}'
```

**対処:**

1. Compaction が止まっている場合:
   - Authority ノードの可用性を確認（過半数がチェックポイント以降の更新を取り込む必要あり）
   - `frontier_skew_ms` が大きい場合、Authority の同期遅延を解消

2. チェックポイント履歴の肥大化:
   - アダプティブ Compaction を有効にして `max_checkpoint_history` を設定

3. 古いバックアップファイルの削除

### 12.5 Certified Write のタイムアウト

**症状:**
- Certified Write が `timeout` ステータスを返す
- `pending_count` が増加し続ける

**診断:**

```bash
curl -s http://localhost:3000/api/metrics | jq '{
  pending_count,
  certified_total,
  certification_latency_mean_us,
  frontier_skew_ms
}'
```

**対処:**

| 原因 | 対処 |
|------|------|
| Authority 過半数が到達不能 | Authority ノードの復旧を優先 |
| Frontier 同期遅延 | `frontier_report_interval` を短縮 |
| ネットワーク遅延 | WAN 環境では certification timeout を延長 |
| 高負荷 | Authority ノードのリソースを増強 |

### 12.6 「key_rotation_total が増加しない」

**これは現仕様であり故障ではない。** 自動鍵ローテーション（FR-008 の
scheduled rotation）は未配線 — production で `EpochManager::stage_keys` を
呼ぶ経路が無く、`check_and_rotate` は常に no-op のため、
`key_rotation_total` / `key_rotation_last_version` / `key_rotation_last_time_ms`
は増加せず常に 0（last_version は初期値のまま）である。これらのメトリクスに
アラートを設定している場合は撤去すること。

鍵の更新が必要な場合（漏洩・定期ローテーション等）は、
`ASTEROIDB_AUTHORITY_KEYS` の再配布 + 再起動による**手動手順**を使う —
`docs/runbook/key-rotation.md` を参照。

Certified Read の証明検証が失敗する場合は鍵ローテーションの問題ではなく、
鍵配布の不整合を疑う:
- `ASTEROIDB_BLS_SEED` が正しく設定されているか
- 全ノードの `ASTEROIDB_AUTHORITY_KEYS` が同一かつ最新のエントリ一式か
  （3 セグメント PoP 形式。上記「BLS 鍵配布のローリングアップグレード手順」参照）

### 症状: 起動時に BLS Proof-of-Possession エラーで終了する

`ASTEROIDB_REQUIRE_SIGNED_FRONTIERS=1`（strict モード）で以下のようなエラーが出て起動に失敗する:

```
error: ASTEROIDB_REQUIRE_SIGNED_FRONTIERS is set but ASTEROIDB_AUTHORITY_KEYS entry for '<id>' ...
```

**原因と対処:**
- `ASTEROIDB_AUTHORITY_KEYS` の当該エントリが PoP を欠く旧 2 セグメント形式（`<ed>/<bls>`）、または PoP の検証に失敗している。各ノードの起動ログにある配布用エントリ（`Authority key entry for ASTEROIDB_AUTHORITY_KEYS distribution: ...`）から正しい 3 セグメント形式（`<ed>/<bls>/<pop>`）をコピーする。
- BLS 検証をそのピアで不要とする場合は、当該エントリの BLS 部（`/<bls>/<pop>`）を削除して Ed25519 のみにする。
- 過渡的に strict を外す場合は `ASTEROIDB_REQUIRE_SIGNED_FRONTIERS` を未設定にする。

### 症状: アップグレード後にピアの frontier 報告が拒否され certification が進まない

lenient モードの警告ログに以下が出る:

```
warning: ASTEROIDB_AUTHORITY_KEYS entry for '<id>' has a BLS key without a proof-of-possession; ignoring the BLS part (Ed25519-only)
```

または未更新ノード側でピアのエントリ全体がスキップされている。

**原因と対処:**
- `ASTEROIDB_AUTHORITY_KEYS` を 3 セグメント形式へ切り替えるタイミングが早すぎ、未更新（旧バイナリ）のノードがピアエントリ全体（Ed25519 鍵を含む）をスキップしている。「BLS 鍵配布のローリングアップグレード手順」（本ガイドの環境変数リファレンス直後の節）に従い、**全ノードのバイナリを新版へ更新してから** env を一斉に切り替える。
- 警告のみで degrade している場合は、該当ピアの配布用エントリ（PoP 付き）を収集して env を更新し、再起動する。

---

## 13. 障害復旧手順

### 13.1 単一ノード再起動

```bash
# 1. グレースフルシャットダウン（Ctrl-C）
# → fan-out leave + システム名前空間保存 + 最終チェックポイント
#   （WAL リプレイは最小化されるが、ヘッダのみのアクティブセグメントは残る）

# 2. 再起動
sudo systemctl restart asteroidb

# 3. 起動後の動作:
# - スナップショット + WAL リプレイでローカルデータを復元
#   （kill -9 等の非グレースフル停止でも ack 済み書き込みは WAL から復元される）
# - 永続化されたピアレジストリを読み込み
# - fan-out join で全ピアに再参加を通知
# - anti-entropy sync で停止中の差分に追いつく

# 4. 収束確認
asteroidb-cli --host localhost:3000 slo
# → replication_convergence の violations が 0 になるまで待機
```

### 13.2 データ復元

```bash
# 1. ノード停止
sudo systemctl stop asteroidb

# 2. 破損データの退避
mv $ASTEROIDB_DATA_DIR ${ASTEROIDB_DATA_DIR}.corrupted.$(date +%s)

# 3. バックアップからリストア
mkdir -p $ASTEROIDB_DATA_DIR
tar xzf /backup/asteroidb/latest.tar.gz -C $ASTEROIDB_DATA_DIR

# 4. ノード再起動
sudo systemctl start asteroidb

# 5. CRDT マージにより不足分は自動同期される
# 進捗を監視:
watch -n 5 'asteroidb-cli --host localhost:3000 metrics | grep sync'
```

### 13.3 ピアレジストリ破損時

```bash
# ピアレジストリが壊れた場合、削除して再起動
rm $ASTEROIDB_DATA_DIR/peers.json

# 設定ファイルにシードピアが含まれていれば自動復旧
# 設定ファイルがない場合はシードピアの環境変数を設定して再起動
sudo systemctl restart asteroidb
```

### 13.4 クラスタ再構築

全ノードを失った場合の完全再構築:

```bash
# 1. 最新のバックアップを各ノードにリストア
for i in 1 2 3; do
  ssh node-$i "
    mkdir -p /var/lib/asteroidb
    tar xzf /backup/asteroidb/latest-node-$i.tar.gz -C /var/lib/asteroidb
  "
done

# 2. シードノード（node-1）を最初に起動
ssh node-1 "sudo systemctl start asteroidb"

# 3. 残りのノードを順次起動
sleep 5
ssh node-2 "sudo systemctl start asteroidb"
ssh node-3 "sudo systemctl start asteroidb"

# 4. クラスタの健全性確認
for i in 1 2 3; do
  echo "=== node-$i ==="
  asteroidb-cli --host node-$i:3000 status
  asteroidb-cli --host node-$i:3000 slo
done

# 5. 全ノードのデータ整合性を確認
# CRDT マージにより自動収束するが、SLO で監視
```

### 13.5 Authority 過半数喪失時

詳細な手順は `docs/runbook/disaster-recovery.md` の Scenario 3 を参照。

**要約:**
1. 生存 Authority ノードの復旧を最優先
2. 復旧不可能な場合は Authority 定義を更新して新ノードを指定
3. 全 Authority 喪失時は手動でシステム名前空間を編集し、新 Authority を定義して再起動

### 13.6 WAL / スナップショット破損時（クラッシュリカバリ runbook）

起動時のリカバリが失敗するのは次の 2 パターンで、いずれもノードは明確な
エラーメッセージを出して **fail-stop** する（空ストアでの暗黙上書きは
しない）。

> **絶対にデータディレクトリを丸ごと退避・削除しないこと。**
> `$ASTEROIDB_DATA_DIR` にはストアごとに独立した回復特性を持つファイルが
> 同居しており、ピアから再構築できるのは **eventual ストアだけ**である:
>
> | パス | 内容 | ピアから再構築 |
> |------|------|----------------|
> | `eventual.snapshot.bin` + `wal/eventual/` | Eventual ストア | **可**（anti-entropy が再充填） |
> | `certified.snapshot.bin` + `wal/certified/` | Certified ストア | **条件付きで可**（certified replication レーンが再充填する。ただし digest 相当の段階的 diff が無いため復旧は certified 全件転送になる。`certified_sync_enabled=false` で運用している場合は**不可** — 破棄は恒久喪失） |
> | `raft/`（`hard_state.json` / `log.json`） | Control-plane Raft 投票状態・ログ | **不可**（白紙化は投票済み term での二重投票を再導入し得る — §14.5 の手順以外で消さない） |
> | `equivocation_evidence.json` | Equivocation 証拠（否認不能な POM） | **不可**（消失した証拠は戻らない） |
> | `frontier_report_clock.json` | frontier 報告 HLC の write-ahead floor（M-12） | **喪失は安全に劣化 / 復元は禁止**。喪失しても起動は成功するが、activation grace 180 秒の間この Authority の frontier 報告が停止する（恒久喪失なし）。**バックアップからの復元は絶対にしない**——古いリースは「全署名済み report を覆う」という floor の証拠性を偽り、誤検知（正当 Authority への偽 equivocation 証拠）を招き得る。data dir を復元する場合はこのファイルを削除してから起動する |
>
> 退避するのは**壊れたストアのファイルのみ**。`raft/`・
> `equivocation_evidence.json`・certified スナップショット/WAL は必ず保持する。

**症状 A: `snapshot ... failed to load` で起動失敗（スナップショット破損）**

エラーメッセージにどちらのストア（`eventual` / `certified`）かが含まれる。

```bash
# --- 破損したのが eventual ストアの場合 ---
# 1. eventual ストアのファイルのみ退避（削除しない — 事後調査用）。
#    raft/ や equivocation_evidence.json、certified.* には触れない
mv "$ASTEROIDB_DATA_DIR/eventual.snapshot.bin" \
   "$ASTEROIDB_DATA_DIR/eventual.snapshot.bin.corrupt.$(date +%s)"
mv "$ASTEROIDB_DATA_DIR/wal/eventual" \
   "$ASTEROIDB_DATA_DIR/wal/eventual.corrupt.$(date +%s)"

# 2. 再起動 → eventual ストアだけ空で起動し、anti-entropy がピアから再充填する
sudo systemctl restart asteroidb

# 3. 収束確認
asteroidb-cli --host localhost:3000 slo
```

```bash
# --- 破損したのが certified ストアの場合 ---
# certified replication レーンが有効（既定）で、かつ他ノードが同じキーを
# 保持しているなら、eventual と同様にピアから再充填できる。ただし certified
# プレーンには digest 相当の段階的 diff が無いため、復旧は certified 全件転送
# になる（キー数によっては長時間かかる）。
mv "$ASTEROIDB_DATA_DIR/certified.snapshot.bin" /tmp/quarantine/
mv "$ASTEROIDB_DATA_DIR/wal/certified" /tmp/quarantine/
# 再起動後、certified_sync_entries_applied_total の増加で再充填を確認する

# レーンを無効化して運用している場合、または当該キーを持つノードが他に無い
# 場合は、ピアからの再構築経路は無い。バックアップからの復元が唯一の手段
cp /backup/.../certified.snapshot.bin "$ASTEROIDB_DATA_DIR/certified.snapshot.bin"
# （バックアップが無い場合、破棄すればそのノードの certified 状態は恒久喪失する。
#  破棄前に必ず破損ファイルを保全し、影響範囲を確認すること）
```

**症状 B: `WAL ... is corrupted mid-log` で起動失敗（WAL の中間破損）**

最終セグメント末尾の不完全レコード（torn tail、クラッシュ時の正常な形）は
警告のみで自動リカバリされる。fail-stop するのは「ack 済みデータが壊れて
いる可能性がある」中間破損のみ。選択肢は 2 つ:

```bash
# 選択肢 (a) 推奨: 当該ストアの snapshot+WAL をバックアップから復元する。
# eventual ストアに限っては、その snapshot+WAL のみを退避してピアから
# 再充填する方法も使える（症状 A の eventual の手順と同じ）。
# certified ストアも certified replication レーンが有効なら同様に退避 +
# ピア再充填が使える（ただし全件転送になる）。レーン無効時はバックアップ復元のみ。
# raft/ と equivocation_evidence.json はどちらの場合も必ず保持する

# 選択肢 (b) 単一ノード運用などバックアップ・ピア再充填とも不可能な場合のみ:
# 最初の無効レコード以降を明示的に切り捨てて起動（それ以降の ack 済み
# 書き込みは失われる）。切り捨てはディスク上でも物理的に行われる
# （破損セグメントは最後の有効レコード境界まで truncate され、以降の
# セグメントは削除される）ため、フラグを外して再起動しても同じ破損で
# 再度 fail-stop することはない
ASTEROIDB_WAL_RECOVER_TRUNCATE=1 asteroidb
# 起動成功後は必ず変数を外すこと（常用すると将来の破損の silent loss になる）
```

**注意**: 破損ノードを復旧させる前に、そのノードを書き込みの宛先から
外すこと。破損状態のままピアへ push させない（無傷レプリカのデータを
優先する）。

---

## 14. Control plane Raft 運用

ポリシー / Authority 定義の変更は内蔵 Raft（`ASTEROIDB_CONTROL_PLANE_NODES` の
静的投票者集合による過半数 commit）で合意される。設計の詳細は
`docs/architecture.md` の「Control Plane Raft の設計」を参照。

### 14.1 状態の確認

```bash
curl -s http://localhost:3000/api/control-plane/raft/status | jq .
# {
#   "node_id": "node-1",
#   "role": "leader",          # leader / follower / candidate / detached
#   "term": 3,
#   "leader_id": "node-1",
#   "leader_addr": null,
#   "commit_index": 12,
#   "last_applied": 12,
#   "last_log_index": 12,
#   "voters": ["node-1", "node-2", "node-3"],
#   "observer_ns_pull_success_total": 0,   # 非 voter のみ増加（M-17、§14.8）
#   "observer_ns_pull_failure_total": 0,
#   "observer_ns_last_pull_unix_ms": 0,
#   "observer_ns_version_counter": 12
# }
```

`commit_index` と `last_applied` が長時間乖離する、あるいは `role` が
`candidate` のまま安定しない場合は、ピア到達性（`ASTEROIDB_RAFT_PEERS`）と
選挙タイムアウト設定（リンク RTT に対して短すぎないか）を確認する。

### 14.2 メンバー変更手順（動的メンバー変更はスコープ外）

joint consensus / single-server 変更は実装していない。投票者集合の変更は
**フルクラスタ再起動**で行う。

**前提条件（commit 済みエントリを失わないための条件 — §14.5 と同趣旨）:**

- **収束済みログを持つノードが「新集合の過半数」を占めること**（言い換える
  と、空状態で参加するノードは新集合の過半数未満に抑えること）。commit 済み
  エントリを守れるのは、それを保持するノード群が新集合内で過半数を形成し
  続ける場合だけである。本実装の投票判定は log up-to-date 比較のみで、
  空ログノード同士は `(0,0) >= (0,0)` で相互に投票できるため、空ログ
  ノードだけで新過半数が成立すると（例: `{A,B,C}` → `{A,B,D,E,F}` の
  一括変更。旧集合の過半数 A,B を含んでいても、空の D,E,F だけで 3/5 が
  成立する）、空ログリーダーが旧 commit 済みエントリと矛盾するログを
  commit し得る — 「旧集合の過半数を含む」だけでは不十分な点に注意。
  この条件を満たせない大規模な入れ替えは、一度に行わず**追加ノードが
  新集合の過半数未満に収まる段階に分割**して行うこと（各段階の後、新
  ノードのログ収束を確認してから次の段階へ進む）。
- **停止前にログ収束を確認すること。** 全投票者で
  `GET /api/control-plane/raft/status` を確認し、`commit_index` /
  `last_applied` / `last_log_index` が全ノードで一致してから停止する。
  乖離したまま停止・再構成すると、最新の commit 済みログを持つノードが
  新集合内で過半数を形成できなくなるリスクが高まる。
- **`data_dir/raft/` は全ノードで保持したまま再起動すること**（白紙化は
  §14.5 の破損復旧手順に限る）。raft/ を失ったノードは「空状態ノード」
  として上の過半数計算に数えること。

**手順:**

1. **全ノードを停止**する（ローリング変更は禁止——1 ノードずつ環境変数を
   変えると新旧構成の過半数が交差せず、同一 term に 2 リーダーが成立し得る）。
2. **全ノードで同一の** `ASTEROIDB_CONTROL_PLANE_NODES`（および必要なら
   `ASTEROIDB_RAFT_PEERS`）に更新する。
3. 全ノードを再起動する。永続化済みの Raft ログ・term は引き継がれる。

**1 → 2 台への拡張の注意**: 過半数が 1 から 2 になるため、新ノードが
起動・到達可能になるまで**制御プレーンは即座に書き込み不能**になる。
拡張はまとめて 1 → 3 台にする方が可用性の谷が浅い。

**既存クラスタからの移行（pre-Raft → Raft）**: 最初のリーダーが自ノードの
ローカルポリシー全量を `Bootstrap` エントリとして複製コアに焼き込む
（reset-and-import）。**最も新しい namespace バージョンを持つノードが最初の
リーダーになるように**起動順を調整する（そのノードだけ先に起動して
リーダー化させてから残りを起動する）——`Bootstrap` の version_floor が
ポリシーバージョンフェンシングの単調性を担保する。非 canonical ノードに
だけ存在した余剰ローカルポリシーは Bootstrap で置換（削除）される。

**災害復旧時の observer 追随の注意（M-17）**: observer の pull 採用ガードは
`(version_counter, last_applied_index)` の辞書式単調比較で新旧を判定する。
Bootstrap はポリシーが 1 件以上あれば version_counter を必ず floor より増や
すため、上記どおり「最新 namespace バージョンのノードを最初のリーダーに」
していれば再構築後の状態は observer に自動採用される。**ポリシー 0 件で
Bootstrap した直後**だけは counter が floor のまま進まず、既存 observer の
採用が保留され得る（その状態には certify 対象も存在しないため実害はなく、
最初のポリシー作成で解消する）。

### 14.3 分断時の挙動

- **少数派側**（過半数と通信できない側）: ポリシーの**参照は可能**
  （GET はローカル読み）だが、**更新は不可**（proposal がタイムアウトし
  503/504）。これは CP としての設計意図であり修正すべきバグではない。
- **データプレーンの Eventual write は無影響**——可用性は CRDT 経路が担う。
- 分断復帰後は少数派側のノードが自動的に follower へ降格し、未 commit の
  エントリは上書きされる。commit 済みエントリが失われることはない。
- **observer（非 voter）の分断時挙動**は §14.8 を参照（署名は止めない・
  分断中 bump の寄与消失は可視化＋自動復帰）。

### 14.4 単一ノード運用

既定（`ASTEROIDB_CONTROL_PLANE_NODES` 未設定）では自ノードのみの単一
投票者クラスタとなり、起動直後に自身がリーダーになって即 commit できる
（開発 UX は従来どおり）。ただし**永続化義務は単一ノードでも免除されない**
（`data_dir/raft/` は再起動安全性の要件）。

### 14.5 Raft 状態ファイル破損からの復旧

起動時に `data_dir/raft/hard_state.json` / `log.json` のパースに失敗した
場合、ノードは **fail-stop**（起動拒否）する——破損状態で起動すると投票済み
term で二重投票し split-brain を招くため。復旧手順:

1. 当該ノードの `data_dir/raft/` を**退避してから削除**する。
2. ノードを再起動する。白紙の Raft 状態で follower として参加し、リーダーの
   ログ複製 / InstallSnapshot で自動的に追い付く。
3. **過半数のノードで同時に Raft 状態を失った場合は白紙削除しない**こと
   （commit 済みエントリの喪失リスク）。残存ノードの `raft/` を正とし、
   個別に判断する。

### 14.5.1 空の authority 定義が掃き出されたときの復旧

起動時に次の WARN が出た場合:

```
dropped persisted authority definitions with an empty node set
  prefixes=["", "user/"]
```

そのノードの永続 namespace に、**authority ノードが 0 個の自動生成
authority 定義**が残っていた。これは旧バイナリの欠陥によるもので、
クラスタ inventory が空のまま `recalculate_authorities` が走ると、
正規手順で設定した authority 定義が `authority_nodes: []` に置換されていた。
authority が 0 個の scope は過半数しきい値が充足不能で何も certify できず、
当該レンジのトゥームストーン GC も止まるため、定義を残すより削除する方が
常に良い（定義が無ければ、正しい inventory が揃った時点で再導出される）。

掃き出しは**バージョンを bump しない**（自動生成定義はノードローカルな
導出であり複製状態には載らないため、削除は巻き戻しにならない）。
現行バイナリでは空の自動生成定義が二度と作られないので、この WARN は
アップグレード後の初回起動で一度だけ出る。

復旧の要否判断:

1. `GET /api/control-plane/authorities` を**全ノードで**実行し、WARN が
   挙げた prefix に定義が在るか確認する。
2. **在る場合**: 複製状態から自己修復済み。対応不要。
3. **無い場合**: その prefix に certified な配置ポリシーが在るなら、
   リーダーに対して定義を再投入する。

```bash
curl -X PUT http://<leader-addr>/api/control-plane/authorities \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"key_range_prefix":"user/","authority_nodes":["node-1","node-2","node-3"]}'
```

再投入するまで、当該 prefix への `POST /api/certified/write` は
`403 POLICY_DENIED` を返す（authority 0 個で暗黙に Pending 滞留させるより、
明示的に拒否する方が安全なため）。`GET /api/certified/{key}` は 200 のまま
`status: "Pending"` を返す。

**空配列・重複 ID・空文字列 ID の投入で「無効化」することはできない**
（いずれも `400 BAD_REQUEST`）。certified 運用を止めたい場合は
`DELETE /api/control-plane/policies/{prefix}` で配置ポリシーを削除すること。
**`certified: false` への変更では止まらない** — certified 経路のどこも
このフラグを読んでいないため、この API で作った手動定義はそのまま
certify し続ける（詳細は api-reference の該当節）。

#### 手動で作られた空の authority 定義について

掃き出しが落とすのは `auto_generated` な空定義だけで、**手動の空定義
（`auto_generated: false`）は意図的に残る** — 空であっても、そこには
この関数が再構成できない運用者の意図が入っている可能性があるため。
ただし旧バイナリはこの API で空配列を受理していたので、そうした定義が
残っているノードが**まだ Bootstrap していない制御プレーンの最初のリーダー**
になると、`Bootstrap` エントリに載って全ノードへ複製される
（`Bootstrap` / `InstallSnapshot` の apply 側には空 spec のスキップが無い）。
新規クラスタ、または pre-Raft からの移行中にのみ起こりうる。

**移行前チェック**: pre-Raft からの移行、または既存 namespace を持つノードで
新しく Raft をブートストラップする前に、全ノードで
`GET /api/control-plane/authorities` を実行し、`authority_nodes` が空の
エントリが無いことを確認する。在る場合は先に非空の値を PUT しておくこと
（当該プレフィックスは P0-3 の修正により恒久的に `403` になる）。

### 14.6 セキュリティ

`/api/internal/raft/*`（vote / append / snapshot）は他の内部エンドポイント
と同じく `ASTEROIDB_INTERNAL_TOKEN` の Bearer 認証で保護される。
**トークン未設定時は無認証で開放される**。その状態では偽の
AppendEntries / InstallSnapshot を投げるだけで制御プレーン
状態全体を汚染できるため、**本番環境ではトークン設定を必須とする**。

### 14.7 observer（非 voter）ノードの運用と namespace pull（M-17）

`ASTEROIDB_CONTROL_PLANE_NODES` に含まれないノードは **observer** として
動作する: 選挙に参加せず、ポリシー / Authority 定義の変更を受け付けない。
Raft は voter にしか複製しないため、observer の namespace は
**committed 済み制御プレーン状態の定期 pull**
（`POST /api/internal/raft/namespace`、周期 `ASTEROIDB_OBSERVER_NS_PULL_MS`、
既定 5 秒）で voter に追随する。pull は選挙状態（term / votedFor）と定足数
会計に一切触れず、採用は `(version_counter, last_applied_index)` の辞書式
単調比較でガードされる（古い状態へのロールバックは構造的に不可能）。

**なぜ重要か**: observer が certification authority を兼ねる場合、その署名が
乗せる policy_version は **observer 自身の namespace** から決まる。pull が
止まったまま policy bump が起きると、observer は旧版で署名し続け、受信側
voter で fence され、**certification 定足数から無音で脱落する**（分母
`total_authorities` は縮まないため、書き込みは Pending→Timeout として顕在化
する）。M-17 以前はこの脱落を示すカウンタが存在しなかった。

**監視（アラート推奨）**:

- observer 側 `GET /api/control-plane/raft/status`:
  `observer_ns_pull_success_total` / `observer_ns_pull_failure_total` /
  `observer_ns_last_pull_unix_ms`（`now − last_pull` が **pull 間隔 × 6 超**
  で WARN ログも出る）/ `observer_ns_version_counter`（voter の値との差が
  namespace ラグ）。失敗カウンタは到達不能だけでなく、**応答者が voter
  集合外だった場合（アドレス誤解決——HTTP としては毎回成功するが採用され
  ない）と、採用時のローカル永続化失敗（ディスク満杯・read-only）も数える**。
  これらの異常では `observer_ns_last_pull_unix_ms` は更新されないため、
  pull-age アラートは誤設定・ローカルディスク障害でも必ず発火する（成功
  扱いになるのは「採用した」「相手の状態が新しくなかった」健全ラウンド
  のみ）。原因の切り分けは pull 失敗 WARN ログの error フィールドで行う。
- voter 側 `GET /api/metrics`: `attestation_stale_version_total` の継続増加
  （= どこかの authority——分断中の少数派 voter を含む——が旧版で報告して
  いる。**bump 1 回目から発火する最速の信号**）、
  `attestation_rejected_fenced_total`（fence 到達 = 寄与消失の確証）。

**分断時の挙動（設計判断）**: 分断中も observer authority は**署名を止めない**。
分断中に bump が無ければ、最後に pull 済みの版での署名は完全に有効で定足数
に寄与し続ける（高遅延・分断環境で observer authority を置く動機そのもの）。
分断中に bump があった場合の寄与消失は certification 意味論上どの設計でも
不可避であり、本実装はそれを「無音の恒久欠損」から「両側で可視の一時欠損 +
分断解消後 1 pull 間隔 + 1 tick（既定 ~6 秒）での自動復帰」に変えている。

**Runbook**: observer authority の pull age が警報中（stale WARN が出ている /
`observer_ns_last_pull_unix_ms` が古い）に policy 変更を行うと、その authority
の寄与を分断解消まで失う。**bump 前に pull age を確認すること。**

**構成の注意**:

- 非 voter を authority 定義に含める `PUT /api/control-plane/authorities` は
  拒否されず 200 で通り、レスポンスの `warnings` フィールドと WARN ログで
  通知される（pull が生きていれば正当な構成のため）。
- observer にするつもりのノードで `ASTEROIDB_CONTROL_PLANE_NODES` を未設定に
  するのは誤り: そのノードは「自分だけの単一 voter クラスタ」として自己選出
  し、**pull は起動しない**（起動時警告あり）。observer には必ず実際の voter
  集合（自分を含まない）を設定する。
- **デプロイ順序**: 新エンドポイントを知らない旧 voter への pull は 404 →
  失敗カウンタ増＋バックオフとなり、従来挙動（凍結）への劣化に留まる。
  **voter 先行 → observer 後追い**でアップグレードすること。
- 鮮度 TTL（署名の自発停止）は実装していない。将来の opt-in ノブ候補として
  `docs/followup-plan.md` に記録がある。

---

## 付録: CLI リファレンス

```bash
# ステータス確認
asteroidb-cli --host <addr> status

# メトリクス表示
asteroidb-cli --host <addr> metrics

# SLO 確認
asteroidb-cli --host <addr> slo

# データ操作
asteroidb-cli --host <addr> get <key>
asteroidb-cli --host <addr> put <key> <value>
```

## 作成ポリシーだけを先に配送する

`ASTEROIDB_OPERATION_POLICY_DESTINATIONS` を設定する。形式は `ASTEROIDB_OPERATION_REQUIRED_DESTINATIONS` と同じ行配列で、例は `[{"namespace_id":"example","key_prefix":"orders/","destinations":["owner-2"]}]`。操作本体を routed にしている場合は、その経路を policy-only にも使える。policy-only だけ独立させる場合は `ASTEROIDB_OPERATION_POLICY_ROUTE_EDGES` と `ASTEROIDB_OPERATION_POLICY_ROUTES` を一緒に設定する。操作本体が non-routed でも、この policy-only 固定経路を使用できる。どちらも `ASTEROIDB_OPERATION_LINK_SEED` と `ASTEROIDB_OPERATION_LINK_KEYS`、隣接先の許可 prefix を設定する。

公開 `POST /api/operations/policy/submit` に完全な署名済み Create packet を渡すと、作成ポリシーを操作台帳の外に保存し、専用の永続 outbox から固定宛先へ再送する。宛先は Enabled/Disabled を本体より先に学べる。policy-only witness の Disabled は、受信ノードが対象キーを担当していなくても公開 Certified 読みに `NotTargeted` として反映される。Disabled の本体未着キーは Certified 読みで `NotTargeted` になる。保存済み `operation_policy_outbox/` がある場合、宛先設定を外すか変更すると起動を拒否する。
