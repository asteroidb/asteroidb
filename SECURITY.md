# セキュリティ

本ドキュメントでは、AsteroidDB の脅威モデル、信頼境界、使用する暗号プリミティブについて説明します。

## 脅威モデルの概要

AsteroidDB の MVP は **クラッシュ故障耐性** を前提に設計されており、
Byzantine 故障耐性は対象外です。参加するすべてのノードは誠実であり、
クラッシュまたは到達不能になることで障害が発生する想定です。
プロトコルから逸脱する侵害ノードは安全性保証を破壊し得ます。

### スコープ内（MVP）

- **クラッシュ故障**: ノードはいつでも停止、再起動、ネットワーク接続の喪失が発生し得る。
- **ネットワークパーティション**: ノード間のリンクは一時的または長期間にわたり障害が発生し得る。
- **ノード間 API へのリプレイ攻撃**: `ASTEROIDB_INTERNAL_TOKEN` 設定時に
  Bearer トークン認証で防止。
- **無許可の control-plane 変更**: 同じ Bearer トークンで保護。認証済みリクエストのみが
  配置ポリシーと Authority 定義を変更可能。

### スコープ外（MVP）

- **Byzantine 故障**: 悪意のあるノードは署名の偽造、データの捏造、
  無効な certificate の生成が可能。BFT 拡張は将来フェーズで計画。
- **クライアント認証/認可**: 公開 HTTP API（読み取りと eventual write）は
  未認証。クライアント向けデプロイメントでは AsteroidDB をリバースプロキシまたは
  API ゲートウェイの背後に配置すること。
- **保存時暗号化**: ディスク上のデータは暗号化されていない。必要に応じて
  ボリュームレベルの暗号化（例: LUKS、dm-crypt）を使用すること。
- **ノード間トラフィックの TLS**: ノード間通信はデフォルトで平文 HTTP を使用。
  本番環境ではサービスメッシュの背後に配置するか、TLS ターミネータを設定すること。

## 信頼境界

```
+---------------------------+
|        Client Zone        |  信頼なし（MVP では認証なし）
+------------+--------------+
             | HTTP
+------------v--------------+
|        Node (public API)  |  読み取り、eventual write、certified read
+------------+--------------+
             | Internal API (Bearer トークン)
+------------v--------------+
|     Node <-> Node         |  Delta sync、frontier 交換、join/leave
+---------------------------+
             | Internal API (Bearer トークン)
+------------v--------------+
|     Control Plane         |  ポリシー変更、Authority 定義
+---------------------------+
```

### 境界 1: クライアントからノード

- **トランスポート**: HTTP（デフォルトで TLS なし）。
- **認証**: MVP では認証なし。すべての公開エンドポイントがオープン。
- **脅威**: ネットワーク上の攻撃者がデータの読み書きが可能。
- **緩和策**: 本番環境ではアプリケーションレベル認証付きの TLS 終端
  リバースプロキシの背後に配置。

### 境界 2: ノード間

- **トランスポート**: HTTP（オプションで Bearer トークン付き）。
- **認証**: `ASTEROIDB_INTERNAL_TOKEN` 設定時、すべての `/api/internal/*`
  エンドポイントに `Authorization: Bearer <token>` が必要。
  トークンなしの場合、ノード間ルートはオープン。
- **脅威**: internal トークンを取得した攻撃者がクラスタへの参加、
  データの注入、sync の妨害が可能。
- **緩和策**: 強力なランダムトークンを使用し、定期的にローテーション。
  ネットワークアクセスをクラスタノードに限定。

### 境界 3: Control Plane

- **トランスポート**: ノード間と同じ HTTP レイヤ。
- **認証**: 変更ルート（`PUT /api/control-plane/policies`、
  `PUT /api/control-plane/authorities`、`DELETE ...`）に Bearer トークン認証が必要。
- **認可**: 有効なトークンを持つリクエストは control plane を変更可能。
  MVP ではロールベースアクセス制御なし。
- **脅威**: トークン漏洩により任意のポリシー変更（例: レプリカ数の削減、
  Authority ノードの再割り当て）が可能。
- **緩和策**: トークンの配布を限定し、`GET /api/control-plane/versions` で
  ポリシーバージョン履歴を監査。

## 暗号プリミティブ

| プリミティブ | ライブラリ | 用途 |
|------------|----------|------|
| **Ed25519** | `ed25519-dalek 2.x` | Majority certificate 用の個別 Authority 署名 |
| **BLS12-381** | `blst 0.3` | Aggregate threshold signatures。複数 Authority 署名を 1 つに統合 |
| **HLC** | カスタム (`src/hlc.rs`) | 因果順序付けと frontier 追跡のための Hybrid Logical Clock |
| **SHA-256** | `blst` DST 経由 | BLS 署名スキーム用のドメイン分離タグ |

### Ed25519 Certificate

各 Authority ノードは Ed25519 署名鍵を保持します。ノードが更新を確認すると、
`(key_range, frontier_hlc, digest_hash)` タプルに署名します。
Majority certificate は `n/2 + 1` 個の個別署名と対応する検証鍵を収集します。

検証: クライアントは各署名を Authority の公開鍵に対して検証し、
宣言された Authority セットの過半数が署名していることを確認します。

### BLS Aggregate Signatures

BLS モードが有効な場合（`ASTEROIDB_BLS_SEED`）、Authority ノードは
Ed25519 の代わりに BLS12-381 署名を生成します。同一メッセージに対する
複数の BLS 署名は単一の署名に集約でき、n Authority の certificate サイズを
O(n) から O(1) に削減します。

ドメイン分離タグ: `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_`

### 鍵管理

- **Keyset バージョニング**: 鍵は system namespace で単調増加する
  `keyset_version`（初期値 1）で管理。
- **Epoch ローテーション**: デフォルトの epoch 長は 24 時間。epoch 境界で
  ノードは次に公開された keyset に切り替え。
- **猶予期間**: 検証は現在の epoch と過去 7 epoch 分の署名を受理し、
  クロックスキューや伝播遅延に対応。
- **ローテーション手順**:
  1. 次の keyset を system namespace に公開。
  2. Epoch 境界を待つ -- ノードは自動的に切り替え。
  3. 猶予期間終了後、旧鍵は無効化。

## 認証: Internal トークン

AsteroidDB はノード間および control-plane の認証に共有シークレット Bearer トークンを使用します。

### 設定

すべてのノードで同じ値の `ASTEROIDB_INTERNAL_TOKEN` 環境変数を設定:

```bash
export ASTEROIDB_INTERNAL_TOKEN=$(openssl rand -hex 32)
```

### 保護対象ルート

トークン設定時、以下のルートに `Authorization: Bearer <token>` が必要:

- `/api/internal/*` -- sync、frontier 交換、join、leave、ping
- `PUT /api/control-plane/authorities`
- `PUT /api/control-plane/policies`
- `DELETE /api/control-plane/policies/{prefix}`

### 非保護ルート

公開 API エンドポイントは常にオープン:

- `GET /api/eventual/{key}`
- `POST /api/eventual/write`
- `GET /api/certified/{key}`
- `POST /api/certified/write`
- `GET /api/status/{key}`
- `GET /api/metrics`
- `GET /api/slo`
- `GET /api/topology`
- `GET /api/control-plane/authorities`（読み取り）
- `GET /api/control-plane/policies`（読み取り）
- `GET /api/control-plane/versions`（読み取り）

## 既知の制限事項

1. **Byzantine 耐性なし**: 侵害された Authority ノードは任意のデータに対して
   正当に見える署名を生成可能。これは明示的な MVP スコープ境界。

2. **共有シークレットトークンモデル**: すべてのノードが同一トークンを共有。
   ノードごとの ID や相互 TLS なし。トークン漏洩でクラスタへのフルアクセスが可能。

3. **TLS なし**: すべてのトラフィックが平文 HTTP。信頼できないネットワーク上で
   盗聴や MITM 攻撃が可能。

4. **クライアント認証なし**: ネットワーク到達可能な任意のクライアントが
   公開 API 経由でデータの読み書きが可能。

5. **監査ログなし**: API リクエストの改ざん防止ログなし。ポリシーバージョン履歴が
   control-plane 変更の部分的な監査性を提供。

6. **クロック依存**: HLC はおおよそ同期されたクロックに依存。大きなクロックスキュー
   （>> epoch 長）は frontier 追跡の異常を引き起こす可能性あり。

## 脆弱性の報告

本プロジェクトは活発に開発中であり、まだ本番環境にはデプロイされていません。
セキュリティ上の問題を発見した場合は、GitHub Issue を作成するか、
メンテナーに直接ご連絡ください。
