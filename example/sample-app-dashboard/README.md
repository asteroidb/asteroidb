# AsteroidDB Mission Control

AsteroidDB の主要機能をインタラクティブに体験できるサンプル Next.js ダッシュボードアプリケーションです。

## 機能

### Telemetry Playground (`/telemetry`)
AsteroidDB の 4 つの CRDT 型を Eventual 整合性でインタラクティブに操作:
- **PN-Counter** — インクリメント / デクリメント カウンタ
- **LWW-Register** — Last-Writer-Wins 単一値レジスタ
- **OR-Set** — 観測削除可能な集合 (add-wins)
- **OR-Map** — 観測削除可能な連想配列 (LWW 値)

### Certified Writes (`/certified`)
Authority ノード群の過半数合意による Certified 書き込みのデモ:
- Certified write の送信 (Counter / Register)
- ステータスポーリング (pending → certified)
- Proof bundle の詳細表示 (frontier, authority signatures)
- 暗号証明の独立検証 (Verify Proof)

### Cluster Operations (`/cluster`)
クラスタの運用状況をリアルタイム監視:
- ヘルスチェック
- トポロジー (リージョン、ノード、Inter-region レイテンシ)
- ランタイムメトリクス (認証レイテンシ、同期失敗率、Frontier Skew 等)
- SLO error budget バー

## セットアップ

### 前提条件
- Node.js 18 以上
- 稼働中の AsteroidDB クラスタ

### 1. AsteroidDB クラスタの起動

```bash
# リポジトリルートから Docker Compose で 3 ノードクラスタを起動
cd ../..
docker compose up -d
```

各ノードは以下のポートで HTTP API を公開します:
| ノード | ポート |
|--------|--------|
| node-1 | `localhost:3001` |
| node-2 | `localhost:3002` |
| node-3 | `localhost:3003` |

### 2. ダッシュボードの起動

```bash
npm install
npm run dev
```

ブラウザで http://localhost:3000 を開きます。

### 接続先の変更

デフォルトでは `localhost:3001` (node-1) に接続します。変更する場合は `.env.local` を編集:

```
ASTEROIDB_URL=http://localhost:3002
```

または単一ノードで起動している場合:

```bash
# AsteroidDB をポート 4000 で起動
ASTEROIDB_BIND_ADDR=127.0.0.1:4000 cargo run

# .env.local を変更
ASTEROIDB_URL=http://localhost:4000
```

## 技術スタック

- Next.js 15 (App Router)
- React 19
- Tailwind CSS v4
- TypeScript 5

外部 UI ライブラリは使用していません。全 UI は Tailwind CSS のみで構築されています。

## アーキテクチャ

```
ブラウザ (React Client Components)
    ↓ fetch(/api/asteroidb/*)
Next.js Dev Server (rewrites proxy)
    ↓ proxy
AsteroidDB Node (localhost:3001)
```

CORS 回避のため、`next.config.ts` の `rewrites` で AsteroidDB の HTTP API をプロキシしています。
