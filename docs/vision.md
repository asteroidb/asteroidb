# AsteroidDB ビジョン (Draft v0.3)

## 1. 目的

AsteroidDB は、次の 2 つを同時に狙う分散 DB プロジェクトです。

- IPA 未踏に提出できる、技術的に尖った一貫したコンセプトを作る
- 地上と宇宙をまたぐネットワークでも成立する DB アーキテクチャを作る

コンセプトは「宇宙専用」ではなく、以下を同じ設計思想で扱います。

- 地上の複数 DC・複数大陸クラスタ
- 衛星コンステレーションのような高遅延・断続接続環境

## 2. 初期ターゲット

初期ターゲットは、すでに大規模分散データ基盤の課題を持つ利用者です。  
具体的には TiKV クラスのシステムを使っている層を想定します。

最初から宇宙実運用を狙うのではなく、まず地上で高遅延・分断耐性の価値を実証し、宇宙連携に拡張します。

## 3. プロダクト方針

MVP は KVS から開始し、将来拡張します。

設計の柱:

- 既定は Eventual Consistency
- Authority ノード群による Certified 状態をオプション提供
- 分断耐性のため CRDT をネイティブ採用
- 固定階層ではなく、タグベース配置ポリシーを採用
- ノードは `store` / `subscribe` / `both` を選択可能にする
- 配置ポリシーと Authority 定義は DB 自身の control-plane で管理する

## 4. 差別化ポイント

AsteroidDB は、データの見え方を次の 2 つに明確に分離します。

- Eventual: まず可用性優先で受理し、最終的に収束する状態
- Certified: Authority ノード群の合意で確定し、証明付きで取得する状態

この分離により、利用者は操作ごとに「可用性優先か、確定性優先か」を選べます。  
さらに、圧縮後も更新反映状況を追跡できるように、HLC ベースの到達情報 (ack frontier) を中核に据えます。

## 5. MVP スコープ

MVP で実施:

- CRDT ベース KVS
- Authority ノード群を使った確定フロー
- タグベース配置/保持ポリシー
- control-plane 用 system namespace
- `get_eventual` / `get_certified` / 認証状態確認 API
- 認証状態は `pending | certified | rejected | timeout` を返す

MVP で実施しない:

- 分散 SQL レイヤ
- グローバル ACID トランザクション
- 本格的な Byzantine 耐性
- 文章編集系 CRDT

## 6. 現時点のポジショニング

AsteroidDB は、CRDT を中核にした分散 KVS であり、  
高遅延・分断環境でも動作する Eventual 基盤の上に、  
Authority 合意による Certified 読み書きを重ねることで、  
地上大規模クラスタから将来の宇宙地上連携までを同じモデルで扱います。

## 7. 将来拡張の方向

- system namespace はハイブリッド方式 (論理階層パス + タグ) で大中小クラスタ管理へ拡張
- majority certificate 署名を本実装し、検証可能性を強化
- 上位レイヤ (SQL やアプリ向け SDK) を追加し、一般開発者向け UX を改善
