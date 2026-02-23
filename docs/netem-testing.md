# AsteroidDB netem Testing Guide

Linux の `tc` (traffic control) / netem を Docker コンテナ内で使い、
ネットワーク遅延や分断をシミュレートして AsteroidDB の挙動を検証する手順を説明します。

## 前提条件

| 項目 | 要件 |
|------|------|
| **Docker** | 20.10 以降 |
| **Docker Compose** | V2 (`docker compose` コマンド) |
| **OS** | Linux (ホスト) または Docker Desktop (macOS/Windows) |
| **Python 3** | シナリオスクリプト内で JSON パースに使用 |

> **Note**: `tc` / netem は Linux カーネル機能です。Docker Desktop (macOS/Windows) では
> Docker VM 内の Linux カーネルを使うため動作しますが、一部制約がある場合があります。

### NET_ADMIN capability

netem ルールの追加には `NET_ADMIN` capability が必要です。
`docker-compose.yml` の各サービスに以下が設定されていることを確認してください:

```yaml
services:
  node-1:
    cap_add:
      - NET_ADMIN
    # ...
```

### iproute2

コンテナ内に `tc` コマンドが必要です。スクリプトは `tc` が見つからない場合、
自動的に `apt-get install iproute2` を実行します。

## 基本コマンド

### 遅延注入

```bash
# node-3 に 200ms の遅延を追加
./scripts/netem/add-delay.sh asteroidb-node-3 200

# node-1 に 50ms の遅延を追加
./scripts/netem/add-delay.sh asteroidb-node-1 50
```

コンテナ内の全送信パケットに指定ミリ秒の遅延が挿入されます。
受信側にも遅延を入れたい場合は、相手側コンテナにも同様に設定してください。

### 完全分断 (100% パケットロス)

```bash
# node-3 を完全分断
./scripts/netem/add-partition.sh asteroidb-node-3
```

100% のパケットロスを設定し、対象コンテナからの全通信を遮断します。
コンテナ自体は正常に動作し続けますが、他ノードとの通信ができなくなります。

### netem ルール除去 (復旧)

```bash
# node-3 のネットワークを復旧
./scripts/netem/remove-netem.sh asteroidb-node-3
```

設定済みの netem ルールを削除し、通常のネットワーク状態に戻します。

### 現在の設定確認

```bash
docker exec asteroidb-node-3 tc qdisc show dev eth0
```

## シナリオ

### シナリオ 1: 遅延環境での eventual 収束

高遅延環境でも CRDT マージにより最終的にデータが収束することを確認します。

```bash
# 1. クラスタ起動
./scripts/cluster-up.sh

# 2. 全ノードにヘルスチェック
./scripts/cluster-status.sh

# 3. node-2, node-3 に 200ms 遅延を注入
./scripts/netem/add-delay.sh asteroidb-node-2 200
./scripts/netem/add-delay.sh asteroidb-node-3 200

# 4. node-1 にデータ書き込み
curl -X POST http://localhost:3001/api/eventual/write \
  -H "Content-Type: application/json" \
  -d '{"type":"counter_inc","key":"delay-test"}'

# 5. 通常より長く待機 (遅延分を考慮)
sleep 5

# 6. 各ノードの値を確認
curl -s http://localhost:3001/api/eventual/delay-test
curl -s http://localhost:3002/api/eventual/delay-test
curl -s http://localhost:3003/api/eventual/delay-test

# 7. 復旧
./scripts/netem/remove-netem.sh asteroidb-node-2
./scripts/netem/remove-netem.sh asteroidb-node-3
```

**期待結果**: 遅延があっても、十分な待機時間後に全ノードで同じカウンタ値が得られる。
CRDT のマージは可換・結合・冪等であるため、到達順序に関係なく収束する。

### シナリオ 2: 分断 -> 復旧 -> certified 確定

ネットワーク分断中のデータ分岐と、復旧後の CRDT 収束を確認します。

```bash
# 自動スクリプトで実行
./scripts/netem/scenario-partition-recovery.sh
```

手動で実行する場合:

```bash
# 1. クラスタ起動
./scripts/cluster-up.sh

# 2. node-1 にカウンタを3回インクリメント
for i in 1 2 3; do
  curl -sf -X POST http://localhost:3001/api/eventual/write \
    -H "Content-Type: application/json" \
    -d '{"type":"counter_inc","key":"partition-test"}'
done

# 3. 同期を待機
sleep 3

# 4. 全ノードで値を確認 (全て 3 であるべき)
curl -s http://localhost:3001/api/eventual/partition-test
curl -s http://localhost:3002/api/eventual/partition-test
curl -s http://localhost:3003/api/eventual/partition-test

# 5. node-3 を分断
./scripts/netem/add-partition.sh asteroidb-node-3

# 6. node-1 にさらに5回インクリメント
for i in 1 2 3 4 5; do
  curl -sf -X POST http://localhost:3001/api/eventual/write \
    -H "Content-Type: application/json" \
    -d '{"type":"counter_inc","key":"partition-test"}'
done

# 7. 同期を待機
sleep 3

# 8. 分岐を確認
#    node-1, node-2: 8 (3 + 5)
#    node-3: 3 (分断前の値のまま)
curl -s http://localhost:3001/api/eventual/partition-test
curl -s http://localhost:3002/api/eventual/partition-test
curl -s http://localhost:3003/api/eventual/partition-test

# 9. node-3 を復旧
./scripts/netem/remove-netem.sh asteroidb-node-3

# 10. 収束を待機して確認
sleep 5
curl -s http://localhost:3001/api/eventual/partition-test
curl -s http://localhost:3002/api/eventual/partition-test
curl -s http://localhost:3003/api/eventual/partition-test

# 11. certified write を試行
curl -X POST http://localhost:3001/api/certified/write \
  -H "Content-Type: application/json" \
  -d '{
    "key": "partition-test",
    "value": {"type": "counter", "value": 8},
    "on_timeout": "pending"
  }'

# 12. 認証ステータス確認
curl -s http://localhost:3001/api/status/partition-test
```

**期待結果**:

| ステップ | node-1 | node-2 | node-3 | 状態 |
|---------|--------|--------|--------|------|
| 初期書き込み後 | 3 | 3 | 3 | 収束済み |
| 分断中の追加書き込み後 | 8 | 8 | 3 | 分岐 |
| 復旧後 | 8 | 8 | 8 | 再収束 |

> **Note**: 現時点ではノード間レプリケーションが docker compose 構成で未接続のため、
> 自動収束は確認できない場合があります。レプリケーション統合後に完全なシナリオが動作します。

## トラブルシューティング

### tc コマンドが見つからない

```
Error: exec: "tc": executable file not found in $PATH
```

**対処**: スクリプトは自動インストールを試みますが、手動で実行する場合:

```bash
docker exec asteroidb-node-3 bash -c "apt-get update && apt-get install -y iproute2"
```

### Permission denied

```
RTNETLINK answers: Operation not permitted
```

**対処**: `docker-compose.yml` に `cap_add: [NET_ADMIN]` が設定されているか確認してください。
設定変更後は `docker compose up -d --build` で再起動が必要です。

### netem ルールが適用されない (macOS/Windows)

Docker Desktop 環境では Docker VM のネットワークスタックを経由するため、
ホストからコンテナへの通信には netem ルールが効かない場合があります。
コンテナ間通信 (Docker ネットワーク内) には正常に適用されます。

### コンテナ名が見つからない

```
Error: No such container: asteroidb-poc-node-3-1
```

**対処**: `docker ps` でコンテナ名を確認してください。`docker-compose.yml` で
`container_name` を明示設定している場合は `asteroidb-node-3` のようになります。

```bash
docker ps --format '{{.Names}}'
```

### 遅延が大きすぎてタイムアウトする

HTTP API のデフォルトタイムアウトを超える遅延を設定すると、クライアント側で
タイムアウトエラーが発生します。curl の `--max-time` を遅延に合わせて調整してください:

```bash
# 500ms 遅延の場合、タイムアウトを長めに設定
curl --max-time 10 http://localhost:3003/api/eventual/test-key
```
