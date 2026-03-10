# AsteroidDB WASM サンプル

ブラウザおよび Node.js 上で AsteroidDB の CRDT 操作を実行するサンプルアプリケーション。

## 前提条件

- Rust (stable)
- `wasm32-unknown-unknown` ターゲット
- `wasm-pack`

```bash
# ターゲット追加
rustup target add wasm32-unknown-unknown

# wasm-pack インストール
cargo install wasm-pack
```

## ビルド

### ブラウザ向け

```bash
cd examples/wasm
wasm-pack build --target web
```

### Node.js 向け

```bash
cd examples/wasm
wasm-pack build --target nodejs
```

## 実行

### ブラウザデモ

ブラウザ向けビルド後、HTTP サーバーを起動して `index.html` を開く:

```bash
# Python の場合
python3 -m http.server 8080

# Node.js の場合
npx serve .
```

ブラウザで `http://localhost:8080` にアクセスすると、以下の CRDT 操作を試せる:

- **PN-Counter**: 2ノード間のインクリメント/デクリメントとマージ
- **OR-Set**: 要素の追加/削除と add-wins マージ
- **LWW-Register**: 値の設定と last-writer-wins マージ
- **Store**: キーバリューストアへの保存・スナップショット

### Node.js テスト

Node.js 向けビルド後:

```bash
node test-node.js
```

全 CRDT 型の基本操作とマージのテストが実行される。

## 提供する WASM バインディング

| クラス | 操作 |
|--------|------|
| `WasmPnCounter` | `new(node_id)`, `increment()`, `decrement()`, `value()`, `merge(other)`, `to_json()` |
| `WasmOrSet` | `new(node_id)`, `add(elem)`, `remove(elem)`, `contains(elem)`, `len()`, `elements_json()`, `merge(other)`, `to_json()` |
| `WasmLwwRegister` | `new(node_id)`, `set(value)`, `get()`, `merge(other)`, `to_json()` |
| `WasmStore` | `new()`, `put_counter(key, counter)`, `put_set(key, set)`, `put_register(key, register)`, `get_json(key)`, `delete(key)`, `contains_key(key)`, `len()`, `keys_json()`, `save_snapshot()`, `load_snapshot()` |
| `self_test()` | 全 CRDT 型の動作確認を実行し結果文字列を返す |

## 制約事項

- BLS12-381 署名は WASM 非対応（`native-crypto` 無効）。Ed25519 のみ利用可能。
- ファイルベースの永続化は利用不可。`MemoryBackend` によるインメモリストレージのみ。
- HTTP サーバー / クライアント機能は WASM 非対応（`native-runtime` 無効）。
- HLC タイムスタンプはブラウザ環境では `Date.now()` の精度に依存。本サンプルでは論理カウンタで代替。
