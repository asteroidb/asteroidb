# 法的クリアランスレポート — AsteroidDB

> **免責事項**: このレポートはエンジニアリングチームによる調査結果をまとめたものです。法的助言ではありません。実際の意思決定においては必ず知財専門弁護士にご相談ください。
>
> **調査日**: 2026年3月8日

---

## 1. BLS 署名の特許クリアランス

### 対象技術

- 実装箇所: `src/authority/bls.rs`
- 使用ライブラリ: `blst = "0.3"` (Supranational 製)
- アルゴリズム: BLS12-381 楕円曲線上の閾値署名

### 特許調査結果

#### US7,653,817「Signature Schemes Using Bilinear Mappings」

| 項目 | 内容 |
|------|------|
| 発明者 | Dan Boneh, Ben Lynn, Hovav Shacham (Stanford) |
| 権利者 | Stanford University |
| 出願日 | 2007年12月12日 |
| 登録日 | 2010年1月26日 |
| **法的状態** | **失効（Expired - Lifetime）** |
| **失効日** | **2023年11月22日** |
| 確認元 | Google Patents: https://patents.google.com/patent/US7653817 |

**主要クレームの概要**: 双線形写像（Weil ペアリング / Tate ペアリング）を用いた楕円曲線上のマルチ署名・階層的プロキシ署名・オンライン/オフライン署名スキーム。

> ✅ **結論**: 2023年11月22日に自然失効。現時点で同特許は公有（パブリックドメイン）に帰している。

#### `blst` ライブラリのライセンス

| 項目 | 内容 |
|------|------|
| ライセンス | **Apache License 2.0** |
| 特許条項 | あり（Section 3 に明示的特許許諾条項を含む） |
| 開発元 | Supranational / Protocol Labs |
| セキュリティ監査 | NCC Group（2021年1月）実施済み |
| 採用事例 | Ethereum 2.0 (Beacon Chain), Filecoin, Chia, Algorand 等 |
| ソース | https://github.com/supranational/blst |

Apache 2.0 の特許条項（Section 3）により、コントリビューターから利用者への明示的な特許許諾が付与される。MIT ライセンスにはない保護であり、BLS 関連の潜在的な特許リスクに対する追加的な防御となる。

> ✅ **結論**: `blst` ライブラリの使用は Apache 2.0 の特許許諾により保護されており、リスクは低い。

#### 注意事項

- エンドモルフィズム（endomorphism）の最適化に関連する特許も過去に存在したが、同様に失効済みとされている（`blst` 開発者のコメントより）。
- BLS 閾値署名に関する Stanford Center for Blockchain Research の継続的研究は学術論文として発表されており、新規特許出願の確認はされていない（2026年3月時点）。
- 継続特許（continuation patent）の可能性については、USPTO データベースの定期確認を推奨する。

---

## 2. CRDT の特許クリアランス

### 対象技術

- 実装箇所: `src/crdt/` (pn_counter.rs, or_set.rs, or_map.rs, lww_register.rs)

### 学術的先行技術

CRDT の基盤アルゴリズムは以下の学術論文として確立されており、これらが強力な先行技術（prior art）として機能する:

| 論文 | 著者 | 年 | 内容 |
|------|------|-----|------|
| "A Comprehensive Study of Convergent and Commutative Replicated Data Types" | Shapiro, Preguiça, Baquero, Zawirski (INRIA) | 2011 | PN-Counter, OR-Set, LWW-Register の形式的定義 |
| "Delta State Replicated Data Types" | Almeida et al. | 2016 | δ-CRDT・差分同期の形式化 |
| "Efficient State-based CRDTs by Delta-Mutation" | Almeida et al. | 2015 | 差分ベース同期の最適化 |

### 業界動向と特許状況

#### Basho Technologies / Riak

| 項目 | 内容 |
|------|------|
| OR-Set 実装 | Basho が独自設計し 2013 年 RICON West で発表 |
| 特許状況 | Basho 破産後、全 IP を bet365 が取得 |
| **現状** | **bet365 が Riak を完全 OSS 化（全 IP 開放）** |
| 確認元 | https://github.com/basho/riak |

bet365 による Riak の完全オープンソース化に伴い、Basho 保有の CRDT 関連 IP は実質的にオープンソースコミュニティに解放された。

#### 各 CRDT の個別評価

| CRDT | リスク評価 | 根拠 |
|------|-----------|------|
| **PN-Counter** | 🟢 低 | Shapiro et al. 2011 で形式化。2005 年以前から学術先行技術あり |
| **LWW-Register** | 🟢 低 | 最終書き込み優先の概念は分散システムの基本原理 |
| **OR-Set** | 🟡 中-低 | Shapiro et al. 2011 + Basho の IP 開放により先行技術が豊富 |
| **OR-Map** | 🟡 中-低 | OR-Set と LWW-Register の合成であり独自性は低い |

> ✅ **結論**: CRDT の基本アルゴリズムはすべて学術的先行技術が充実しており、新規の特許侵害リスクは低い。業界大手（Redis, AWS, Azure Cosmos DB）も CRDT を採用しており、特許によるブロックは現実的でない。

---

## 3. Hybrid Logical Clock (HLC) の特許クリアランス

### 対象技術

- 実装箇所: `src/hlc.rs`
- アルゴリズム: Kulkarni & Demirbas (2014) による HLC

### 調査結果

| 項目 | 内容 |
|------|------|
| 発表形式 | 学術論文（OPODIS 2014 Conference Proceedings） |
| 技術レポート | SUNY Buffalo Tech Report 2014-04 |
| **特許出願** | **確認されず** |
| 採用事例 | CockroachDB, Google Cloud Spanner（類似アプローチ） |

論文: "Logical Physical Clocks" — Kulkarni, Demirbas et al., OPODIS 2014, Springer LNCS vol.8878

HLC は学術コミュニティへのオープンな貢献として発表されており、USPTO・Google Patents での特許出願は確認されていない。CockroachDB や Spanner 等の大手データベースでも採用されており、特許リスクは実質的に存在しない。

> ✅ **結論**: 特許リスクなし。学術的先行技術として確立済み。

---

## 4. 差分同期（Delta Sync）/ アンチエントロピーの特許クリアランス

### 対象技術

- 実装箇所: `src/network/sync.rs`
- アルゴリズム: HLC ベースの差分同期 + フルシンクフォールバック

### 調査結果

| 項目 | 内容 |
|------|------|
| 参照論文 | Almeida et al. (2016) δ-CRDT, OPODIS 2015 |
| **特許出願** | **確認されず** |
| 採用事例 | Cassandra（ゴシップ）, DynamoDB, Riak 等で先行実装 |

HLC フロンティアを用いた差分同期は学術論文に基づき、Cassandra・DynamoDB 等多数の分散データベースで先行実装が存在するため、特許リスクは低い。

> ✅ **結論**: 特許リスクなし。

---

## 5. 商標クリアランス —「AsteroidDB」

### USPTO 検索

USPTO の商標データベース（TESS）での「ASTEROIDDB」「ASTEROID DB」の登録商標・出願は確認されなかった。

> ⚠️ **ただし**: USPTO TESS は JavaScript ベースの動的データベースのため、Web クロールによる完全な確認に限界がある。正確な調査には USPTO TESS（https://tmsearch.uspto.gov）での直接検索が必要。

### 既存プロジェクトとの競合

**重要な発見**: 同名の先行プロジェクトが存在する。

| 項目 | 内容 |
|------|------|
| 名称 | AsteroidDB |
| 作者 | Yusuf Cihan |
| 初出 | 2019年12月16日（Kodular Community フォーラム） |
| 用途 | MIT App Inventor / Kodular 向け簡易 BaaS（Firebase 代替） |
| 現状 | **2021年1月にリポジトリをアーカイブ化、開発停止** |
| 商標登録 | **なし**（確認できず） |
| ライセンス | OSS（具体的ライセンス不明） |
| ソース | https://community.kodular.io/t/asteroiddb-simple-but-powerful-database-service-for-your-apps/46809 |

### リスク評価

| 観点 | 評価 | 根拠 |
|------|------|------|
| 商標登録による侵害 | 🟢 低 | 登録商標が確認されていない |
| コモンロー商標（使用による権利） | 🟡 中 | 2019年から先行使用実績あり |
| 混同可能性 | 🟢 低 | ターゲット市場・用途が全く異なる |
| 名称変更の必要性 | 要検討 | 予防的観点から検討の余地あり |

**混同可能性の分析**:
- 先行 AsteroidDB: MIT App Inventor アプリ向けの単純な BaaS、個人開発者コミュニティ向け
- 本プロジェクト: 分散 KV ストア（衛星コンステレーション等の低レイテンシ環境向け）、エンタープライズ向け

用途・市場・技術スタックが大きく異なるため、消費者の混同が生じる可能性は極めて低い。ただし、完全を期すため USPTO TESS での直接確認と「asteroid」関連の Class 009（コンピュータソフトウェア）・Class 042（SaaS）での検索を推奨する。

### 推奨アクション

1. **今すぐ**: [tmsearch.uspto.gov](https://tmsearch.uspto.gov) で「ASTEROID」を Class 009・042 で直接検索
2. **近期**: 知財弁護士によるコモンロー商標リスクの評価を依頼
3. **判断**: 名称継続使用 or 変更を経営判断として決定
4. **登録推奨**: 使用継続の場合、USPTO Class 009/042 で商標出願を検討

---

## 6. 依存ライブラリのライセンス適合性

`cargo license` による確認推奨。現時点での評価:

| ライブラリ | ライセンス | 特許条項 | 評価 |
|-----------|-----------|---------|------|
| `blst` | Apache 2.0 + MIT | **あり** | ✅ |
| `ed25519-dalek` | MIT / Apache 2.0 | Apache 側にあり | ✅ |
| `tokio` | MIT | なし | ✅ |
| `axum` | MIT | なし | ✅ |
| `serde` | MIT / Apache 2.0 | Apache 側にあり | ✅ |
| `reqwest` | MIT / Apache 2.0 | Apache 側にあり | ✅ |
| `subtle` | BSD-3-Clause | なし | ✅ |
| `proptest` | MIT / Apache 2.0 | Apache 側にあり | ✅ |

GPL 汚染のリスクは現時点では確認されない。

---

## 7. 総合評価サマリー

| カテゴリ | リスク | 状態 |
|---------|--------|------|
| BLS 署名特許（US7,653,817） | 🟢 **解消** | 2023年11月失効確認 |
| `blst` ライブラリ特許許諾 | 🟢 **良好** | Apache 2.0 特許条項あり |
| CRDT アルゴリズム | 🟢 **低リスク** | 学術先行技術 + Basho IP 開放 |
| HLC | 🟢 **リスクなし** | 学術論文のみ、特許出願なし |
| Delta Sync | 🟢 **リスクなし** | 学術論文のみ、広範な先行技術 |
| 商標「AsteroidDB」 | 🟡 **要確認** | 先行使用例あり、登録商標なし |
| 依存ライセンス | 🟢 **良好** | 全て寛容ライセンス |

---

## 8. 推奨アクションリスト（優先度順）

| 優先度 | アクション | 担当 |
|--------|-----------|------|
| ★★★ | USPTO TESS で「ASTEROID」を Class 009/042 で直接確認 | チーム |
| ★★☆ | 知財弁護士によるコモンロー商標リスク評価 | 外部弁護士 |
| ★★☆ | 商標登録の出願検討（使用継続の場合） | 外部弁護士 |
| ★☆☆ | `cargo license` を CI に組み込み依存ライセンスを継続監視 | エンジニア |
| ★☆☆ | 継続特許の有無を USPTO で年次確認（BLS 関連） | チーム |

---

*このドキュメントは 2026年3月8日時点の調査に基づきます。特許・商標の状況は変化するため、定期的な再調査を推奨します。*
