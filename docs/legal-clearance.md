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

### USPTO TESS 実検索結果（2026年3月8日取得）

「ASTEROID」全件（64件）の検索結果を分析した。**「ASTEROIDDB」「ASTEROID DB」の登録・出願はゼロ件**（完全クリア）。

「ASTEROID」単体・複合語でソフトウェア・データベース関連クラスに絞った結果は以下のとおり。

#### Class 009（コンピュータソフトウェア製品）

| Serial# | 商標 | 状態 | 内容 | 権利者 | 登録年 |
|---------|------|------|------|--------|--------|
| 85718972 | ASTEROID | **Dead** | ゲームアプリ | Dancing Penguins LLC | 2012 |
| 77916702 | ASTEROID-Z: FS5 | **Dead** | ゲームソフト | Flipside5 Inc. | 2010 |
| 86193145 | ASTEROID RUN-TIME ENVIRONMENT | **Dead** | 半導体チップ/ソフトウェア | Mediatek, Inc. | 2014 |
| **76026190** | **ROY MORGAN ASTEROID** | **Dead** | **Database management software（データベース管理ソフト）** | Roy Morgan International | **2002** |
| 90740089 | MISSION ASTEROID | **Live** | ダウンロード型ビデオゲーム | MMMera, Inc. | 2023 |
| 97751358 | ASTEROID | **Live** | ギター用サウンドエフェクトペダル | McBride, Cole | 2023 |

> 📌 **注目**: Serial #76026190「ROY MORGAN ASTEROID」は 2002 年登録の **Database management software（IC 009）** — まさに同分野での先例だが **Dead（失効）**。

#### Class 042（SaaS・ソフトウェアサービス）

| Serial# | 商標 | 状態 | 内容 | 権利者 | 登録日 |
|---------|------|------|------|--------|--------|
| 86650278 | ASTEROID | **Dead** | 臨床試験情報提供 | Acorda Therapeutics | 2015出願 |
| 88440171 | ASTEROID | **Dead** | 臨床試験情報提供 | Acorda Therapeutics | 2019出願 |
| 97625707 | ASTEROID | **Dead** | 臨床試験情報提供 | Acorda Therapeutics | 2022出願 |
| 90603364 | ASTEROID INSTITUTE | **Dead** | 物理・科学研究 | B612 Foundation | 2021出願 |
| **97705794** | **ASTEROID INSTITUTE** | **🟡 Live** | **ソフトウェア設計・開発・実装；小惑星追跡分野のオンライン検索可能データベース提供** | **B612 Foundation** | **2024-08-27** |
| 97160029 | ASTEROID THERAPEUTICS | **Dead** | がん治療薬開発 | Asteroid Therapeutics, Inc. | 2021出願 |
| 86851619 | LONELY ASTEROID STUDIOS | **Dead** | ビデオゲーム設計・開発 | Lonely Asteroid Studios | 2015出願 |

---

### 分析：個別エンティティのリスク評価

#### [最重要] ASTEROID INSTITUTE（B612 Foundation）— Class 042 Live 登録あり

| 項目 | 内容 |
|------|------|
| Serial # | **97705794** |
| 状態 | **Live（登録済み、2024年8月27日）** |
| 権利者 | B612 Foundation（カリフォルニア州非営利法人） |
| カバー範囲 | IC 042: "Design, development and implementation of software; Monitoring of asteroids; Scientific research in the field of asteroid discovery; **Providing an on-line searchable database** in the field of space object discovery and tracking" |
| ソース | https://tsdr.uspto.gov/#caseNumber=97705794 |

**分析**:
- "Design, development and implementation of software" および "Providing an on-line searchable database" という広い文言を含む
- ただし商品・サービスの説明に **"in the field of space object discovery and tracking"** という分野限定がある
- AsteroidDB（分散 KV ストア）とは **分野が明確に異なる**
- "ASTEROID INSTITUTE" vs "AsteroidDB" は語句構成も異なり、混同可能性は低い
- **リスク評価**: 🟢 低（分野限定により直接競合しない）

#### [修正] Asteroid (YC W25) — `asteroid.ai`

| 項目 | 内容 |
|------|------|
| 名称 | Asteroid |
| TESS 登録状況 | **出願・登録ともに確認されず**（2026年3月時点） |
| 事業活動開始 | 2024年末〜2025年初頭 |
| 製品 | AI ブラウザエージェント（バックオフィス業務自動化） |
| ソース | https://www.ycombinator.com/companies/asteroid |

**コモンロー商標権**: 2024年末から商用利用実績あり（X アカウント、Product Hunt、YC Demo Day）。ただし **USPTO 出願なし** = 登録商標権は未発生。

**重要な更新**: 前回評価では「最重要リスク」としたが、TESS 実検索で出願なしが確認された。コモンロー権は存在するが、**今すぐ「ASTEROIDDB」を出願すれば先願優位を確保できる**状況。

**分析**:
- 製品の本質が異なる（AI ブラウザ操作 vs. 分散データベース）
- 「ASTEROID」vs「ASTEROIDDB」— サフィックス「DB」による差別化
- **リスク評価**: 🟡 中（コモンロー権は存在するが、出願で防御可能）

#### AsteroidOS — スマートウォッチ向けオープンソース OS

- TESS に**登録・出願なし**（確認）
- 用途が全く異なる（スマートウォッチ OS vs. 分散データベース）
- **リスク評価**: 🟢 低

#### AsteroidDB (Kodular/Yusuf Cihan) — 廃止済み

- TESS に**登録・出願なし**（確認）
- GitHub リポジトリ削除済み、完全廃止
- **リスク評価**: 🟢 非常に低

---

### 総合リスク評価（TESS 実データ反映後・修正版）

| 競合 | リスクレベル | 主な懸念 |
|------|-------------|---------|
| **ASTEROID INSTITUTE (B612)** | 🟢 **低** | Live 登録だが分野が「小惑星追跡研究」に限定 |
| **Asteroid (YC W25)** | 🟡 **中** | コモンロー権あり。ただし出願未了 → 先願で防御可能 |
| **AsteroidOS** | 🟢 **低** | 登録なし、用途別 |
| **AsteroidDB (Kodular)** | 🟢 **非常に低** | 廃止・削除済み |
| **「ASTEROIDDB」の空き状況** | 🟢 **クリア** | 完全に空き。今すぐ出願可能 |

### 推奨アクション（TESS 実データ反映後・最終版）

1. **★★★ 今すぐ**: 知財弁護士と協議し「ASTEROIDDB」を **Class 009（ダウンロード可能ソフトウェア）および Class 042（SaaS）で USPTO 出願**。Asteroid YC W25 が出願前に先願を確保できる絶好の機会。
2. **★★★ 今すぐ**: ASTEROID INSTITUTE (B612、Serial #97705794) の登録範囲を弁護士に精査依頼 — "software design/development" の文言が広いため影響範囲を確認
3. **★★☆ 近期**: Asteroid (YC W25) の USPTO 出願動向を月次で監視（先に出願された場合の対応策を事前に検討）
4. **★☆☆ 継続**: `cargo license` を CI に組み込み依存ライセンスの継続監視

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
| 商標「AsteroidDB」 | 🟡 **要出願** | TESS 実検索で完全クリア。Asteroid YC W25 が出願前に先願を取るべき |
| 依存ライセンス | 🟢 **良好** | 全て寛容ライセンス |

---

## 8. 推奨アクションリスト（最終版・TESS 実データ反映）

| 優先度 | アクション | 担当 | 期限目安 |
|--------|-----------|------|---------|
| ★★★ | 知財弁護士と協議し「ASTEROIDDB」を Class 009/042 で **USPTO 出願**（先願確保） | 外部弁護士 | 今すぐ |
| ★★★ | ASTEROID INSTITUTE (B612, Serial #97705794) の登録範囲が AsteroidDB に及ぶか精査 | 外部弁護士 | 今すぐ |
| ★★☆ | Asteroid (YC W25) の USPTO 出願動向を月次監視（先に出願されたら即対応） | チーム | 継続 |
| ★★☆ | 名称継続の経営判断を確定（TESS はクリアなので継続が合理的） | 経営陣 | 近期 |
| ★☆☆ | `cargo license` を CI に組み込み依存ライセンスを継続監視 | エンジニア | 近期 |
| ★☆☆ | 継続特許の有無を USPTO で年次確認（BLS 関連） | チーム | 年次 |

---

*このドキュメントは 2026年3月8日時点の調査に基づきます。特許・商標の状況は変化するため、定期的な再調査を推奨します。*
