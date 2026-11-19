# EdgeCore と Rust 操作経路の照合状況

2026-10-17 に作成、2026-11-18 に更新。対象は `../aste-lean/AsteroidDB/EdgeCore.lean` と後続の
`EdgePolicy`、`EdgeRecovery`、`EdgeRouting`、`EdgeProgress`、
`EdgeCompletionReports`、`EdgeProgressConditions` に対応する新しい
`/api/operations/*` 経路。ここは確認済みの実装境界を示す作業用の索引であり、
Lean と Rust の包括的な refinement 証明ではない。

2026-11-18 の現行作業ツリーでは、Rust の `cargo fmt --check`、
`cargo clippy --all-targets -- -D warnings`、`cargo test --quiet` が成功した。
ライブラリテストは 1,988 件成功し、統合テスト群も成功（2 件 ignored）。
Lean は主エントリーポイントの `lake build` が 202 jobs で成功し、そこに
import されない配送・圧縮系列の末端モジュールを指定した build も 61 jobs で成功した。
これらの実行結果は各実装の検証であり、両者の包括的な精密化証明ではない。

| Lean 側の条件 | Rust の対応と確認した根拠 |
| --- | --- |
| Eventual はローカル receipt の永続化後に成功応答でき、Certified の確定成功にはさらに証明書が必要。応答の待機方法は証明書収集の義務を変えない | `EdgeRecovery.Durable.mayReply` は receipt の存在を共通条件とし、`.certified` を待つ応答にだけ確定条件を加える。`OperationRelay::submit_local` は inbox 保存後に受領票を返す。`submit_certified_http` は同じ保存後に状態を確認し、検証済み証明書があれば `200 certified`、期限時にまだなければ `202 pending` または `202 policy_unknown` を返す。`certificate_http_read_requires_exact_local_body_and_verified_proof` は未証明時の `wait_ms=0` で `202`、証明書到着後の同じ操作の `wait_ms=0` 再提出で即時 `200` を確認する。`202` 後も保存済み操作の証明書収集は続く |
| Certified 提出は対象キーの実際の Authority scope で受け付ける | 単一 scope の `submit_certified_http` も、保存前に live namespace の最長一致 Authority と placement policy が検証器の scope に完全一致するか確認する。trust pin と scope は同じ registry・namespace 読み取りガードで照合する。non-routed relay では両ガードを保存完了まで保持し、検査後の子 scope 追加と同一 scope 設定更新の競合を防ぐ。routed relay は自身の live policy lock と全体 digest 検査を保存まで保持する。以前は親 prefix だけで子 scope のキーを保存し、親では証明できない `202 pending` を返せた。既存の親子 scope を持つ HTTP 回帰テストで、子キーを `422` で保存前に拒否し、親キーの提出は継続して受けることを確認した |
| 通常読みは保持済みの全操作、Certified 読みは本体と一致する検証済み証拠だけを使う。削除が値に優先する | `src/api/operation_view.rs` の `project` と `certified_read_with_policy`、`src/network/operation_transport.rs` の `ordinary_read` と `certified_read`。証拠だけ先着した操作は Certified 読みに寄与しない。証明書保存後に再起動し、Add と Create の本体が順に届く回帰テストでは `Unknown → PolicyUnknown → Certified` を確認する |
| 値本体を保持する間は Certified read に必要な完全証明書を保持する | `compact_ledger_bodies_at_limit` は ledger GC 後も本体が残る操作を証拠圧縮候補から除く。receipt は全受理操作に存在し、生存する認証済み winner も持つため、それだけで完全証明書を compact 化すると read basis が失われる。実 inbox と証明書を用い、GC 前後で Certified read の値と basis が同じであることを回帰テストで確認した。回収済み本体の証拠は compact 証明書として status の判定に残す |
| 同じ操作集合の通常読みは到着順・重複に依存せず、Certified 読みの収束にはさらに同じ証明済み集合が必要 | `EdgeCoreConvergence` の条件に対し、Rust の `project` は削除を優先し、値は `(HLC, bytes)` の最大を選ぶ。`ordinary_projection_keeps_pending_update_and_delete` は逆順と重複、`equal_hlc_uses_value_tie_break` は同 HLC の byte 順、`pending_update_and_delete_do_not_hide_certified_value` は未証明の更新・削除が Certified 値を変えず、削除の証明後だけ結果が変わることを確認する。GC 後の同一 ID 再送は receipt による内容拘束を別途照合する |
| 削除は遅延更新や GC 後の再配送でも復活しない | `src/api/operation_gc.rs` は削除記録と未確定操作を回収候補から外す。`OperationRelay::submit` は削除済みキーへの新しいローカル提出を拒否し、削除前に保存した操作の完全一致再提出は root tombstone と ledger receipt まで照合して受け付ける。`OperationInbox::receive` は遅延した遠隔操作を受け取れる。射影は保持済みの削除を優先する。GC・root 回収・再起動・削除後の旧操作再提出を回帰テストで確認した |
| 作成根拠の未着は対象外と区別する | `OperationInbox::creation_policy_known` と公開 status/read。compact な Add が Create より先に届く場合は `PolicyUnknown`。`src/api/operation_view.rs` の `operation_status` も作成根拠の既知判定を受け取り、宣言の mode だけで `NotTargeted` とせず、回収済み本体の compact 証拠も照合する。`submit-certified` は保存後の待機期限に `202 policy_unknown` を返す |
| Authority は構造・権限・担当 range・作成時宣言を検証した対象操作だけを記録する | `src/authority/operation_admission.rs` の `admit_for_signing`、`src/authority/writer_auth.rs` の署名済み Create 検証、`src/authority/creation_policy.rs` の作成根拠照合。compact な後続操作は一致する Create 本体、receipt、または初回に完全な Create を検証した policy witness が Authority に届くまで署名対象にしない。値なしに圧縮した witness も再起動時と admission 時に作成者署名・宣言・commitment と固定した信頼設定を照合する。独立した witness の identity は inbox 直下の manifest に永続記録し、再起動時に full/compact の実体欠落や不一致を拒否する。初回 open が lock 同期後、空 manifest 作成前に中断した場合だけ、他の保存状態がないことを確認して初期化を完了する。policy witness を理由に Create 本体を Authority ledger へ追加しない |
| ローカル提出の writer と保存ノードは別の ID でよい | Lean の `EdgeCore.Step.submit` と `EdgeRecovery.DurableStep.submit` は namespace と抽象 admission を要求し、`EdgeRouting.LocalGuard` は保存ノードのキー担当を要求する。`EdgeCore.Trace.client_writer_submits_to_owner` は異なる ID の提出を構成する。Rust の `OperationRelay::submit` は担当キーを確認し、`WriterRegistry` による署名・namespace 権限の検証を inbox 受理時に行う。Lean の admission に具体的な Ed25519 検証を接続する証明はない |
| Certified は対象操作の検証可能な quorum 証拠を要する | `OperationMerkleCertificate::verify` と `CompactOperationCertificate::verify` は署名済み root、包含証明、固定 scope と keyset を検査する。共通の `verify_supports` は `n > 3f` と少なくとも `n−f` 人の異なる署名者を要求する。Authority ID と Ed25519 公開鍵はともに keyset 内で一意でなければならず、個別 support の受理と永続ストアの再起動時にも同じ条件を検査する |
| 再起動後も操作 ID の内容ロックを失わない | `OperationLedger::load_locked` は checkpoint がない場合も WAL sequence が 1 から連続することを要求する。初回 ACK より前に台帳ディレクトリ外の使用済み印を永続化し、その印があるのに内部 marker や WAL 全体が消えた状態では起動を拒否する。先頭 segment・全 segment・marker の消失を回帰テストで確認した |
| 初回初期化中のクラッシュは未受領の空状態として復旧できる | `OperationInbox` は lock 同期後に空 manifest 作成が中断した場合だけ、保存済み項目がないことを確認して manifest を完成させる。`OperationLedger` は外側の使用済み印がなく操作 frame もない場合に、lock、内部 marker、初回 WAL header の途中状態を復旧する。`OperationSupportStore` はログ MAGIC の途中書き込みや used marker の欠落・不完全書き込みを、空ログかつ他の保存項目がない場合だけ修復する。`OperationRootLog` は発行済み印がなくログ MAGIC の前方部分だけが残る場合に限って修復する。`OperationEvidenceStore` も初回 MAGIC の前方部分だけを修復し、`StaticOperationEvidenceStore` はログの lock 取得後に anchor と trust を再照合する。policy witness outbox は初回 pin 発行より前に未確定 temp を lock 下で掃除・同期し、再利用された PID と一時ファイル名の衝突を避ける。各ストアは受領・発行済み記録が失われた可能性のある状態を拒否する |
| GC 後の同一 ID 再送は内容を照合し、本体を復活させない | Lean の `Durable.receipts` は論理的な完全操作 ID 拘束を保持し、`Durable.ingest` は一致する再送を no-op にする。`EdgeCertifiedReplayTrace` は一致する再送後も本体が回収済みであることと、同じ ID・キーで異なる値の受理を拒否することを示す。Rust の `OperationHistory::outcome_for` と `OperationInbox::receive_with_role` は保存済み receipt の commitment と packet 全体を照合し、完全一致の再送だけを本体復活なしで受け入れる。`routed_owner_reports_new_sender_after_collected_value_and_restart` は別 sender からの同一操作を GC・再起動後に受けても本体を復活させず、その sender 宛ての完了通知を再起動後も保持することを確認する。Lean の完全な leaf は論理的な拘束を表し、Rust の固定長 commitment の衝突耐性は暗号前提である |
| 拒否したローカル提出を復旧用 root に残さない | `OperationInbox::receive_local_root` は作成 commitment、保存済み policy witness の packet identity、custody の競合作成を root fsync 前に確認する。compact witness と異なる packet を拒否しても root が残らず、再起動が成功する回帰テストで確認した |
| 未処理の署名・組立・公開仕事を永続入力から再発見する | `OperationAuthorityWorker::reconcile_routed` は root と signing intake を再走査し、admission ledger を同期してから root を発行する。`OperationCertificateState::record_support_blocking` は support を保存して quorum を組み立て、`reconcile_evidence_delivery_blocking` は保存済み証拠から配送を再登録する。公開 status/read はローカル本体または receipt と作成ポリシーを別途確認する |
| support の受領票は既存の完成証拠も反映する | 完成証明書が support より先に保存済みなら、新しい support 一件だけでは quorum を再組立できなくても `certificate_ready` を true にする。HTTP 受信とローカル Authority 受信は、同じ操作本体に一致する検証済みの完全または圧縮証明書を確認する |
| 故障 Authority の少数 support は正しい quorum を妨げない | Lean の `MerkleFlowState.known` は同じ subject に異なる payload や、同じ Authority からの別 root の個別 support を保持できる。Rust の `OperationSupportStore` も `(subject, commitment)` ごとに候補を分け、同一候補の同じ Authority に属する複数 proof を保存しながら、その Authority を quorum で 1 票だけ数える。正しい証明書と異なる少数 support は共存でき、support の値本体回収は一致する receipt の候補だけに行う。`SupportOutbox` は packet 全体の commitment で配送を区別し、別 root の証拠にも個別 ACK を得られる。少数候補の先着・再起動・正しい quorum の後着、証明書との受信順序、同じ Authority の別 root の中継を回帰テストで確認した |
| support・証明書の受領は後続の配送登録に依存しない | 検証済み support または証明書を永続化した後、outbox 登録だけが失敗しても受領票を返す。support 保存後の証明書保存が一時的な I/O 異常で失敗した場合は `certificate_ready: false` とし、再照合に組立を任せる。`reconcile_evidence_delivery_blocking` は保存済み support・証明書から不足した証拠と送信義務を再発見し、個別項目の組立・保存・登録に失敗しても後続項目を試してから最初のエラーを返す。poisoned evidence は固定 trust での復旧後だけ証明書配送に使う。証明書を配送可能な形式にできない場合は受領成功にしない。outbox と証明書ログの障害、サイズ上限で失敗する証明書の後続登録を回帰テストで確認した |
| 経路なしのローカル提出も保存し、送れる宛先だけ登録する | Lean の `DurableStep.submit` と `EdgeRouting.LocalGuard` は Authority 経路を要求しない。Rust の routed relay も署名・作成根拠・担当キーの admission 後に root と操作を永続化し、必須宛先やローカル経路がなくても受領票を返す。必要な経路がある宛先だけ outbox へ登録し、起動時・オンライン再照合も同じ部分進行を行う。経路や Authority 配置が足りない Enabled 操作は証拠がそろうまで Pending。既知の配置と署名済み宣言の矛盾、live Authority policy digest の変更は拒否する。未配送 Authority がいる間は root と値本体を回収しない |
| 継続して有効な配送仕事を後続の新規仕事が飢餓させない | 操作転送、support、certificate、policy witness、完了通知、retirement notice の再送 worker は共通の `RetryCursor` で周期開始時の仕事 ID を固定し、消えた仕事を飛ばして巡回する。新しい ID が数値順・辞書順のどこに増えても、有限周期を終えれば失敗した古い ID に戻る。ディスク・ネットワークの継続的な成功や worker の無期限実行は別の前提 |
| GC 後も作成時ポリシーと操作 ID の意味を保つ | ledger 本体の回収には作成根拠の確認を要求する。署名済み custody packet を消す前に `known-creation-policy/` の marker を fsync し、receipt と圧縮済み packet の同一性記録と照合して再起動後も判定を保つ。marker は元の Create commitment も保持し、Add の回収後に異なる Create witness を受けることを拒否する。marker のない圧縮状態は保守的に `PolicyUnknown` とし、同一 packet の再提出で根拠を再検証できれば marker を復元する。値の物理回収の詳細は [operation-value-retention.md](operation-value-retention.md) |
| 作成ノードが Authority でも値の回収条件を確認する | `OperationAuthorityWorker` は永続 root log の発行位置・subject・commitment と admitted ledger を照合してローカル発行証拠を inbox に渡す。起動時は relay の root tombstone 検査より先に全担当 scope の worker を開き、実行時は root 発行後に証拠を更新する。配送完了・証明済み receipt・この証拠が揃った root だけを回収し、root log が失われた回収済み root は起動時に拒否する |
| GC の `released` は ledger 本体を使う外部仕事が残らない条件 | Rust の `compact_certified_ledger_bodies` は exact な署名済み packet・検証済み証明書・作成ポリシーを確認し、root の各必須宛先には永続転送 descriptor、受信 transit には子 descriptor または終端の署名 intake を要求する。inbox と outbox の lock 下で判定し、未 ACK の再送には別の署名済み packet/outbox コピーを保持する。ledger 本体の receipt 化は配送完了を意味しない。本番呼び出しは保存済み証拠を固定 scope と trust に対して再検証し、GC メソッドの可視性も network 内に限定する。Lean の `EdgeCompaction` も外部 obligation の判定を抽象述語に分け、全 replica の ACK を要求するものではない。Rust の descriptor・packet 保持条件がこの述語を満たすことの機械的証明は未実施 |
| 証拠の受理時刻は現在 epoch を超えない | `EvidenceVerifier::verify_at` と `verify_compact_at` が未来の `accepted_epoch` を拒否し、個別取得・read basis・再起動 recovery・圧縮に同じ条件を適用する。高い epoch で開いた store を低い epoch で読み圧縮する回帰テストで確認した |
| 配送 ACK と最終完了通知は別の情報である | `OperationOutbox::record_receipt` と `record_completion` は別記録。子配送の ACK と完了情報が揃うまで親の完了通知を生成しない。終了境界は送信者単位の連続範囲だけを圧縮する |
| 親子転送の詳細回収後も完了根拠を再発見する | Lean `EdgeCompletionMetadata.collapse` は handoff を消す時に親の lookup を保存する。Rust は親子 link の identity を圧縮後の `.send` descriptor に残す。子の ACK と完了報告を fsync してから closure journal と closed index を永続化し、reopen は両者から閉鎖を復元する。受信側も retirement fence を fsync してから転送・終端記録の詳細を回収する |
| 最終宛先では永続受け渡し前に完了としない | `OperationRelay::receive_transfer` は転送の保存後、担当キーまたは固定宛先の Disabled Authority なら `mark_terminal`、Enabled Authority なら `mark_signing_intake` を保存する。`pending_completion_reports` はこれらの永続記録から通知を再発見する。純粋中継の transit packet は署名済み custody と値なし receipt を永続化するが application ledger へは入れず、再起動後も公開・内部の relay 読みで `Unknown` になる。非 routed の直接受信も担当外なら custody-only とし、ACK 前に direct marker を fsync する。再起動時はこの marker で ACK 済みの直接受信と未 ACK の転送途中を区別する。退役後は compact な転送記録を残して packet 本体を回収できる。routed/non-routed 起動時は application ledger の各本体に所有・origin・Authority intake のいずれかのローカル役割を要求し、役割のない中継本体は拒否する |
| 固定経路では各ホップで宛先への順位が下がる | `OperationRouteManifest::new` が各有効 prefix の隣接許可、継続先、順位低下を検査する。ローカル隣接先のアドレス・prefix 許可と Authority 最終受け取り担当も、起動時の新しい pin を作る前に `validate_routed_relay_configuration` で照合する。必要宛先への初回登録前にも経路を確認する。routed relay の HTTP は peer のリンク署名を検査する `relay_router_authenticated` を使う。Bearer のみの `relay_router` は routed relay を構築時に拒否し、単体受信ルーター経由の routed inbox 直接受信も拒否する |
| 転送 ID は送信者ごとに一つの packet と宛先に結び付く | Lean の `EdgeCustody.State.Identified` と `EdgeDeliveryInvariant.Metadata` に対し、Rust outbox は連続 descriptor と高水位を再起動時に検査し、ACK・完了記録が descriptor と一致しなければ拒否する。受信 inbox は `(sender, transfer_id)` の catalog を ACK 前に fsync し、別 packet・受信先での ID 再利用を ledger 受理前に拒否する。圧縮済み catalog は receipt と commitment を再照合し、退役 fence より前の再送には ACK を生成しない。Rust の連番は有限の `u64` だが、転送・support・certificate の outbox と Authority root 発行は上限前に `checked_add` で拒否する。ACK 後も compact descriptor を残し、再起動後に同じ ID を再利用しない。転送 identity と再起動後の fence に関する対象 Rust テスト 2 件が通過した |
| 操作台帳の WAL segment 番号を再利用しない | Lean の永続履歴には有限番号の wrap がない。Rust の WAL は `u64` なので、open 時の高水位に加え rotate 時も `checked_add` で上限を拒否する。以前は active segment が `u64::MAX` の場合、回転で番号 0 への wrap または panic が起こり得た。`rotate_rejects_sequence_overflow_without_reusing_zero` が上限での拒否を確認する |
| 再起動後も永続状態から未完了仕事を再発見する | `OperationRelay::new_with_mode` と `reconcile_durable_enrollments` が root と受信転送から配送義務を復元する。`OperationAuthorityWorker::reconcile` は永続 inbox を再走査し、発行済み root を再署名しない。support と certificate の配送キューは証拠ストアから再登録され、各 retry worker は未 ACK 項目を循環して再送する |
| 固定 Authority 受け取り担当を再起動時に変えない | routed relay はノード ID、Authority の最終受け取り scope、配置・Authority 定義を含む mounted policy snapshot とその digest を inbox に fsync し、再起動時に照合する。snapshot は operation assignments も含み、既存 root の配送義務には保存時の設定を使う。新規 Enabled 提出は、対象キーの実効 Authority・配置だけを live namespace と読み取りガード下で照合する。終端の署名受理も live の対象キー設定を確認する。起動時は全 certificate scope の正確な Authority 定義、鍵集合、静的信頼 digest、epoch 0、`n > 3f` を読み取り専用で検査し、作成ポリシー・リンク ID・経路・owned-prefix・Authority の既存 pin も検査してから欠けた pin を作る。設定不一致で起動が失敗しても空の旧 inbox に新しい pin を固定しない。未 pin の既存 custody 状態を別担当として読み直すことを拒否する。担当変更には明示的な移行処理が必要 |
| 実行時にも操作経路と背景仕事が接続される | `src/main.rs` は提出・読み取り、転送・完了・退役、証拠・状態確認のルートを登録し、配送・完了・support・certificate の再送と Authority 再走査を起動する。`OperationRelay::submit` は inbox 保存後に配送を登録し、登録失敗後も永続入力から再発見できる |
| operations モードの読み書きは操作台帳を使う | `src/main.rs` は writer-key manifest があると旧 Eventual/Certified ストアの WAL・snapshot を開かず、旧データ worker と checkpoint も止める。`router_with_operation_mode` は旧公開の読み書き・status・proof verify と旧内部データ同期を `409` で拒否する。制御プレーン、membership、epoch 処理は続ける。旧形式を操作 API へ暗黙に変換しない |
| 操作 ID のローカル保存は WAL 同期後に公開する | `OperationLedger::record_and_sync` は構造と既存 ID を検査して WAL に追記・同期し、その成功後に本体をメモリへ反映する。追記または同期の失敗後は台帳を復旧要状態にし、別内容や別 ID の追加と値本体の回収を拒否する。失敗注入テストでは追記前の失敗と、追記済みフレームの同期後に次セグメントの作成だけが失敗する場合を確認した。後者は再起動後に元の ID 内容を選び直す |
| 発行 root と受理済み support のログ欠落を空状態と誤認しない | `OperationRootLog` は初回 root 発行前に親ディレクトリの使用済み印を fsync し、発行後にログ全体が失われた再起動を拒否する。`OperationSupportStore` は初期化時に親ディレクトリへ使用済み印を fsync し、保存済み support ログまたはそのディレクトリの欠落を拒否する。単一 scope の使用済み印は起動時の scope 配置検査にも含め、設定から証拠領域を黙って落とさない |
| 証明書と Authority の作業領域を丸ごと失っても新規状態にしない | `StaticOperationEvidenceStore` は初回証明書保存前に親ディレクトリへ使用済み印を fsync し、証拠ディレクトリ・trust anchor・ログの欠落を拒否する。非空の証拠ログに anchor がない場合も起動を拒否する。`missing_or_damaged_anchor_cannot_mount_nonempty_evidence` と `missing_directory_after_certified_evidence_is_rejected` が証明済み状態を空として再開しないことを確認する。`OperationAuthorityWorker` は初期化した作業領域の親に使用済み印を fsync し、作業領域・admitted 台帳・root 領域・trust anchor の欠落を拒否する。単一 scope の外側に残る両方の印も配置検査で見つけ、scope を外した起動を拒否する |
| 複数 scope の親領域も消失を検出する | 各 scope の evidence と support を開いた後、`src/main.rs` は scope ID ごとの使用済み印をデータディレクトリ直下へ fsync する。再起動時は印と設定を照合し、保存済み scope の削除、または `operation_certificate_scopes` 親領域の欠落を拒否する。初回起動でまだ印がない scope は開いた後に固定する |
| 保存済み証拠の scope を再起動設定や live policy から落として公開状態を降格させない | `src/main.rs` の `validate_certificate_state_layout` は保存済みの旧形式・複数 scope 形式の証拠領域と設定を照合して起動を拒否する。同じ scope の鍵・故障数などの変更も `StaticOperationEvidenceStore::open` の trust anchor 検査で拒否する。証拠 trust digest は scope の実効 Authority・配置、鍵集合、所有範囲を固定する。子 scope の変更は digest を変えず、各証拠を読む・配送する・GC や退役に使う時に live selector で対象キーがまだ元 scope に属するか調べる。これにより兄弟キーの証明は失効しない。複数 scope の公開 read/status はキーに一致する全ての旧 scope の trust pin と live selector を同じ namespace 読み取りガードで評価し、証明器なしの応答もガードを保持して作る。検査後の設定変更で trust 不一致を Pending に降格させる競合を防ぐ。単一 scope の公開 read/status もキーが保存済み scope に一致すれば Unknown/NotTargeted/Pending を返す前に trust pin を検査する。無関係なキーや存在しない操作 ID はその scope の証拠を使わない。certificate/support 受信と `submit-certified` も元の prefix の trust pin を確認する |
| frontier 発行の永続 lease は署名時刻より厳密に大きい | Lean `ReportEmissionStep.cover` は物理時刻 `u64::MAX` を拒否する。Rust の `ReportClockFloor::cover` は従来この値でも飽和加算した lease `MAX` を保存して成功したため、再起動ごとに同じ HLC の異なる digest を署名できた。現在は厳密に大きい lease を作れない場合に報告を飛ばす。HLC の論理 overflow も物理時刻 `MAX` では飽和状態を保ち、次の呼び出しで古い時刻を再発行しない。最大値の lease から再起動した runner と、cover・HLC の境界テストで確認した |

Lean の `CertifiedReply.evidence` はノードが保持する全キーの認証済み操作を含む
論理的な証明対象で、wire 形式は定めていない。Rust の公開 Certified read は
内部で一致する保存済み本体と検証済み証明書から結果を計算し、応答の `basis` には
要求キーの完全な証明書だけを入れる。結果の射影は要求キー以外の操作に依存しない。
公開済み証拠の trust と routed relay の新規 root 配送は別に固定する。後者は
保存済み mounted policy snapshot と固定経路で既存 root の義務を保ち、新規
Enabled 提出では対象キーの実効設定だけを live 設定と比較する。無関係な
control-plane 更新後も提出と再起動を続けられる。対象キーの設定が変わった場合の
新規提出や担当・経路の移行処理は未実装である。
Authority の生成由来フラグと member の並び順は実効署名者集合を変えないため、
新規提出のキー単位照合と既存証拠の trust digest から除く。署名者 ID の重複は拒否し、
集合の変更は引き続き検出する。
操作経路の配置判定に使うのは対象キーの実効 prefix と `certified` 設定である。
replica 数、タグ、version、遅延・費用のヒントだけの更新では新規提出と既存の
証拠 trust を無効化しない。`certified` の無効化と子 prefix への切り替えは検出する。

公開 submit の JSON 上限は 2 MiB。routed 転送・完了通知、policy witness、
support・certificate の内部受信は 8 MiB とし、公開上限直下の操作に配送 envelope を
付けても内部の既定 2 MiB 上限で 413 にならないようにした。実 HTTP の境界テストで確認した。
non-routed 転送は 2 MiB のままなので、公開 submit で packet 自体が収まっても
転送 envelope が超過する候補を、root または inbox 保存前に 413 で拒否する。
routed では各ホップの転送と完了通知の JSON サイズを、永続 root 保存前と各受信 ACK 前に
検査する。再起動時も保存済み packet と transfer の配送可能性を再確認する。
完成 certificate の JSON が 8 MiB を超える場合は、各署名済み support を個別に
永続配送する。保存済みの完成証明書だけを持つノードも再起動時に個別配送を再登録し、
全隣接先から全 support の永続 ACK が届くまで証拠を圧縮しない。
初回 submit の永続 ACK より前と support の初回保存前に、固定 scope・keyset から
完成証明書と個別 packet の最大サイズを検査する。完全・圧縮証拠のレコードは
16 MiB、内部 HTTP packet は 8 MiB、Authority は 1024 件を上限とし、超過は拒否する。

この索引の各行はコードと関連テストを確認した範囲に限る。`../aste-lean` は
Lean 4.34.0 のツールチェーンを直接指定し、`LEAN_MAIN_USE_THREAD=0 LEAN_NUM_THREADS=1`
を設定して、現行ソースで `lake build`（202 jobs）と
`AsteroidDB/*.lean` 全 216 モジュールの明示ビルド（217 jobs）が成功した。
明示ビルドでは、既定ターゲット外の `EdgeCompactedDiscoveryTrace`、
`EdgeCompactedProgress`、`EdgeCompactedDeliveryTrace` に見つかった型エラーも修正した。
二つの trace は receipt がキーではなく完全な操作本体を保持する現行モデルに合わせた。
`EdgeCompactedProgress` の owner intake は `Durable.save` ではなく `Durable.ingest` を使い、
同一操作 ID の再受信では本体を復元せず exact receipt と保持済み transfer で
完了条件を示す。これは Rust の GC 後の同一 packet 再送と対応する。
現行ソースで Rust の `cargo test --quiet` はライブラリ全 1,988 件と
実行ファイル・統合テスト全体が通過した（統合テスト 2 件は ignored）。
配送完了と policy-only witness の再監査でも、Lean の前提内で新たな
到達可能差分は見つからなかった。
`cargo fmt --check`、
`cargo clippy --all-targets -- -D warnings`、`git diff --check` も通過した。
操作の Authority 証拠・worker・read 射影は native runtime の network 型に依存するため、
`native-runtime` でまとめて有効化する。`cargo test --lib --no-default-features` の
1,120 件と Lean 側の OR-Map 再現例が core-only 構成で通ることを確認し、
この構成のテストビルドを CI に追加した。
その後、operations API のローカル submit、Certified submit、status、通常・Certified
read を `Durable.mayReply/status` と `EdgePolicy` の結果に再照合し、新たな到達可能差分は
見つからなかった。`elan` 経由の起動が停止したため、上記のビルドは
`/home/coder/.elan/toolchains/leanprover--lean4---v4.34.0/bin/lake` を直接使った。
`EdgeLiveCompaction.reclaimDominated` と `EdgeRecovery.collectLive` も現行の
`operation_gc.rs` と本番の回収呼び出しに照合し、証明済み winner の保持、
未証明・未解放操作の保持、`(HLC, bytes)` による支配判定に到達可能な差分は
見つからなかった。回収候補 2 件と、回収後の再提出・再起動に関する 2 件の
Rust テストが通過した。対象 Lean モジュールも上記の明示ビルドに含めて確認した。
読み取り・状態判定について、削除優先、receipt だけが残る証明済み操作、
作成根拠未着、policy-only witness、担当外の公開 API 境界を再確認した。
証明経路も admission 台帳、root 発行、support の永続配送、quorum 証明書、
再起動後の status まで照合し、新たな到達可能差分は見つからなかった。
`EdgeCompletionReports` の ACK・完了通知・退役 fence と Rust の routed 転送記録も
再照合した。ACK と完了通知は別々に永続化され、両方が揃ってから送信記録を閉じる。
受信側は退役 fence の保存後に転送詳細を回収する。確認した範囲では新たな
到達可能差分は見つからなかった。
support 配送から quorum 確定までの対象 Rust テスト 1 件が通過した。
作成束縛・受理条件と操作本体回収・再起動時の保持条件を現行 Rust と再照合した範囲では、
新たな到達可能差分は見つからなかった。
実ネットワークの成功、
無期限の安定稼働、ディスク媒体の存続、経路・担当・隣接設定の変更は Lean 側でも別の前提または範囲外である。
再発見ループは保存済み root・転送・support・証明書を再走査する。outbox への
永続的な I/O 失敗が先頭 root に続くと、後続 root の登録もその tick では試さない。
証拠 store の継続的な障害も support の再登録を止め得る。保存済み仕事は残り、
障害解消後の再照合で再開するため、これは Lean の正常な保存進行前提下での
失われた義務ではない。障害中に独立した仕事を進める保証は別途検討する。
現在の Rust テスト成功だけで無限実行の公平性や包括的な refinement を証明したとは扱わない。
Lean の操作値は `List UInt8`、Rust の新操作値は `Vec<u8>` で、どちらも任意バイト列を
辞書順で比較する。同一 HLC では `[1] < [1,0] < [2]` の順に勝者を選び、Rust の GC も
同じ順序で支配された値を判定する。Lean の `OperationValue.lean` の例と Rust の
`operation_view.rs` のテストでこの境界を確認した。空バイト列と非 UTF-8 バイト列も
`arbitrary_value_bytes_survive_signed_transport_and_restart` で署名済み受信・再起動後の
通常読み・同一 packet 再受信まで確認した。値の符号化・署名バイト列まで含む
機械的な Lean/Rust refinement は証明していない。
`operation_gc.rs` の `small_state_gc_candidates_preserve_both_read_projections` は、
削除と同一 HLC のバイト列を含む 64 通りの三操作、確定・解放の各 8 通りを列挙し、
削除キーと支配済み値の回収候補について通常・Certified の両読取結果が変わらないことを確認する。
署名対象を再監査した範囲では、`BoundOperation::canonical_bytes` は可変長項目を
長さ付きで、数値を固定幅で符号化し、subject・操作・作成宣言を含む。
`SignedCreation` の署名は Create commitment と policy digest を束縛し、
操作署名はその作成証拠にも束縛する。異なる有効操作が同じ署名対象になる
具体例は見つかっていない。`SignedCreation.version` は署名対象外だが、現行の
検証器は version 3 だけを受け入れる。Lean の `CrdtOperationCommitment` も
バイト符号化の単射性とハッシュの耐衝突性を別の義務として残している。
HLC の node ID は Lean で `Nat`、Rust で文字列の辞書順なので、精密化には
デプロイで使う有限の node ID を辞書順に並べ、その 1 始まりの順位を `Nat` に対応させる必要がある。
`0` は Lean の空 HLC の番兵値として予約する。
文字列を十進整数として読むだけでは `"10" < "2"` の Rust 順序が逆転する。
`EdgeRecovery.Durable.status` は作成ポリシーを扱わない下層の状態であり、
作成根拠が未着の公開状態との比較には `EdgePolicy.State.operationStatus` を使う。
`EdgePolicy.State.certifiedResult` は認証済み policy だけが届き操作本体のないキーにも
`NotTargeted` を返せる。現行 Rust は Create 本体または一致する receipt から mode を学び、
`OperationInbox::receive_policy_witness` は完全な署名済み Create を最初に検証して
ledger 外に永続化し、Disabled の本体未着キーを `NotTargeted` として読める。
必要な policy 配送の ACK 後は、署名済み作成根拠と commitment を値なし記録へ圧縮する。
policy witness だけが残る未 pin inbox も既存の作成根拠として扱い、後から別の作成時
policy manifest を固定して再解釈することを拒否する。
`operation_policy_transport.rs` は専用のリンク認証付き hop と固定宛先を扱い、
`operation_policy_outbox.rs` は未 ACK の完全な packet と ACK を fsync して再起動後も再送し、
ACK 済みの送信記録は値なし identity へ原子的に置換する。
outbox の rename・fsync が不確実な結果を返した場合は送信を止め、同じディレクトリ
lock を保持した再走査で pin、送信記録、ACK を検証してから処理を再開する。
policy-only 配送を有効化した inbox は通常の操作経路で受けた完全な署名済み Create も
ACK 前に独立した policy witness として fsync する。再起動時の worker は保持済み witness
から必要宛先への送信を再登録するため、通常 custody の本体回収後も配送仕事を失わない。
中継で witness を永続保存した後は、後続の送信登録や古い送信記録の圧縮が失敗しても
受領 ACK を返し、同じ再照合 worker が残りの仕事を復元する。
owner に必須宛先への経路がない場合は ACK 未取得の witness 本文を圧縮せず、後から
配送できる余地を保つ。終端ノードは自身が宛先であれば圧縮できる。
Policy-only 配送先は操作本体の配送先と独立に固定する。実ネットワーク下の進行性と
包括的な Lean/Rust refinement は未証明。
二つの必須宛先を使う回帰テストで、片方の ACK 後も送信元が完全な witness を保持し、
再起動後にもう片方へ配送してから値を圧縮することと、両宛先の本体なし Disabled 読みを確認した。
Lean の `State.certifiedResult` は各ノードの内部状態を直接観測できるため、policy-only
Disabled を受け取った中継ノードでも `NotTargeted` になる。Rust の
`OperationInbox::certified_read` と公開 Certified 読みは、完全な署名済み Create の
policy witness を受け取った担当外ノードでも `NotTargeted` を返す。担当外でも同じ
Disabled 作成根拠と操作本体または receipt を持つ場合は、操作 ID の status も
複数 certificate scope の公開 router を含め `NotTarget` を返す。この判定でも一致する
保存済み scope の trust pin を先に確認する。担当外の Enabled と未確認キーは公開 API
では `Unknown` に留める。
通常読みの owned-prefix 境界も維持する。
この公開範囲全体について無条件の観測同値は主張しない。
Lean の `EdgePolicy.CertifiedReply.evidence.exactBasis` は照会キー以外の確定操作も
含む論理的な証拠対象で、wire 形式の指定ではない。Rust の内部
`CertifiedOperationRead.basis` も呼び出し側が渡した検証済み操作のうち保持本体と
一致するものをキーで絞らず残し、値の射影時だけ照会キーを選ぶ。公開 Certified read は
照会キーの証拠だけを返し、選択した scope 以外の証拠も集めない。この公開証拠集合の
同一性は主張しない。Lean の `EdgeRecovery.DurableStep.certify` は Enabled ポリシーを
既知とする条件を持ち、以後その知識を失わないため、Rust の status が証拠より先に
作成ポリシーを確認しても、到達可能な状態では結果が分かれない。
Rust は owner が本体を receipt 化した後も、同じ操作の別 transfer を厳密な
receipt と新たに永続化した署名済み packet で完了できる。
Lean の `EdgeEndpoint` に、owner が正確な操作本体を保持していた時だけ作れる
`ownerIntake` の履歴を追加した。`Ready(.operation)` は本体または Authority inbox
に加え、この履歴と現在保持する同一 packet の転送が揃えば成立する。
`Steps.owner_intake_origin` は履歴の出自を、
`repeat_owner_complete_after_collection` は本体回収後の再転送完了条件を示す。
Rust の回帰テストは旧転送と ledger 本体を回収してから別 sender の同一 packet を
受け、ledger 本体を復元せず terminal を永続化できることを確認する。
`EdgeCertifiedReplayTrace` は空の runtime から到達できる証明済み状態を起点とし、
owner の受理履歴を記録して `DurableStep.collectLive` を実行した後、保持済みの
転送元 packet から新しい転送を作り、owner が同じ本体を台帳へ戻さず完了する
`EdgeEndpoint.Steps` を示す。owner の crash/restart を GC と再転送の間に挟む
別の履歴でも、受理履歴と receipt が残り、同じ完了条件が成立する。
証明済みの起点までの runtime 履歴と、その後の
Endpoint 履歴は別の定理で接続している。旧転送の退役まで含む単一履歴ではない。
認証・配送・永続化の境界は [operation-policy-only.md](operation-policy-only.md) に整理した。
経路なし root は起動時・オンライン再照合でも保持し、送れる宛先だけを登録する。
後から経路を修復する場合、outbox が転送 ID を一度も発行せず、inbox に受信転送・
退役記録・回収済み root がないと確認できる時だけ routing pin を置換できる。
既に一部へ転送した場合、または neighbor/link manifest や Authority 配置を変更する場合の
pin 移行は未実装で、その固定設定では残る宛先への配送・Certified 到達を保証しない。
保存済み root と未配送義務は回収せず Pending を維持する。
同一操作 ID の内容一意性と分断をまたぐ競合作成の排除は
`EdgeCreation.Context` のデプロイ全体の前提である。Rust の署名検証と
ローカル競合検査だけから、この全体前提を証明したとは扱わない。
support ストアは故障 Authority の個別 support を扱うため、同じ操作 ID の
複数内容を commitment 別に保持する。完成証明書ストアは検証済み quorum の
内容を操作 ID ごとに固定する。少数の相反する support と正しい証明書は
共存でき、両ストアの保存済み本体が常に一致するとは要求しない。
署名者が同じ ID に異なる Create を発行する入力は Lean の `sameIdContent`
前提から外れ、その入力でローカル operation 台帳と各ノードの読み値を
同一内容に保つ保証は現状ない。
`OperationAdmission.valid` の `eventAllowed` は抽象的な受理述語であり、counter の
正数・連番・単調増加を仕様として固定しない。Rust は署名とローカルの同一 subject
内容一致を検査し、本体回収後も receipt を残して ID 再使用を拘束する。
たとえば同じキーを所有する二ノードが分断中に異なる writer の署名済み Create を
それぞれ受理すると、再接続後の相互配送はローカルの競合検査で拒否され、読み値が
分かれたままになる。これは Lean が `noCompetingCreates` で除外する入力列であり、
現行 Rust には全ノード共通の作成担当や一意発行を検証する仕組みがない。
Lean はこの条件を認証済み Create の入力前提として置き、全 Create の発行を待たせる
プロトコルを定義していない。Eventual の提出はローカル保存後に応答し、Certified の
提出は同じ保存後に操作証明書を待つ。Rust の `submit` と `submit-certified` もこの区別に
沿う。分断をまたぐ作成一意性の実行時保証を追加するなら、作成担当の永続ロックと
検証可能な発行証拠が必要で、Eventual の即時受理とは別の仕様判断になる。
Rust の `submit` はローカル inbox の保存が成功した後、配送 outbox の登録が
失敗しても受領票を返す。保存済み root または操作から、起動時と稼働中の
再照合 worker が配送義務を再登録する。保存中の失敗は HTTP エラーになり得るが、
途中で root intent だけが永続化された場合は復旧後に操作が現れる可能性がある。
`direct_submit_returns_receipt_after_outbox_enrollment_failure` と
`completion_tick_enrolls_durable_root_and_child_after_outbox_failure` を現行ソースで
再実行し、直接配送と routed 配送の保存後応答・後続登録を確認した。
遠隔からの直接受信と routed 転送も、操作本体・転送 ID・必要な終端記録の
永続保存後は、後続 outbox 登録だけの失敗で ACK を取り消さない。保存済み
受信記録から同じ再照合 worker が未登録の送信義務を復元する。
Lean の `Durable.mayReply` は成功応答を許す条件であり、ディスク I/O 失敗時の
HTTP 応答を定義していない。
Lean の `EdgeCreation.Context` は全ノードに共通の認証済み作成ポリシーも前提とする。
Rust の `CreationPolicyManifest` は各ノードのローカル pin なので、例えば一方だけが
Enabled override を許す設定なら、同じ署名済み Create を一方は受理し、他方は拒否する。
署名済み Create は manifest digest を creator 署名と操作署名に結び付け、受信時と
値なし作成根拠の再検証でローカル pin との完全一致を要求する。設定の異なるノードは
保存・ACK 前に拒否する。クラスタ全体への同一 manifest 配布はなお運用前提であり、
設定不一致の構成での配送・署名進行性は Lean の共有ポリシー前提からは導けない。

## OR-Map と残る検証

- Lean の `DotState.WellFormed` が要求する live と removed の非交差に合わせ、
  Rust の `OrSet` と `OrMap` は受信時に同一 dot の矛盾した配置を拒否する。
  dot の counter `0` と、採番カウンターが保持 dot や GC floor を下回る payload も拒否し、
  floor 以下でなお live な dot は受理する。JSON と bincode の受信テストで確認した。
  payload 単体の検査だけでは、手元の別要素・別キーにある同じ dot との衝突を
  見つけられない。`Store::merge_value` と delta merge は変更前に双方の live dot を
  照合し、OR-Set の別要素割当、OR-Map の別キー割当と同一 dot の異なる値・HLC を拒否する。
  旧 Certified 複製の満杯時に使う「変化しない再送」の早期終了でも同じ検査を先に行う。
  衝突は不変 dot の恒久的な拒否として同期側へ返し、保存値や待機中の証明仕事を変えない。
  未観測 dot の真の所有者まではローカル状態から判定できず、Lean の共有 owner registry と
  home writer の受理条件は引き続きデプロイ側の前提である。
- `OrSet` / `OrMap` の受信 payload は `compaction_floor ≤ counters` を検査するが、
  floor が正当な GC 履歴から来たことまでは証明しない。空の live 集合と高い floor は
  形式検査を通り、旧 `/api/internal/sync` の merge で既存の live dot を消せる。
  Lean の単純な `DotState.Step.receive` も `WellFormed` な任意 payload を許す一方、
  `OrMapOwnerNetwork` の証明は実際の送信状態に由来する in-flight payload を要求する。
  正当な GC 後の floor も同じ形なので、形だけで拒否することはできない。
  旧 sync は送信元と floor の来歴を暗号学的に結び付けておらず、共有 Bearer token
  を持つ peer を信頼する運用前提が残る。operations モードは旧 sync ルートを拒否し、
  新しい署名付き操作経路の値・証拠台帳とは分離する。
- Lean の `Nat` と違い Rust の dot counter は `u64` なので、GC の floor 走査は
  `u64::MAX` の直後を内部の `u128` カーソルで表す。保存する floor は `u64` のままで、
  最大 counter の tombstone を回収した後の再 sweep も panic せず同じ floor を保つ。
  保存済み mark の sweep では、後続の live dot が増えても floor を mark 時点の
  writer counter 以下に制限する。通常の連続走査と Stage 2 の hole jump は独立で、
  本番では後者を有効にしない。OR-Set と OR-Map の mark 後書き込みを回帰テストで確認した。
- `Stage2HoleJump` の inbound 証拠は、実際の replica 集合を漏れなく含むことまで
  証明していない。Rust の peer は ping 失敗で registry から除外され得るため、
  mark 時にいた peer が live dot を保持したまま除外されると、現在の registry
  だけを見る旧 gate は空集合として通り、hole を跨ぐ floor が再参加時の値を消し得る。
  このため Rust の runtime は Stage 2 hole-jump を実行せず、
  `ASTEROIDB_GC_HOLE_JUMP=1` は起動時に拒否する。公開 GC API からも
  hole-jump の bool・ceiling 指定を外す。連続して決定済みの dot だけを
  通る通常の floor 走査は維持し、Stage 2 の走査ロジックは内部テストで確認する。
- GC の peer gate は成功した push の snapshot 時刻を NodeId とアドレスに結び付ける。
  同じアドレスに別 NodeId が後から登録されても旧 ACK は使わず、同じアドレスを
  複数 ID が同時に使う場合も gate を閉じる。`run_gc` は membership の世代を
  判定前後で照合し、sweep が終わるまで registry lock を保持するため、判定直後の
  peer 入替えも見逃さない。旧 peer への ACK 後に同一アドレスへ新 peer を登録し、
  次の sync より先に GC を実行する回帰テストで確認した。HTTP 応答が設定上の
  NodeId 本人から来ることは、アドレス設定の信頼前提である。
  壁時計が戻ると mark 前の ACK 時刻が mark より大きく見えるため、初回 mark と
  sweep 後の新 mark を store・Certified の lock 下で作るたびに、過去の peer ACK と
  Authority frontier の進行受領時刻を失効させる。Certified lock が取れなければ
  新しい mark は作らず再試行する。新 mark 後の同期・frontier 進行だけが
  GC gate の新しい証拠になる。旧 ACK を `u64::MAX` とした回帰テストでも、
  新 mark 後の sweep が閉じることを確認した。
  `push_full_state_to_peer` は送信 entry が空なら HTTP を送らず成功を返すが、通常の
  Eventual/GC 遷移では mark 対象の deferred dot を持つ OR-Set/OR-Map entry は
  sweep 後も floor とともに store に残る。production の書き込み・GC 経路は
  `Store::delete` でその entry を物理削除しないため、空の full push を使って
  未配送の mark 対象を回収する到達列はない。
- GC の Authority gate は現在の名簿の各 NodeId に対して同じ range・policy version の
  frontier と mark 後のローカル受理時刻を要求する。同じ policy version のまま
  Authority 名簿を入れ替えても、残存する旧メンバーの報告を新メンバーの代わりに
  数えない。さらに `FullGcStep.sweep` の認証済み floor 条件に合わせ、現行名簿の
  quorum が同じ正規化 checkpoint に署名し、その署名対象の full frontier が受理済みで、
  checkpoint が mark 時刻以上の場合だけ sweep する。未署名の frontier だけでは
  gate を開かず、同じ報告への後着署名で開ける回帰テストで確認した。
  `FullGcStep.sweep` は署名済み checkpoint の発行時刻が mark 後であることを
  要求しない。mark 前に署名された checkpoint でも、正規化時刻が mark 以上で、
  各 Authority の frontier 進行受領が mark 後なら、この抽象条件を満たす。
  この frontier 進行は個々の削除の取り込みを意味しない。Lean の
  `GcEvidence.frontierPastMark` も Authority ごとの時刻条件で、削除 ID を含まない。
  `FullFrontierGcTrace.gcEvidenceFromRuntime` は frontier の物理時刻と mark を比較し、
  `HlcGcJointHistory` は sweep 後も古い値を持つ peer が残る実行を許す。Rust の
  `FrontierReporter::report_frontiers_at` も各削除の取り込みを調べずに時刻を発行する。
  したがって削除ごとの Authority 取り込みは現行 Lean 仕様の要件ではなく、
  それを保証する場合は Lean と Rust の双方で条件を追加する必要がある。
  名簿変更後の `run_gc` が新メンバーの報告まで閉じる回帰テストでも確認した。
  gate 判定時の namespace version を sweep 直前に再照合し、sweep 中は namespace の
  read lock を保持する。途中で名簿・policy が変わった場合はその pass の gate を閉じる。
  HLC timestamp の圧縮も現行名簿の ID だけから majority を作り、保存済み checkpoint
  と現在の policy version の一致を要求する。`CertifiedPrune` の条件に合わせ、
  現行名簿の署名付き quorum が受理済み full frontier と完全一致し、その正規化
  checkpoint が削除対象の HLC 以上でなければ圧縮しない。未署名 frontier や、
  同一 checkpoint の別の full frontier の署名を根拠にしない回帰テストで確認した。
  高頻度書き込みで次の checkpoint 作成条件を満たしても、未証明の既存 checkpoint を
  先に上書きしない。署名済み floor が届いた後に既存 checkpoint の時刻を圧縮し、
  それから新しい checkpoint を作る。未署名中の保持と後着署名後の圧縮を回帰テストで確認した。
  圧縮の namespace snapshot も store を
  変更する直前に版を再照合し、圧縮中は read lock を保持する。旧メンバーの報告や旧版の
  checkpoint では timestamp を prune しない回帰テストで確認した。
  親 `user/` と子 `user/private/` の Authority scope が重なる場合は、各 key を最長一致の
  Authority と placement policy が同じ scope を指すときだけその scope に集計する。
  timestamp の削除にも同じ判定を適用し、親の署名済み floor だけで子 key を削らない。
  親 checkpoint の削除対象と子への書き込み集計をそれぞれ回帰テストで確認した。
  `NodeRunner::check_compaction` に親の署名済み floor だけを与えるテストでも、
  親キーの timestamp が削られ、子キーの timestamp が残ることを確認した。
  `Store` の prefix 範囲走査は Unicode scalar を繰り上げて排他的な上限を作る。
  以前の UTF-8 最終バイト増分と lossy 変換は、`ÿ` の範囲に隣接する `Ā` のキーを
  混入させ、prefix 読みと timestamp 圧縮の両方で誤った対象を選べた。
  両経路を使う Unicode 隣接キーの回帰テストで修正を確認した。
  `HlcIndexedStore` と `PushResponse` の同期条件も照合した。Rust は差分送信の
  transport・HTTP・decode・全 key 応答を確認し、圧縮 floor より古い baseline に
  未配送分が残る場合は `push_prune_debt` を保持する。差分送信後も digest または
  full-state push で全状態を確認してから GC 用 ACK を進める。`internal_sync` は
  応答対象の最後の WAL 位置を同期して `durable=true` を返す。digest 応答も
  root・bucket を読んだ同じ store lock 内で WAL 位置を捕らえ、その同期後に
  `durable=true` を返す。送信側は HTTP・復号・キー別 merge と全 batch の
  `durable` を確認してから push baseline と GC 用 ACK を進める。`Interval` では
  内部応答が同期を待ち、`Off` または永続化無効では `durable=false` として
  GC gate を閉じる。digest の未永続 root match が誤った ACK を作る競合を防ぐ。
  HTTP 回帰テストは未同期 digest の応答保留、interval の内部同期、off の
  `durable=false` を確認する。runner テストは非永続応答で ACK を保留する。
  digest 統合テストの GC 証拠を扱う peer は実 WAL を使い、32 件が通過した。
  digest 経路は root 一致なら同じ正規化状態を確認し、不一致なら送信側にキーがある
  bucket を配送する。照合時点から送信時点に値が変わった場合は GC 用 ACK を進めない。
  応答元が設定上の peer 本人であることは外部の通信信頼前提として残る。
- GC mark の保持期間は `mark_ms + retention <= now_ms` を `u128` で判定する。
  保持期間 0 でも壁時計が mark より前へ戻った場合は sweep しない。
  `u64` ミリ秒を超える設定の切り詰めも許さない。
- `run_gc` の floor 進行は次の checkpoint または同一キーの WAL 書き込みまで
  揮発的である。その前のクラッシュでは元の tombstone と低い floor に戻る。
  この後退は削除を失わず、peer の高い floor の再配送や再 sweep で回復する。
  sweep 前後の snapshot と peer merge は `compaction_floor_regression_recovers_from_snapshot_and_peer_merge`
  で確認した。Lean の `SystemModel` はこのクラッシュ・再起動をモデル化していない。
- 同じアドレスの peer が別の NodeId に入れ替わると、旧 push frontier を使った
  空 delta scan から新 NodeId の GC 用 ACK を記録できた。新 peer が古い live dot を
  持つ場合に削除の配送証拠を偽るため、`NodeRunner` は各同期周期の前に
  アドレスと NodeId の対応を照合し、入れ替わり時には push・pull・digest・
  Certified の各 cursor と ACK・backoff を破棄する。
  `replacement_peer_gets_full_tombstone_before_new_gc_ack` は、同じアドレスの新 peer が
  完全状態の削除を受け取ってから新しい ACK を得ることを実 HTTP で確認する。
  `certified_lane_restarts_cursor_for_new_node_at_same_address` は、新 peer の確定データが
  旧 cursor より前にあっても最初の Certified pull で取得することを確認する。
  同時に複数の NodeId が一つのアドレスを指す設定はアドレス単位の cursor では表せない。
  `PeerRegistry` は構築・追加・アドレス更新・保存済み JSON の読み込みで重複を拒否し、
  参加通知は入力エラー、gossip と ping の衝突は無視する。
- 旧 `OrMap` の単一 LWW register は削除済み dot の値を残して merge 順序依存を
  生じさせた。Lean の `DotValueCore` に合わせ、Rust の `OrMap` も live dot ごとに
  不変の値と HLC を保持し、読み取りは live dot のみから計算する。旧保存形式との
  互換性は実験段階の要件ではない。CRDT の digest は新しい dot/value 対応を含める。
  Lean の `OrMapValue` は比較用の旧単一 register モデルで、現在の Rust の値選択とは
  一致しない。例えば高 HLC の dot だけが削除され低 HLC の dot が生き残ると、
  旧モデルは高 HLC の値を残し得るが、Rust と `DotValueCore` は生存 dot の値を返す。
  旧 Eventual の OR-Set/OR-Map は、WAL に未永続の dot を同期先へ送ってから元ノードが
  落ちると、復旧した counter で同じ dot を再発行し得た。永続 WAL を開くたびに
  incarnation high-water を独立した sidecar に fsync し、ローカル dot の writer ID に
  論理ノード ID と起動世代を単射に符号化する。WAL が失われても peer が保持した旧 dot と
  復旧後の新 dot は衝突しない。HLC とセッショントークンの論理ノード ID は変えない。
  sidecar の世代値は CRC 付きで読み直し、破損を検出したら起動を拒否する。
  起動世代は WAL segment 番号とは独立に進める。segment 作成が失敗しても起動世代は
  消費し、次の segment は同じ連続番号で再試行するため操作台帳の WAL 連番を壊さない。
  WAL を無効にしたモードには永続化済み起動世代がなく、Lean の durable-only 公開条件の
  保証対象には含めない。
  旧 Eventual API のローカル counter、set、map、register 更新は型・存在検査後、
  HLC 採番を済ませてから store を変更する。HLC overflow のエラー時に未保存の
  値や空 key を残さず、再試行で一回だけ更新することを回帰テストで確認した。
  WAL が有効な場合はさらに候補値を先に作り、その redo record の追記が成功してから
  store の値・変更時刻・session frontier を公開する。追記失敗後の GET、同期、checkpoint
  が未永続のローカル値を観測しない。新規 register と既存 set の削除で失敗と再試行を
  回帰テストで確認した。
  `CrdtValue::Map` の旧 HTTP 経路は UTF-8 文字列に限るが、文字列の辞書順は
  UTF-8 バイト列への埋め込みで Lean の `OperationValue` 順序を保つ。
  任意バイト列の操作値は新しい `/api/operations/*` 経路で扱う。
  未来 HLC の遠隔値を保持した後のローカル更新は、HLC 順の delta scan から
  隠れ得る。`NodeRunner::run_sync` は delta push を最適化として扱い、digest による
  全状態確認か full push の成功後だけ push frontier と GC 用 ACK を進める。
  `OrMap::delta_since` が counter だけ・floor だけの変化で `None` を返しても、
  `Store::delta_entries_since` は対象キーの HLC が frontier より新しければ値全体を送る。
  HLC scan 自体が空なら digest または full push で照合する。OrMap の digest は
  counter と floor を含み、prune gap がある場合は全状態 push を要求する。
  digest 無効時は full push で確認する。遠隔 clock skew と低 HLC の OR-Map dot を
  含む回帰テストで確認した。
  `OrSet::delta_from` と `OrMap::delta_from` は GC floor が変わった場合、または
  旧 live dot が現状態の live と tombstone の両方から消えた場合、完全な live 集合と
  floor を返す。後者は、既存 floor に覆われた live dot を削除して tombstone を
  圧縮し、floor の値が変わらないときにも必要。受信側に残る古い live dot を消せる。
  それ以外の場合だけ floor を持たない部分 delta を返す。これらの `delta_from` は
  現在の本番同期経路では使っていない。部分 delta に新 tombstone または変更キーの
  既存 dot が含まれ、writer counter 自体は増えていない場合、以前は counter を省き、
  受信側の Deserialize が合法な差分を拒否していた。両 `delta_from` は送る全 dot の
  writer counter を同梱する。OR-Set と OR-Map の回帰テストは、修正前の受信拒否を
  再現し、修正後の JSON round-trip と merge 後の削除・値を確認した。
- dot counter が `u64::MAX` に達した場合、公開 `EventualApi` の Set 追加と
  Map 設定は `try_add` / `try_set` の checked addition を通り、既存値を
  変えずに `DotCounterExhausted` を返す。HTTP では `409` として扱う。
  内部の `add` / `set` ラッパーは上限で panic するが、公開書き込み経路からは
  呼ばれない。両 CRDT の上限テストと Map の公開 API テストで dot の再使用を
  防ぐことを確認した。Rust の有限 counter 上限は Lean の `Nat` との差として
  残るが、到達時に拒否するため既存 dot と同一 identity の書き込みは受理しない。
- full-state push の受信で `merge_remote` が CRDT をメモリへ反映した後、HLC の
  `now()` が overflow すると WAL に値本体がまだない。受信キーに
  merge-failed 印を付けて WAL に記録し、同じ値の再送を RR gate で省略せず `UpsertApplied` を
  記録する。floor-only push、HLC overflow、同値再送、WAL replay の回帰テストで
  floor と削除が復旧後も残ることを確認した。
- 個々の抽象条件から実装全体への包括的な refinement は未証明。
  配送、完了通知、圧縮後の仕事再発見、退役境界、公開読み取り、
  作成根拠と後続操作の宣言の追加照合では、
  確認した範囲で確実な意味上の差分は見つかっていない。
  `EdgeRecordCompaction`、`EdgeCompactedDiscovery`、`EdgeCompactionRecovery`、
  `EdgeCompactedDelivery` についても、sender ごとの
  連続閉鎖範囲と穴、ACK と完了報告の保存後の閉鎖、受信側 fence の保存後の回収、
  descriptor を使う再起動時の仕事再発見を照合した。退役 fence より古い
  packet の再配送は新しい ACK を生成しない。二つの受信側 fence 後の再起動と、
  fence 保存後だけ終端詳細を回収する Rust テストが通過した。`EdgeRetirementProgress` の
  公平性条件には、Rust の有限タイムアウトと再送対象の巡回が対応する。
  `RetryCursor` に渡す pending ID は、操作・support・証明書・policy witness の
  outbox では `BTreeMap` の順序で列挙し、完了報告は `(sender, transfer ID)` で
  ソートして重複を除く。カーソルは周期開始時の有限な ID 集合を固定するため、
  新しい仕事が増え続けても、継続して pending の古い項目を次の周期で再選択する。
  これは worker の tick と各処理の終了を前提にした送信側の巡回であり、
  `EdgeProgressConditions` が示す受信飢餓まで排除する保証ではない。
  送信 outbox の圧縮、完了通知・policy witness の登録で個別項目が失敗しても、
  保存状態の復旧と既存送信の検証が成功すれば別の pending 再送を続ける。
  復旧・検証が失敗する間の進行までは保証しない。
  この配送・完了通知・退役境界の照合は固定宛先を持つ routed v2 経路が対象。
  Lean の `EdgeRouting` は隣接転送までを定め、custody/ACK 契約や routing protocol を
  選んでいない。non-routed v1 の直接 gossip はこの下層に対応する。
  `EdgeCompletionReports` の完了・回収条件は固定宛先と親転送を持つ上位経路に対応する。
  固定宛先が Authority のみで、操作の作成宣言が Disabled の場合も、
  その Authority は署名せずに値なし custody と終端記録を永続化して ACK する。
  Lean の `EdgeEndpoint.queueSigning` は名前に反して署名開始ではなく、Authority
  に届いた packet の永続受け渡しを記録する遷移である。Disabled の操作でも
  `Ready(.operation)` を満たして完了でき、`needsSigning` は既知の Enabled
  policy を別途要求する。Rust はこの受け渡しを Enabled なら signing inbox、
  Disabled なら terminal 記録に分ける。Lean の `OperationLeaf` は作成宣言の
  mode を含まないため、この保存先の区別そのものはモデル化していない。
  `disabled_authority_intake_completes_without_signing` は既知の Disabled policy
  のもとで終端完了が可能で、署名仕事が生じないことを示す。
  完了通知は終端記録から再起動後にも再発見でき、送信側の閉鎖通知を
  受信側が永続化した後は終端記録を回収できる。これにより Eventual の
  ローカル保存後の成功応答が、Authority-only 宛先で永久拒否に変わらない。
  non-routed の直接 gossip には最終宛先の完了通知と受信 custody の退役境界がなく、
  受信コピーを保持する。policy-only の独立配送は固定宛先の経路を要するが、
  操作本体を non-routed で配送するノードにも別の経路を設定できる。
  non-routed に ACK だけを根拠とする受信コピー回収を加えると、循環した gossip で
  全コピーを失い得るため、この差分は配送方式と完了条件を決めてから扱う。

## 旧 Certified cache の checkpoint 条件

`../aste-lean/AsteroidDB/AttestationPoolCacheReadGate.lean` の
`CachedReadEligible` は、証明書の署名者全員について、同じ checkpoint の
frontier を別途受理済みであることを要求する。Rust の `AckFrontierSet` は
受理した checkpoint をプロセス内で記録し、`CertifiedApi` は署名が元の
frontier report と一致し、その full frontier が受理済みの場合だけ
attestation pool に入れる。発行時には全署名者の受理済み frontier を
cache に写し、read 時に証明書の scope・checkpoint・署名者と再照合する。
これにより、別の新しい frontier があるだけで古い未受理 checkpoint の署名を
cache に発行する経路を閉じ、受理履歴が 128 件の上限で消えた後も発行時の
根拠を保持する。policy version を再割当する unfence は旧 cache を失効させる。
`update_frontier_verified` の attestation は元の full frontier 報告に束縛され、
pool は同一 scope・checkpoint の署名だけで証明書を組み立てる。発行時には
固定 Authority 名簿と各署名者の受理済み full frontier を確認する。
同じ checkpoint の別の未署名報告を署名の受理根拠や発行時の witness に
代用できない。新しい checkpoint の受理記録が未署名報告で上書きされても、
一つ前の有効な署名済み checkpoint を使える。未署名報告と完全に同じ報告の
署名が後着した場合は採用できる。これらの対象 Rust テストが通過した。
受理履歴と pool・cache は再起動時に消えるため、古い署名の遅延到着は
保守的に採用しない場合がある。

この `readScope` は対象書き込みに pin した発行時 scope として扱う。発行済み
cache は現在の namespace で range や名簿が変わっても、その書き込みの
証明として残る。`CertifiedWriteApplication.lean` は別の新しい操作経路を表す。
そちらの証明書は create/add の operation leaf に key・value・完全な HLC を結び付ける。
旧 `/api/certified/*` の frontier 証明書は range・checkpoint・policy version の
証明であり、value のバイト列を署名しない。両者を同じ value-bound な証明と
見なさず、旧 cache の読取条件だけをこの節で照合する。

旧 Certified API の証明済み cache は以前、10,000 件を超えると古い証拠を
破棄していた。保存値が変わらず、保持期間切れで pending receipt が消えたキーでも
公開状態が `Certified` から `Pending` に降格するため、この件数による破棄を外した。
証明済みキー数に応じたメモリ増加は残る。境界を超えた後も最初のキーの証明と
`Certified` 状態が残る回帰テストを追加した。再起動時に旧 cache が揮発する設計は
別の境界であり、この変更で永続化されたとは扱わない。
旧 Certified のローカル書き込みと複製受信も、CRDT の候補 post-state を
先に計算し、WAL append が成功してから store・証明 cache・scope pin・pending
記録を公開する。append 失敗で以前の証明済み値が `Pending` に落ちたり、
複製先に未記録の値が残って再送を省略したりしない。両失敗経路の再試行を
対象テストで確認した。HTTP read/status は取得時の WAL 位置を耐久化してから
応答する。直接の in-process 読み取りは append 後・fsync 前の状態を見得る。
旧 Certified 複製の RR gate は、CRDT post-state が同じでも受信 HLC が
保存済み head より新しければ別の書き込みとして受理する。以前は値の merge が
no-op なら新しい HLC も飛ばし、peer cursor だけが進んで受信側の書き込み ID と
証明状態が古いままになった。省略条件を `incoming HLC ≤ stored HLC` に絞り、
新しい HLC では scope pin・pending head を更新して旧証明を失効させる。
同値で HLC が古い再送と、同値で HLC が新しい書き込みの双方を回帰テストで確認した。

`SignatureByteBoundary.lean` の正規化 checkpoint 条件に合わせ、Rust の
証明書 verifier は physical の 1000 ms 境界、logical 0、空 node ID と、
証明書と read bundle の range・HLC・policy version・署名者集合の一致を
検査する。Lean の raw report/certificate byte 衝突と同じ 132 byte の Rust
メッセージを構成し、その report 署名が低レベルのバイト検証には通る一方、
公開 verifier では非正規化 checkpoint として拒否される回帰テストを追加した。
分離の根拠は domain tag 単体ではなく、この正規化検査である。
包括的な精密化証明は未実施。
旧証明書 verifier は registry の有無にかかわらず、呼び出し側が渡す対象 range の
信頼済み Authority 名簿から quorum 分母と署名者の所属を検査する。registry には
別 range の鍵も含まれ得るため、登録鍵数だけでは名簿を決めない。registry を使わない
検証には、信頼済み ID と公開鍵の組を渡し、証明書の埋め込み鍵との一致も検査する。
Ed25519 の多数証明書は署名方式の宣言も Ed25519 に限定する。dual mode 証明書は
mode と署名方式の宣言が一致しなければ拒否し、BLS と宣言した証明書を Ed25519
署名だけで有効にしない。
署名者を ID ごとに数える前提を守るため、keyset 登録は同じ公開鍵を異なる
Authority ID に割り当てない。同じ版の重複だけでなく、grace 期間に同時使用する
保持済み版との交差重複も拒否する。BLS 鍵も PoP の検証に加えて同じ ID・鍵の
一意性を要求し、Ed25519 keyset にいない ID は登録しない。
Certified 複製は書き込み時の scope pin（range・policy version・Authority 名簿・HLC）を
wire で運ぶ。受信側はその名簿と range が自身の信頼済み定義に一致する場合だけ適用し、
一致しない間は delta baseline を進めない。復旧時に pin が欠けたキーは起動を拒否し、
現在の名簿で補完して証明することはない。過去の名簿を信頼する履歴は未実装なので、
名簿が変更されたままの旧書き込みの複製は停止する。
delta cursor は `(HLC, key)` で保持する。同一 HLC の複数キーが batch 境界をまたいでも
残りを再取得でき、後続キーが一時拒否された場合も受理済みの前キーから再開する。
後者は実 HTTP と `NodeRunner::run_certified_sync` を通した回帰テストで確認した。
