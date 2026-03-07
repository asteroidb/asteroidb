# Key Rotation Runbook

## Overview

AsteroidDB uses Ed25519 keys for Authority certificate signing. Keys are
managed per `keyset_version` with a 24-hour epoch and 7-epoch grace period
for past versions (FR-008).

## Rotation Trigger

Key rotation should be triggered when:

- A scheduled rotation epoch arrives (every 24 hours by default).
- A key compromise is suspected (emergency rotation).
- A new Authority node joins and needs its own signing key.

## Rotation Procedure

1. **Generate new keyset**: Increment `keyset_version` and generate a new
   Ed25519 keypair for each Authority node in the scope.

2. **Distribute keys**: Push the new public keys to all nodes via the
   control-plane authority definition update:

   ```bash
   curl -X PUT http://seed:3000/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -d '{
       "key_range_prefix": "",
       "authority_nodes": ["auth-1", "auth-2", "auth-3"],
       "approvals": ["auth-1", "auth-2"]
     }'
   ```

3. **Verify rotation**: Confirm the new keyset version is active:

   ```bash
   curl http://seed:3000/api/control-plane/authorities | jq .
   ```

4. **Grace period**: The old keyset remains valid for 7 epochs (7 days).
   During this period, certificates signed with the old key are still accepted.

## Verification

After rotation, verify that certified reads still produce valid proofs:

```bash
asteroidb-cli --host seed:3000 get my-certified-key
```

Check the proof bundle contains the new `keyset_version`.

## Rollback

If the new keyset causes issues:

1. The old keyset is still valid within the 7-epoch grace period.
2. Revert the Authority definition to the previous node set.
3. Nodes will fall back to the previous keyset automatically.

If beyond the grace period, a full re-certification cycle is required:

1. Generate a new keyset (increment version again).
2. Distribute and verify as above.
3. All pending certifications will be re-evaluated with the new keys.
