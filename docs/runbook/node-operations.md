# Node Operations Runbook

## Join Procedure

1. Configure the new node with a unique `node_id` and the seed node address:

   ```bash
   export ASTEROIDB_NODE_ID=node-4
   export ASTEROIDB_BIND_ADDR=0.0.0.0:3003
   export ASTEROIDB_ADVERTISE_ADDR=10.0.1.4:3003
   ```

2. Start the node. It will fan-out join to all known peers automatically.

3. Verify join succeeded:

   ```bash
   asteroidb-cli --host 10.0.1.4:3003 status
   ```

4. Confirm the node appears in peer lists on existing nodes:

   ```bash
   curl http://10.0.1.1:3000/api/internal/keys | jq .
   ```

## Leave Procedure

1. Graceful shutdown sends fan-out leave to all peers automatically on Ctrl-C.

2. For manual removal, POST to the leave endpoint on a seed node:

   ```bash
   curl -X POST http://10.0.1.1:3000/api/internal/leave \
     -H 'Content-Type: application/json' \
     -d '{"node_id": "node-4"}'
   ```

3. Verify the node was removed from the peer registry.

## Restart Procedure

1. Stop the node (Ctrl-C for graceful shutdown).
2. The node persists its peer registry to `$ASTEROIDB_DATA_DIR/peers.json`.
3. Restart the process. On startup, the node will:
   - Load the persisted peer registry.
   - Fan-out join to re-announce its presence.
   - Resume anti-entropy sync from the persisted frontier.

4. If the peer registry file is corrupted, delete it and restart:

   ```bash
   rm ./data/peers.json
   ```

   The node will start with an empty registry and re-join via seed nodes.

## Health Checks

- `GET /api/metrics` returns operational metrics.
- `GET /api/slo` returns SLO budget status.
- Use the CLI for quick checks:

  ```bash
  asteroidb-cli status
  asteroidb-cli slo
  ```
