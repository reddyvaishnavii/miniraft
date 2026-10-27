# RaftBoard — MiniRaft Collaborative Whiteboard

RaftBoard is a real-time collaborative whiteboard backed by three Node.js replica processes. It demonstrates Raft-style leader election, term-based voting, replicated stroke logs, majority-quorum commits, and follower catch-up. A gateway discovers the active leader, forwards drawing and clear requests, and broadcasts successful operations to connected browser clients.

> **Scope:** RaftBoard is an educational implementation of selected Raft concepts. It is not production-ready or formally verified. It does not implement dynamic membership changes, log compaction or snapshots, or exhaustive network-partition and recovery guarantees.

## Architecture

```text
Browser clients
      │
      │ WebSocket
      ▼
Gateway
(leader discovery, request forwarding, broadcast)
      │
      │ HTTP
      ▼
┌──────────┬──────────┬──────────┐
│ replica1 │ replica2 │ replica3 │
│  :4001   │  :4002   │  :4003   │
└──────────┴──────────┴──────────┘
      │          │          │
      ▼          ▼          ▼
 replica1-data replica2-data replica3-data
```

Each replica runs the same shared implementation with its own replica ID, port, peer configuration, and persistent data volume. The gateway communicates with the replicas over HTTP and serves connected browser clients over WebSockets.

## Features

- **Leader election:** Randomized election timeouts and term-based voting.
- **Vote handling:** Candidate log-freshness checks and persisted voting state.
- **Log replication:** AppendEntries-style requests support heartbeats and entry replication.
- **Conflict reconciliation:** Previous-log consistency checks and conflict-aware follower log repair.
- **Quorum-based commits:** Commit advancement requires majority replication and follows the current-term entry rule.
- **Persistent state:** Each replica stores its term, vote, log, and commit index in a separate JSON state file.
- **Leader discovery:** The gateway discovers the active leader and retries requests when a leader becomes unavailable.
- **Collaborative drawing:** Stroke operations are forwarded to the leader and broadcast to connected clients after a successful response.
- **Request deduplication:** Request IDs help prevent repeated committed operations from creating duplicate log entries.
- **Clear synchronization:** Clear requests are handled as replicated operations rather than local-only canvas changes.
- **Follower catch-up:** Replicas can reconcile their logs after temporary unavailability.
- **Historical replay:** The gateway replays historical entries to newly connected clients.

## Tech Stack

- **Backend:** Node.js, Express, Axios
- **Real-time communication:** WebSocket
- **Frontend:** HTML, CSS, JavaScript, HTML Canvas
- **Infrastructure:** Docker, Docker Compose
- **Testing:** Node.js built-in test runner

## Prerequisites

- Docker Desktop
- Docker Compose
- Git

## Run Locally

Clone the repository:

```bash
git clone https://github.com/reddyvaishnavii/miniraft.git
cd miniraft
```

Build and start the services:

```bash
docker compose up --build -d
```

Check the running containers:

```bash
docker compose ps
```

Open the whiteboard:

**http://localhost:8080**

The gateway is exposed on port `3000`, and the three replicas are exposed on ports `4001`, `4002`, and `4003`.

To view service logs:

```bash
docker compose logs -f
```

To inspect a specific service:

```bash
docker logs --tail 100 gateway
docker logs --tail 100 replica1
docker logs --tail 100 replica2
docker logs --tail 100 replica3
```

## Useful Endpoints

| Endpoint | Purpose |
|---|---|
| `GET http://localhost:3000/status` | Gateway status |
| `GET http://localhost:3000/cluster` | Replica and leader information |
| `GET http://localhost:4001/status` | Replica 1 status |
| `GET http://localhost:4002/status` | Replica 2 status |
| `GET http://localhost:4003/status` | Replica 3 status |
| `GET http://localhost:4001/log` | Committed log entries on replica 1 |
| `GET http://localhost:4002/log` | Committed log entries on replica 2 |
| `GET http://localhost:4003/log` | Committed log entries on replica 3 |

The `/status` endpoints expose information such as replica ID, role, current term, known leader, log length, and commit index. The `/log` endpoint returns the committed entries for inspection.

## Testing

### Unit tests

The log reconciliation helper has unit tests covering matching entries, conflicting uncommitted entries, committed-entry conflict rejection, invalid terms, index validation, input immutability, and empty heartbeats.

Run the tests from the repository root:

```bash
node --check replica/log-reconciliation.js
node --check replica/index.js
node --test replica/test/log-reconciliation.test.js
git diff --check
docker compose config -q
```

### Replication smoke test

1. Start the cluster and open the whiteboard.
2. Draw a few strokes.
3. Inspect the status of all three replicas.
4. Compare their log entries and commit indexes.
5. Verify that the newly committed entries appear consistently across the replicas.

### Failover smoke test

1. Open the whiteboard and draw several strokes.
2. Check `http://localhost:3000/cluster` to identify the current leader.
3. Pause the leader container. Replace `replica1` below with the actual leader's container name:

   ```bash
   docker pause replica1
   ```

4. Wait for a new election and inspect the cluster status.
5. Verify that a different replica becomes leader.
6. Resume the paused replica:

   ```bash
   docker unpause replica1
   ```

7. Wait for the replica to rejoin and catch up.
8. Inspect the committed logs and verify that the cluster converges.

A pause test simulates an unresponsive process, while `docker stop` stops a container. Failover duration can vary with election timeouts, network conditions, and the cluster's current state; measure it during actual runs rather than assuming a fixed duration.

### Quorum-loss test

A three-replica cluster requires a majority of two replicas to commit a new entry. To test quorum loss, make two replicas unavailable and submit a new operation to the remaining replica.

The request should not report a successful commit without a majority. An entry appended locally before quorum loss may remain in the log and become committed later if replication resumes and the protocol's commit conditions are satisfied.

**Important:** A failed request does not always mean the operation will never take effect. Clients should use request IDs and check the resulting state before blindly retrying an operation.

## Persistence

Each replica stores its local Raft state in:

```text
/data/raft-state.json
```

Docker Compose mounts a separate named volume for each replica:

- `replica1-data`
- `replica2-data`
- `replica3-data`

These volumes preserve local state across container restarts and recreation as long as the volumes are retained.

To restart the services without intentionally deleting the stored data:

```bash
docker compose restart
```

**Warning:** The following command deletes the named volumes and their persistent data:

```bash
docker compose down -v
```

Do not use it unless you intentionally want to reset the cluster's persisted state.

## Project Structure

```text
miniraft/
├── docker-compose.yml
├── frontend/
│   ├── Dockerfile
│   └── index.html
├── gateway/
│   ├── Dockerfile
│   ├── index.js
│   └── package.json
└── replica/
    ├── Dockerfile
    ├── index.js
    ├── log-reconciliation.js
    ├── package.json
    └── test/
        └── log-reconciliation.test.js
```

## Implementation Scope and Limitations

### Implemented

- Three fixed replicas using a shared implementation.
- Term-based leader election and RequestVote log-freshness checks.
- AppendEntries-style heartbeats and log replication.
- Previous-log consistency checks and conflict reconciliation.
- Local JSON persistence for term, vote, log, and commit index.
- Majority-based commit advancement for current-term entries.
- Gateway leader discovery and WebSocket broadcasting.
- Request-ID-based deduplication for supported operations.
- Separate persistent volumes for each replica.

### Limitations

- No dynamic membership changes.
- No log compaction or snapshots.
- No production-grade storage engine or fully crash-consistent storage protocol.
- No formal verification of Raft safety or liveness.
- No exhaustive testing of every concurrent election, replication, or network-partition scenario.
- No production-grade authentication, authorization, or TLS configuration.
- No guarantee of uninterrupted service during every failure scenario.
- No comprehensive production monitoring or alerting.

RaftBoard is intended for learning, experimentation, and demonstrating distributed-systems concepts. The implementation and tests should not be interpreted as a guarantee of production-grade consensus correctness.

## License

This repository is an educational project. Add a `LICENSE` file if you intend to distribute it under a specific open-source license.
