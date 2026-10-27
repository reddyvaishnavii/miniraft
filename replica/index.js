/**
 * MiniRaft Replica Node — shared implementation for all three replicas.
 * This is an educational Raft-style implementation, not a production Raft library.
 */
const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { reconcileLog } = require("./log-reconciliation");

const REPLICA_ID = process.env.REPLICA_ID || "replica1";
const PORT = Number.parseInt(process.env.REPLICA_PORT || "4001", 10);
const PEERS = (process.env.PEERS || "").split(",").map(s => s.trim()).filter(Boolean);
const DATA_DIR = process.env.DATA_DIR || "/data";
const STATE_FILE = path.join(DATA_DIR, "raft-state.json");

const HEARTBEAT_INTERVAL_MS = 150;
const ELECTION_TIMEOUT_MIN = 500;
const ELECTION_TIMEOUT_MAX = 800;
const RPC_TIMEOUT_MS = 250;

let state = "FOLLOWER";
let currentTerm = 0;
let votedFor = null;
let leaderId = null;
let log = [];
let commitIndex = -1;
let electionTimer = null;
let heartbeatTimer = null;
let peerNextIndex = new Map();
let peerMatchIndex = new Map();
let electionRound = 0;

const app = express();
app.use(express.json({ limit: "2mb" }));

function logInfo(message) {
  console.log(`[${REPLICA_ID}][term=${currentTerm}][${state}] ${message}`);
}

function persistState() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const tmp = `${STATE_FILE}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ currentTerm, votedFor, log, commitIndex }));
  fs.renameSync(tmp, STATE_FILE);
}

function loadState() {
  try {
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    currentTerm = Number.isInteger(saved.currentTerm) ? saved.currentTerm : 0;
    votedFor = saved.votedFor ?? null;
    log = Array.isArray(saved.log) ? saved.log : [];
    commitIndex = Number.isInteger(saved.commitIndex) ? saved.commitIndex : -1;
    commitIndex = Math.min(commitIndex, log.length - 1);
    logInfo(`Loaded persistent state: logLength=${log.length}, commitIndex=${commitIndex}`);
  } catch (err) {
    if (err.code !== "ENOENT") logInfo(`Could not load saved state: ${err.message}`);
    persistState();
  }
}

function randomElectionTimeout() {
  return Math.floor(Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN + 1)) + ELECTION_TIMEOUT_MIN;
}

function stopElectionTimer() {
  if (electionTimer) clearTimeout(electionTimer);
  electionTimer = null;
}

function stopHeartbeat() {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = null;
}

function resetElectionTimer() {
  stopElectionTimer();
  electionTimer = setTimeout(() => startElection(), randomElectionTimeout());
}


function becomeFollower(term, newLeader = null) {
  const termChanged = term > currentTerm;
  const leadershipChanged =
    state !== "FOLLOWER" ||
    leaderId !== newLeader ||
    termChanged;

  if (termChanged) {
    currentTerm = term;
    votedFor = null;
    persistState();
  }

  state = "FOLLOWER";
  leaderId = newLeader;
  stopHeartbeat();
  resetElectionTimer();

  if (leadershipChanged) {
    logInfo(`Became FOLLOWER${newLeader ? `; leader=${newLeader}` : ""}`);
  }
}


function lastLogIndex() {
  return log.length - 1;
}

function lastLogTerm() {
  return log.length ? log[log.length - 1].term : -1;
}

async function startElection() {
  if (state === "LEADER") return;
  const round = ++electionRound;
  state = "CANDIDATE";
  currentTerm += 1;
  votedFor = REPLICA_ID;
  leaderId = null;
  persistState();
  const electionTerm = currentTerm;
  logInfo("Starting election");
  resetElectionTimer();

  let votes = 1;
  const majority = Math.floor((PEERS.length + 1) / 2) + 1;

  await Promise.all(PEERS.map(async peer => {
    try {
      const response = await axios.post(`${peer}/request-vote`, {
        term: electionTerm,
        candidateId: REPLICA_ID,
        lastLogIndex: lastLogIndex(),
        lastLogTerm: lastLogTerm()
      }, { timeout: RPC_TIMEOUT_MS });

      if (response.data.term > currentTerm) {
        becomeFollower(response.data.term);
        return;
      }
      if (state === "CANDIDATE" && currentTerm === electionTerm && response.data.voteGranted) {
        votes += 1;
      }
    } catch {
      logInfo(`Vote request to ${peer} failed`);
    }
  }));

  if (round !== electionRound || state !== "CANDIDATE" || currentTerm !== electionTerm) return;
  if (votes >= majority) {
    becomeLeader();
  } else {
    logInfo(`Election did not reach quorum (${votes}/${majority} required)`);
    becomeFollower(currentTerm);
  }
}

function becomeLeader() {
  state = "LEADER";
  leaderId = REPLICA_ID;
  stopElectionTimer();
  peerNextIndex = new Map(PEERS.map(peer => [peer, log.length]));
  peerMatchIndex = new Map(PEERS.map(peer => [peer, -1]));
  logInfo("Became LEADER");
  startHeartbeat();
  // Immediately announce leadership and try to catch followers up.
  replicateAllPeers().catch(() => {});
}

function higherTermResponse(response) {
  if (response && Number.isInteger(response.term) && response.term > currentTerm) {
    becomeFollower(response.term);
    return true;
  }
  return false;
}

/**
 * Send AppendEntries (including empty heartbeats) to a peer.
 * On log mismatch, back up nextIndex and retry. Entries are sent as a suffix.
 */
async function replicatePeer(peer) {
  if (state !== "LEADER") return false;
  let nextIndex = peerNextIndex.get(peer) ?? log.length;

  for (let attempt = 0; attempt < Math.max(3, log.length + 2); attempt++) {
    if (state !== "LEADER") return false;
    nextIndex = Math.max(0, Math.min(nextIndex, log.length));
    const prevLogIndex = nextIndex - 1;
    const prevLogTerm = prevLogIndex >= 0 ? log[prevLogIndex].term : -1;
    const entries = log.slice(nextIndex);

    try {
      const response = await axios.post(`${peer}/append-entries`, {
        term: currentTerm,
        leaderId: REPLICA_ID,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit: commitIndex
      }, { timeout: RPC_TIMEOUT_MS });

      if (higherTermResponse(response.data)) return false;

      if (response.data.success) {
        const matchIndex = prevLogIndex + entries.length;
        peerNextIndex.set(peer, matchIndex + 1);
        peerMatchIndex.set(peer, matchIndex);
        return true;
      }

      // A stale-term response or missing/conflicting previous entry: retry earlier.
      nextIndex = Math.max(0, Math.min(nextIndex - 1, Number.isInteger(response.data.conflictIndex)
        ? response.data.conflictIndex : nextIndex - 1));
      peerNextIndex.set(peer, nextIndex);
    } catch {
      return false;
    }
  }
  return false;
}

async function replicateAllPeers() {
  if (state !== "LEADER") return;
  await Promise.all(PEERS.map(peer => replicatePeer(peer)));
}

function advanceCommitIndex() {
  if (state !== "LEADER") return;
  // Raft only advances commitIndex directly for an entry from the current term.
  for (let index = log.length - 1; index > commitIndex; index--) {
    if (log[index].term !== currentTerm) continue;
    let replicated = 1; // leader itself
    for (const peer of PEERS) {
      if ((peerMatchIndex.get(peer) ?? -1) >= index) replicated += 1;
    }
    const majority = Math.floor((PEERS.length + 1) / 2) + 1;
    if (replicated >= majority) {
      commitIndex = index;
      persistState();
      logInfo(`Committed through index ${commitIndex}`);
      break;
    }
  }
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function startHeartbeat() {
  stopHeartbeat();
  heartbeatTimer = setInterval(async () => {
    if (state !== "LEADER") return;
    await replicateAllPeers();
    if (state !== "LEADER") return;
    advanceCommitIndex();
    // Propagate any newly advanced commitIndex, even when no entries need sending.
    await replicateAllPeers();
  }, HEARTBEAT_INTERVAL_MS);
}

app.post("/request-vote", (req, res) => {
  const { term, candidateId, lastLogIndex: candidateIndex, lastLogTerm: candidateTerm } = req.body || {};
  if (!Number.isInteger(term) || typeof candidateId !== "string") {
    return res.status(400).json({ term: currentTerm, voteGranted: false, error: "invalid-request" });
  }
  if (term < currentTerm) return res.json({ term: currentTerm, voteGranted: false });
  if (term > currentTerm) becomeFollower(term);

  const logIsUpToDate =
    candidateTerm > lastLogTerm() ||
    (candidateTerm === lastLogTerm() && candidateIndex >= lastLogIndex());
  const canVote = votedFor === null || votedFor === candidateId;

  if (canVote && logIsUpToDate) {
    votedFor = candidateId;
    persistState();
    resetElectionTimer();
    return res.json({ term: currentTerm, voteGranted: true });
  }
  return res.json({ term: currentTerm, voteGranted: false });
});

app.post("/append-entries", (req, res) => {
  const body = req.body || {};
  const { term, leaderId: incomingLeader, prevLogIndex, prevLogTerm } = body;
  const entries = Array.isArray(body.entries) ? body.entries : (body.entry ? [body.entry] : []);
  const leaderCommit = Number.isInteger(body.leaderCommit) ? body.leaderCommit : -1;

  if (!Number.isInteger(term) || term < currentTerm) {
    return res.json({ term: currentTerm, success: false, conflictIndex: log.length });
  }
  becomeFollower(term, incomingLeader);

  if (!Number.isInteger(prevLogIndex) || prevLogIndex < -1) {
    return res.status(400).json({ term: currentTerm, success: false, conflictIndex: log.length });
  }
  if (prevLogIndex >= 0) {
    if (prevLogIndex >= log.length) {
      return res.json({ term: currentTerm, success: false, conflictIndex: log.length });
    }
    if (log[prevLogIndex].term !== prevLogTerm) {
      const conflictTerm = log[prevLogIndex].term;
      let conflictIndex = prevLogIndex;
      while (conflictIndex > 0 && log[conflictIndex - 1].term === conflictTerm) conflictIndex--;
      return res.json({ term: currentTerm, success: false, conflictIndex });
    }
  }

  
  const result = reconcileLog({
    log,
    commitIndex,
    prevLogIndex,
    entries,
  });

  if (!result.ok) {
    return res.status(409).json({
      term: currentTerm,
      success: false,
      error: result.error,
      conflictIndex: result.conflictIndex,
    });
  }

  log = result.log;


  if (leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, log.length - 1);
  }
  persistState();
  return res.json({ term: currentTerm, success: true, logLength: log.length });
});




const inFlightStrokes = new Map();
const inFlightClears = new Map();

app.post("/stroke", async (req, res) => {
  if (state !== "LEADER") {
    return res.status(307).json({
      error: "not-leader",
      leaderId,
      term: currentTerm
    });
  }

  const stroke = req.body?.stroke;
  const requestId = req.body?.requestId;

  if (!stroke || !Array.isArray(stroke.points)) {
    return res.status(400).json({ error: "invalid-stroke" });
  }

  if (typeof requestId !== "string" || requestId.length === 0) {
    return res.status(400).json({ error: "request-id-required" });
  }

  // A request with this ID is already being processed by this leader.
  if (inFlightStrokes.has(requestId)) {
    const result = await inFlightStrokes.get(requestId);
    return res.status(result.status).json(result.body);
  }

  // The ID may already exist in the log, including after a restart.
  const existingEntry = log.find(entry => entry.requestId === requestId);
  if (existingEntry) {
    if (commitIndex >= existingEntry.index) {
      return res.json({
        ok: true,
        index: existingEntry.index,
        term: existingEntry.term,
        duplicate: true
      });
    }

    return res.status(503).json({
      error: "request-already-appended-not-committed",
      index: existingEntry.index
    });
  }

  const processing = (async () => {
    const entry = {
      index: log.length,
      term: currentTerm,
      requestId,
      stroke
    };

    log.push(entry);
    persistState();

    await Promise.all(PEERS.map(async peer => {
      const ok = await replicatePeer(peer);
      if (ok) advanceCommitIndex();
    }));

    if (state !== "LEADER") {
      return {
        status: 503,
        body: { error: "leadership-changed", term: currentTerm }
      };
    }

    advanceCommitIndex();

    if (commitIndex >= entry.index) {
      await replicateAllPeers();
      return {
        status: 200,
        body: {
          ok: true,
          index: entry.index,
          term: currentTerm
        }
      };
    }

    logInfo(`Stroke at index ${entry.index} has not reached quorum`);
    return {
      status: 503,
      body: {
        error: "quorum-not-reached",
        index: entry.index
      }
    };
  })();

  inFlightStrokes.set(requestId, processing);

  try {
    const result = await processing;
    return res.status(result.status).json(result.body);
  } catch (err) {
    logInfo(`Stroke request failed: ${err.message}`);
    return res.status(500).json({ error: "stroke-processing-failed" });
  } finally {
    inFlightStrokes.delete(requestId);
  }
});



app.post("/clear", async (req, res) => {
  if (state !== "LEADER") {
    return res.status(307).json({
      error: "not-leader",
      leaderId,
      term: currentTerm
    });
  }

  const requestId = req.body?.requestId;

  if (typeof requestId !== "string" || requestId.length === 0) {
    return res.status(400).json({ error: "request-id-required" });
  }

  // If the same Clear is already being processed, share its result.
  if (inFlightClears.has(requestId)) {
    const result = await inFlightClears.get(requestId);
    return res.status(result.status).json(result.body);
  }

  // Handle retries for requests already present in the log.
  const existingEntry = log.find(entry => entry.requestId === requestId);

  if (existingEntry) {
    if (commitIndex >= existingEntry.index) {
      return res.json({
        ok: true,
        index: existingEntry.index,
        term: existingEntry.term,
        duplicate: true
      });
    }

    return res.status(503).json({
      error: "request-already-appended-not-committed",
      index: existingEntry.index
    });
  }

  const processing = (async () => {
    const entry = {
      index: log.length,
      term: currentTerm,
      requestId,
      type: "clear"
    };

    log.push(entry);
    persistState();

    await Promise.all(PEERS.map(async peer => {
      const ok = await replicatePeer(peer);
      if (ok) advanceCommitIndex();
    }));

    if (state !== "LEADER") {
      return {
        status: 503,
        body: {
          error: "leadership-changed",
          term: currentTerm
        }
      };
    }

    advanceCommitIndex();

    if (commitIndex >= entry.index) {
      await replicateAllPeers();

      return {
        status: 200,
        body: {
          ok: true,
          index: entry.index,
          term: entry.term
        }
      };
    }

    logInfo(`Clear at index ${entry.index} has not reached quorum`);

    return {
      status: 503,
      body: {
        error: "quorum-not-reached",
        index: entry.index
      }
    };
  })();

  inFlightClears.set(requestId, processing);

  try {
    const result = await processing;
    return res.status(result.status).json(result.body);
  } catch (err) {
    console.error("Clear request failed:", err.message);
    return res.status(500).json({ error: "clear-request-failed" });
  } finally {
    inFlightClears.delete(requestId);
  }
});



app.post("/sync-log", (req, res) => {
  const fromIndex = Math.max(0, Number.parseInt(req.body?.fromIndex ?? "0", 10));
  res.json({ entries: log.slice(fromIndex).filter(e => e.index <= commitIndex), commitIndex });
});



app.get("/status", (req, res) => {
  res.json({ replicaId: REPLICA_ID, state, currentTerm, leaderId, logLength: log.length, commitIndex });
});

app.get("/log", (req, res) => {
  res.json({ entries: log.filter(entry => entry.index <= commitIndex), commitIndex });
});

loadState();
app.listen(PORT, () => {
  logInfo(`Listening on port ${PORT}; peers=${PEERS.join(",") || "(none)"}`);
  resetElectionTimer();
});

process.on("SIGTERM", () => {
  logInfo("SIGTERM — shutting down");
  stopHeartbeat();
  stopElectionTimer();
  process.exit(0);
});
process.on("SIGINT", () => {
  stopHeartbeat();
  stopElectionTimer();
  process.exit(0);
});
