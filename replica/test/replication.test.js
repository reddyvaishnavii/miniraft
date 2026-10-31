const test = require("node:test");
const assert = require("node:assert/strict");
const { replicateBatches } = require("../replication");

test("catches up 120 entries in batches of 50, 50, and 20", async () => {
  const log = Array.from({ length: 120 }, (_, index) => ({
    index,
    term: 1,
  }));

  const batches = [];
  const peerNextIndex = new Map([["follower", 0]]);
  const peerMatchIndex = new Map();

  const result = await replicateBatches({
    getLog: () => log,
    getTerm: () => 1,
    getLeaderId: () => "leader",
    getCommitIndex: () => 119,
    getState: () => "LEADER",
    peer: "follower",
    peerNextIndex,
    peerMatchIndex,
    sendAppendEntries: async (_peer, payload) => {
      batches.push(payload.entries.length);
      return { term: 1, success: true };
    },
  });

  assert.equal(result, true);
  assert.deepEqual(batches, [50, 50, 20]);
  assert.equal(peerMatchIndex.get("follower"), 119);
  assert.equal(peerNextIndex.get("follower"), 120);
});

test("backs up to the conflict index and retries", async () => {
  const log = Array.from({ length: 60 }, (_, index) => ({
    index,
    term: 1,
  }));

  const attemptedIndexes = [];
  const peerNextIndex = new Map([["follower", 60]]);
  const peerMatchIndex = new Map();
  let firstAttempt = true;

  const result = await replicateBatches({
    getLog: () => log,
    getTerm: () => 1,
    getLeaderId: () => "leader",
    getCommitIndex: () => 59,
    getState: () => "LEADER",
    peer: "follower",
    peerNextIndex,
    peerMatchIndex,
    sendAppendEntries: async (_peer, payload) => {
      attemptedIndexes.push(payload.prevLogIndex);

      if (firstAttempt) {
        firstAttempt = false;
        return { term: 1, success: false, conflictIndex: 10 };
      }

      return { term: 1, success: true };
    },
  });

  assert.equal(result, true);
  assert.deepEqual(attemptedIndexes, [59, 9]);
});

test("stops replication when a peer reports a higher term", async () => {
  const log = [{ index: 0, term: 1 }];
  const peerNextIndex = new Map([["follower", 0]]);
  const peerMatchIndex = new Map();

  const result = await replicateBatches({
    getLog: () => log,
    getTerm: () => 1,
    getLeaderId: () => "leader",
    getCommitIndex: () => -1,
    getState: () => "LEADER",
    peer: "follower",
    peerNextIndex,
    peerMatchIndex,
    sendAppendEntries: async () => ({
      term: 2,
      higherTerm: true,
      success: false,
    }),
  });

  assert.equal(result, false);
});
