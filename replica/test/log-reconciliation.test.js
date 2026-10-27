
const test = require("node:test");
const assert = require("node:assert/strict");

const { reconcileLog } = require("../log-reconciliation");

test("preserves matching entries without truncating the log", () => {
  const log = [{ index: 0, term: 1 }, { index: 1, term: 1 }];

  const result = reconcileLog({
    log,
    commitIndex: 1,
    prevLogIndex: 0,
    entries: [{ index: 1, term: 1 }],
  });

  assert.equal(result.ok, true);
  assert.deepEqual(result.log, log);
});

test("replaces conflicting uncommitted entries", () => {
  const log = [
    { index: 0, term: 1 },
    { index: 1, term: 1 },
    { index: 2, term: 2 },
  ];

  const result = reconcileLog({
    log,
    commitIndex: 1,
    prevLogIndex: 1,
    entries: [
      { index: 2, term: 3 },
      { index: 3, term: 3 },
    ],
  });

  assert.equal(result.ok, true);
  assert.deepEqual(result.log, [
    { index: 0, term: 1 },
    { index: 1, term: 1 },
    { index: 2, term: 3 },
    { index: 3, term: 3 },
  ]);
});

test("rejects conflicts with committed entries without modifying the log", () => {
  const log = [{ index: 0, term: 1 }, { index: 1, term: 2 }];
  const originalLog = structuredClone(log);

  const result = reconcileLog({
    log,
    commitIndex: 1,
    prevLogIndex: 0,
    entries: [{ index: 1, term: 3 }],
  });

  assert.equal(result.ok, false);
  assert.equal(result.error, "committed-entry-conflict");
  assert.equal(result.conflictIndex, 1);
  assert.deepEqual(result.log, originalLog);
  assert.deepEqual(log, originalLog);
});

test("rejects entries with invalid terms", () => {
  const log = [{ index: 0, term: 1 }];

  const result = reconcileLog({
    log,
    commitIndex: 0,
    prevLogIndex: 0,
    entries: [{ index: 1, term: -1 }],
  });

  assert.equal(result.ok, false);
  assert.equal(result.error, "invalid-log-input");
  assert.deepEqual(result.log, log);
});

test("does not mutate the original log when replacing entries", () => {
  const log = [{ index: 0, term: 1 }, { index: 1, term: 2 }];
  const originalLog = structuredClone(log);

  const result = reconcileLog({
    log,
    commitIndex: 0,
    prevLogIndex: 0,
    entries: [{ index: 1, term: 3 }],
  });

  assert.equal(result.ok, true);
  assert.deepEqual(log, originalLog);
  assert.deepEqual(result.log, [
    { index: 0, term: 1 },
    { index: 1, term: 3 },
  ]);
});

test("rejects entries whose indices are not consecutive", () => {
  const log = [{ index: 0, term: 1 }];

  const result = reconcileLog({
    log,
    commitIndex: 0,
    prevLogIndex: 0,
    entries: [{ index: 5, term: 2 }],
  });

  assert.equal(result.ok, false);
  assert.equal(result.error, "invalid-log-input");
  assert.deepEqual(result.log, log);
});

test("accepts an empty heartbeat without changing the log", () => {
    const log = [
      { index: 0, term: 1 },
      { index: 1, term: 2 },
    ];
    const originalLog = structuredClone(log);
  
    const result = reconcileLog({
      log,
      commitIndex: 1,
      prevLogIndex: 1,
      entries: [],
    });
  
    assert.equal(result.ok, true);
    assert.deepEqual(result.log, originalLog);
    assert.deepEqual(log, originalLog);
  });