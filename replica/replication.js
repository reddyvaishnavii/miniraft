async function replicateBatches({
  getLog,
  sendAppendEntries,
  getTerm,
  getLeaderId,
  getCommitIndex,
  getState,
  peerNextIndex,
  peerMatchIndex,
  peer,
  maxBatchSize = 50,
  maxAttempts,
}) {
  if (getState() !== "LEADER") return false;

  let log = getLog();
  let nextIndex = peerNextIndex.get(peer) ?? log.length;
  const attemptsLimit = maxAttempts ?? Math.max(3, log.length + 2);

  for (let attempt = 0; attempt < attemptsLimit; attempt++) {
    if (getState() !== "LEADER") return false;

    log = getLog();
    nextIndex = Math.max(0, Math.min(nextIndex, log.length));

    const prevLogIndex = nextIndex - 1;
    const prevLogTerm = prevLogIndex >= 0 ? log[prevLogIndex].term : -1;
    const entries = log.slice(nextIndex, nextIndex + maxBatchSize);

    let response;
    try {
      response = await sendAppendEntries(peer, {
        term: getTerm(),
        leaderId: getLeaderId(),
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit: getCommitIndex(),
      });
    } catch {
      return false;
    }

    if (
      response?.higherTerm ||
      (Number.isInteger(response?.term) && response.term > getTerm())
    ) {
      return false;
    }

    if (response?.success) {
      const matchIndex = prevLogIndex + entries.length;
      peerNextIndex.set(peer, matchIndex + 1);
      peerMatchIndex.set(peer, matchIndex);

      if (matchIndex < getLog().length - 1) {
        nextIndex = matchIndex + 1;
        continue;
      }

      return true;
    }

    nextIndex = Math.max(
      0,
      Math.min(
        nextIndex - 1,
        Number.isInteger(response?.conflictIndex)
          ? response.conflictIndex
          : nextIndex - 1,
      ),
    );
    peerNextIndex.set(peer, nextIndex);
  }

  return false;
}

module.exports = { replicateBatches };
