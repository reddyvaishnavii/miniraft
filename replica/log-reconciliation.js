
function reconcileLog({ log, commitIndex, prevLogIndex, entries }) {
    if (
      !Array.isArray(log) ||
      !Number.isInteger(commitIndex) ||
      !Number.isInteger(prevLogIndex) ||
      prevLogIndex < -1 ||
      !Array.isArray(entries) ||
      entries.some(
        (entry, i) =>
          !entry ||
          !Number.isInteger(entry.term) ||
          entry.term < 0 ||
          !Number.isInteger(entry.index) ||
          entry.index !== prevLogIndex + 1 + i
      )
    ) {
      return { ok: false, error: "invalid-log-input", log };
    }
  
    const nextLog = [...log];
  
    for (let i = 0; i < entries.length; i++) {
      const incoming = entries[i];
      const targetIndex = prevLogIndex + 1 + i;
  
      if (targetIndex < nextLog.length) {
        if (nextLog[targetIndex].term !== incoming.term) {
          if (targetIndex <= commitIndex) {
            return {
              ok: false,
              log: nextLog,
              conflictIndex: targetIndex,
              error: "committed-entry-conflict",
            };
          }
  
          nextLog.splice(targetIndex);
          nextLog.push(...entries.slice(i));
          break;
        }
      } else {
        nextLog.push(...entries.slice(i));
        break;
      }
    }
  
    return { ok: true, log: nextLog };
  }
  
  module.exports = { reconcileLog };
  