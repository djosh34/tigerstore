// Package planted switches on deliberate bugs, so that tests can prove
// the simulator and the checkers catch them. Without the planted build tag
// On is always false and the compiler drops the bug paths; a binary built
// for production never contains them.
package planted

// Bugs lists every planted bug: its name and what it breaks.
var Bugs = map[string]string{
	"B01a": "linearizable reads are served without ReadIndex",
	"B01b": "a read is served once its read index is known, before it is applied",
	"B02":  "the leader's disk driver reports an append durable before its fsync",
	"B03":  "a follower's disk driver reports an append durable before its fsync",
	"B04a": "WAL recovery accepts a record whose checksum does not verify",
	"B04c": "WAL recovery skips checksums, and a payload bit flips so that the record still decodes",
	"B04b": "WAL recovery cuts the record before a torn tail too",
	"B05":  "a new WAL segment takes records before its directory entry is synced",
	"B06a": "a watcher drops the last event of a write that changes several keys",
	"B06b": "a watcher gets the first event of every write twice",
	"B06c": "watch events of updates and deletes lose their previous value",
	"B06d": "a progress notification goes out before the events it covers",
	"B07":  "a new watch starts one revision early",
	"B08":  "compaction drops the tombstone at the compaction revision",
	"B09a": "each changed key of a write gets its own main revision",
	"B09b": "a key created again after a delete keeps its old version and create revision",
	"B10":  "followers expire leases on their own clock, without the log",
	"B11":  "the disk driver treats a failed fsync as success",
	"B12":  "the WAL keeps going after a short write",
	"B13":  "the node driver waits for a slow watch stream instead of closing it",
	"B14":  "the peer writer has no write deadline",
	"B15":  "the history treats a call that timed out as certainly aborted",
	"B16":  "WAL rotation moves to a new segment without the old one's last sync",
	"B17":  "the leader's stalled-disk watchdog never fires",
	"B18":  "closing a watch stream leaves its watchers in place",
	"B19":  "removing old segments drops one that recovery from the durable checkpoint still needs",
	"B20":  "entries leave memory above the applied index",
	"B21":  "a checkpoint names the committed index instead of the applied one",
	"B22":  "a follower installs a snapshot without checking it",
	"B23":  "a restart replays the log one entry short of its commit index",
	"B24":  "the index names a value one byte after where it is in its Raft entry",
	"B25":  "value reads skip the checksum",
	"B26":  "a follower stops when it cannot fetch its snapshot, instead of starting again from its log",
	"B27":  "the core goes on after a snapshot install instead of starting again from the log",
	"B28":  "MemberUpdate acknowledges a change without updating the peer URLs",
	"B29":  "the cleaner removes segments before syncing their replacement root",
	"B30":  "the cleaner omits a value still named by the durable checkpoint",
	"B31":  "a cache lookup ignores the Raft index and returns an overwritten value",
}
