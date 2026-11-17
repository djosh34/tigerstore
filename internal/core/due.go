package core

// The core's time-based work runs on deadlines, not on every tick (#488,
// #504). Each part knows when it next has to act: the leader's next
// heartbeat, a follower's earliest election timeout, a request's deadline,
// a read index to ask for again, a token to drop, a progress notification.
// Due is the earliest of them. Until then a Tick only moves the clocks and
// Raft's election clock, and its checks find nothing to do, so a driver
// may hold the ticks back and wake only when one is due or something
// happens. It then gives the core, in order, each tick it held back up to
// the moment it could have run again, as if each had come on time; of the
// ticks after, which passed while it waited for a CPU or was busy, one is
// a Tick and the rest go to Elapse, as a ticker drops them
// (node.Node.commit).
