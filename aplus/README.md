# A+ storage prototype (#133)

Throwaway. Question: built crudely and measured, what does design A+ cost
against main 76952348, and how many extra bytes does it write per Put?

A+ as built here:

- Values stay in the Raft log segments, written once. The index keeps a
  descriptor per version: the file, offset and length (crude: a slice of an
  mmap of the file) and a CRC32C of the value.
- A checkpoint writes only the dirty leaf pages (about 4 KiB, keys with all
  their retained versions) of a copy-on-write metadata tree, the group pages
  above changed leaves (64 leaves per group), a root page and a 4 KiB
  superblock slot. Cut every `APLUS_CKPT_EVERY` bytes of log (default 64
  MiB) and after each compaction.
- Raft entries stay in RAM for `APLUS_WINDOW` bytes (default 64 MiB) past
  the newest durable checkpoint. A follower behind that gets a snapshot:
  the newest checkpoint's pages plus every value they reference.
- A cleaner copies the live values of a sealed file with at most
  `APLUS_CLEAN_U` (0.5) of its bytes live into a new value file; the
  committer swaps the references (copy on write). Files are deleted once no
  durable checkpoint references them.
- Restart loads the newest checkpoint and replays the log after it.

Not built: leases in checkpoints, a reader pool (a cold read is a page
fault in the connection writer), streaming snapshots (one message in RAM),
pins (deleted files stay mapped 30 s), crash safety of snapshot install.

Run (on a GitHub runner, see `.github/workflows/aplus.yml`):

    aplus/setup.sh
    BINS=bin CERTS=certs aplus/big.sh candidate 536870912 1600000 out
    BINS=bin CERTS=certs aplus/catchup.sh candidate 300000 10 out
    BINS=bin CERTS=certs aplus/churn.sh candidate 200000 10000 180 10 100000 out
    TARGETS="baseline candidate" BINS=bin CERTS=certs bench/run.sh put-1k out

Each node logs an `aplus-stats` line every second with the bytes written
by component.
