# Sable

[![CI](https://github.com/vvr1701/sable/actions/workflows/ci.yml/badge.svg)](https://github.com/vvr1701/sable/actions/workflows/ci.yml)

A persistent key-value storage engine for the JVM, built from scratch as a log-structured merge tree (the design
behind LevelDB, RocksDB and Cassandra): write-ahead log, in-memory memtable, immutable on-disk SSTables with bloom
filters and sparse indexes, and background size-tiered compaction. Java 17, zero runtime dependencies.

## Highlights

- **Crash-safe.** CRC-checked WAL with torn-tail recovery and atomic manifest updates. A test SIGKILLs a writing
  process at random moments and verifies that no acknowledged write is ever lost (passed 60 consecutive kills).
- **Fast reads.** Bloom filters cut disk reads for absent keys from 7.0 to 0.06 blocks per lookup (99% fewer).
  Point lookups over 1M keys: p99 12.9 µs, about one block read each.
- **Group commit.** Concurrent writers share one fsync: 7.6x the durable write throughput (425 → 3,249 ops/s).
- **Lock-free reads.** Readers pin a reference-counted snapshot and never block on writers, flushes or compactions.
- **Compaction.** Reclaims overwritten keys and deletes: disk usage of an overwrite-heavy workload drops from
  5.4x to 1.1x the live data size.
- **Tested against a model.** Randomized operations, restarts and compactions are checked against a `TreeMap`.

## Quick start

```bash
./gradlew test     # unit, model, concurrency and crash tests
./gradlew bench    # benchmark suite (see Benchmarks below)
```

Keys and values are `byte[]`, ordered as unsigned bytes.

```java
Options opts = new Options().memtableBytes(4 << 20).syncWrites(true);
try (Sable db = Sable.open(Path.of("data"), opts)) {
    db.put("user:1".getBytes(UTF_8), "ada".getBytes(UTF_8));
    byte[] v = db.get("user:1".getBytes(UTF_8));          // null if absent or deleted
    db.delete("user:1".getBytes(UTF_8));
    try (Sable.Scan scan = db.scan("user:".getBytes(UTF_8), "user;".getBytes(UTF_8))) { // [from, to)
        while (scan.hasNext()) { var e = scan.next(); /* e.getKey(), e.getValue() */ }
    }
}
```

## Architecture

```
write path                         read path (no locks)
----------                         ---------
put/delete                         get(k): pin State (one CAS)
  |  lock                            |
  v                                  +-> memtable (active)      newest
append to WAL ----> fsync            +-> memtable (being flushed)
  |  (group commit)                  +-> SSTable 0 (bloom -> index -> 1 block)
  v                                  +-> SSTable 1 ...
active memtable                      +-> SSTable N              oldest
  | full                           first hit wins; a tombstone means "absent"
  v
rotate: freeze memtable + WAL,
        start new ones

background (two single-thread executors)
----------------------------------------
sable-flush:   frozen memtable -> SSTable (tmp, fsync, rename) -> MANIFEST (tmp, fsync, rename, dir fsync)
               -> publish new State -> delete the frozen WAL
sable-compact: pick newest run of >= N age-adjacent, similar-size tables -> k-way merge -> new SSTable
               -> MANIFEST -> publish new State -> old tables deleted when the last reader releases them
```

## Code map

| File | Role |
|---|---|
| `Sable.java` | Public API, write path, flush, compaction scheduling, recovery |
| `WriteAheadLog.java` | Append-only log, group commit, crash replay |
| `Memtable.java` | Concurrent skip list holding recent writes |
| `SSTable.java` | On-disk table: writer, reader, sparse index, block checksums, ref counting |
| `BloomFilter.java` | Per-table filter (double hashing, about a 1% false-positive rate) |
| `MergingIterator.java` | K-way heap merge, used by both scans and compaction |
| `SizeTiered.java` | Compaction policy |
| `Manifest.java` | Atomic record of the live files |
| `State.java` | Immutable, reference-counted snapshot that readers pin |

## File formats

Directory: `LOCK`, `MANIFEST`, `NNNNNN.wal`, `NNNNNN.sst`. All ids come from one counter.

```
WAL record : [crc32c:int][len:int][type:byte][keyLen:int][key][value]    crc covers len and payload
             replay stops at the first torn or corrupt record

SSTable    : [data block]... [index block] [bloom block] [footer]
data block : ([keyLen:int][valLen:int, -1 = tombstone][key][value])... [crc32c:int]
index block: ([keyLen:int][firstKey][offset:long][length:int])...     [crc32c:int]   one per data block
bloom block: [k:int][bits:long...]                                     [crc32c:int]
footer     : [indexOffset:long][indexLength:int][bloomOffset:long][bloomLength:int][magic:long]

MANIFEST   : [crc32c:int][nextFileId:long][walId:long][count:int][tableId:long]...   newest table first
```

## Design decisions and trade-offs

- **Age-adjacent size-tiered compaction** (as in RocksDB universal compaction). Only neighbouring tables are
  merged, so table order stays a total order by recency and a lookup can stop at the first table that has the key.
  There are no per-entry sequence numbers. The cost is higher write and space amplification than leveled
  compaction (see the overwrite benchmark).
- **Tombstone drop rule.** Compaction drops a tombstone only when the run includes the oldest table. Otherwise an
  older table outside the run could still hold the key, and dropping the tombstone would resurrect it.
- **Lock-free reads.** An immutable `State` (active memtable, frozen memtable, table list) is published through a
  volatile field. Readers pin it with a CAS on a refcount; tables stay open and undeleted until the last pinning
  reader lets go, even after compaction replaced them. Writers, state swaps and manifest writes share one lock.
- **Group commit.** Appends are serialized, but the fsync happens outside the lock: the first waiting writer
  fsyncs for everyone appended so far. Cuts fsyncs per write roughly by the number of concurrent writers.
- **Visibility before durability.** A write can be visible to readers before its fsync completes, but is never
  acknowledged to its writer before. After a crash a reader may have seen a value that is then lost (never one
  that was acknowledged and lost). Fixing it needs sequence numbers and a "durable up to" watermark that reads
  respect.
- **Manifest.** Rewritten whole via temp file, fsync, atomic rename, directory fsync, so it is always the old or the
  new version. While a flush is pending it keeps pointing at the frozen memtable's WAL, otherwise a crash
  between rotation and flush would lose it.
- **fsync failure is fatal.** After a failed fsync Linux may have dropped the dirty pages, so a retry can "succeed"
  without the data on disk. Any WAL append or sync error stops further writes instead of retrying.
- **Orphan cleanup.** On open, `*.tmp` files, SSTables not in the manifest (a crash before the manifest rename)
  and WALs older than the manifest's `walId` are deleted. Surviving WALs are replayed into a new SSTable.
- **Failure policy.** An error in background flush or compaction, or a failed WAL append (which may leave a torn
  record that would hide later ones on replay), poisons the database: later operations throw `IOException`.
- **Trust boundary.** Keys and values are copied on the way in and out, so callers cannot mutate stored data.

## Testing

- `WriteAheadLogTest`: round trip; truncation at every byte offset replays exactly the complete records; a bit
  flip in any byte of record k replays exactly k records.
- `ModelTest`: 20 seeds x 3000 random operations (put, delete, get, bounded and unbounded scans, flush, reopen,
  compactAll) compared with a `TreeMap` oracle, using tiny memtables and blocks so flushes and compactions are constant.
- `ConcurrencyTest`: 8 writers with group commit, 4 readers asserting per-key values never go backwards, a scanner
  asserting strictly increasing keys, while flushes and compactions run.
- `CrashTest`: a child JVM writes with 4 threads and fsync, is SIGKILLed at a random point (including during
  startup and recovery), and the parent reopens the database and checks that every acknowledged put is present,
  every acknowledged delete took effect, and values are intact, across all previous kills too.
  `./gradlew test -PcrashIterations=100` runs more iterations (default 15).

## Benchmarks

Machine: WSL2, ext4, 16 logical CPUs (nproc = 16), 13th Gen Intel Core i7-13620H, 12 GB RAM, OpenJDK 17.
1,000,000 keys, 16-byte keys, 100-byte random values, default options unless stated (4 MiB memtable, 4 KiB blocks,
10 bloom bits per key). Single run, no warm-up control; treat as indicative. fsync on WSL2 is not representative of
bare-metal NVMe.

Durable writes (fsync before ack), 3 s each:

| config | ops/s | writes per fsync |
|---|---|---|
| 1 thread | 411 | 1.0 |
| 16 threads, no group commit | 425 | 1.0 |
| 16 threads, group commit | 3,249 | 8.4 |

Bulk load in random order, no fsync: 277,341 ops/s (3.6 s); 43 flushes, 12 compactions, 7 SSTables at end.

Point reads, 200k random gets:

| workload | ops/s | p50 us | p99 us | block reads/get |
|---|---|---|---|---|
| existing keys | 247,992 | 3.3 | 12.9 | 1.04 |
| missing keys, bloom 10 bits | 1,396,480 | 0.5 | 4.1 | 0.06 |
| missing keys, no bloom | 54,833 | 16.6 | 47.4 | 7.00 |

Range scans, 10k scans of 100 entries from random starts: 22,165 scans/s.

Overwrite 250,000 keys x 5 passes, no fsync (space amp = SSTable bytes / (keys x 116)):

| compaction | sstables | space amp | block reads/get |
|---|---|---|---|
| off | 54 | 5.44 | 1.04 |
| off, then compactAll | 1 | 1.09 | 1.00 |
| auto | 6 | 3.83 | 1.02 |
| auto, then compactAll | 1 | 1.09 | 1.00 |

Reproduce: `./gradlew bench` or `./gradlew bench -Pargs="500000 build/bench"` (numKeys, directory).

## Limitations

- No MVCC snapshots: a scan is not a point-in-time view of the memtable part (SSTables it reads are stable).
- No compression and no block cache; every non-bloom-filtered lookup reads a block through the OS page cache.
- Crash tests use SIGKILL, which tests process crashes, not power loss or lost page-cache writes.
- Directory fsync (opening a directory as a file channel) works on Linux only.
- Single writer lock. A directory can be open once per JVM (checked in-process, because closing any descriptor
  on the lock file would drop the POSIX lock) and once across processes (`LOCK` file).
- Writes stall while a frozen memtable is still being flushed.
