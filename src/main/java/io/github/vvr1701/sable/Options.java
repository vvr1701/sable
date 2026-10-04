package io.github.vvr1701.sable;

/** Tuning knobs for {@link Sable}. Defaults suit general use; tests and benchmarks shrink them. */
public final class Options {
    int memtableBytes = 4 << 20;
    int blockBytes = 4 << 10;
    int bloomBitsPerKey = 10;
    boolean syncWrites = true;
    boolean groupCommit = true;
    boolean autoCompaction = true;
    int compactionMinTables = 4;

    /** Memtable size that triggers a flush to an SSTable. */
    public Options memtableBytes(int bytes) {
        memtableBytes = bytes;
        return this;
    }

    /** Target uncompressed size of an SSTable data block. */
    public Options blockBytes(int bytes) {
        blockBytes = bytes;
        return this;
    }

    /** Bloom filter size per key; 10 gives about a 1% false-positive rate, 0 disables the filter. */
    public Options bloomBitsPerKey(int bits) {
        bloomBitsPerKey = bits;
        return this;
    }

    /** fsync the write-ahead log before a write returns. Off trades crash durability for speed. */
    public Options syncWrites(boolean sync) {
        syncWrites = sync;
        return this;
    }

    /** Let concurrent writers share one fsync. Off means one fsync per write, serialized. */
    public Options groupCommit(boolean enabled) {
        groupCommit = enabled;
        return this;
    }

    /** Run size-tiered compaction in the background after flushes. */
    public Options autoCompaction(boolean enabled) {
        autoCompaction = enabled;
        return this;
    }

    /** How many similar-size, age-adjacent tables trigger a merge. */
    public Options compactionMinTables(int tables) {
        if (tables < 2) throw new IllegalArgumentException("compactionMinTables must be >= 2");
        compactionMinTables = tables;
        return this;
    }
}
