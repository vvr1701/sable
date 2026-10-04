package io.github.vvr1701.sable;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/** Sorted in-memory buffer of recent writes. Reads are lock-free; writes are serialized by {@link Sable}'s lock. */
final class Memtable {
    // Rough cost of a skip-list node, so the flush threshold tracks heap use rather than just payload bytes.
    private static final int NODE_OVERHEAD = 64;

    private final ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(Entry.KEY_ORDER);
    private volatile long bytes;

    void put(byte[] key, byte[] value) {
        byte[] old = map.put(key, value);
        bytes += old == null ? key.length + value.length + NODE_OVERHEAD : value.length - old.length;
    }

    /** The value, {@link Entry#TOMBSTONE} if deleted here, or null if this memtable has never seen the key. */
    byte[] get(byte[] key) {
        return map.get(key);
    }

    long approximateBytes() {
        return bytes;
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    /** Entries in [from, to) in key order; a null bound is unbounded. Weakly consistent under concurrent writes. */
    Iterator<Entry> iterator(byte[] from, byte[] to) {
        NavigableMap<byte[], byte[]> range = map;
        if (from != null) range = range.tailMap(from, true);
        if (to != null) range = range.headMap(to, false);
        return range.entrySet().stream().map(e -> new Entry(e.getKey(), e.getValue())).iterator();
    }
}
