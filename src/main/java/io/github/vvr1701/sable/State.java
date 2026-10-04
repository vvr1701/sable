package io.github.vvr1701.sable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An immutable snapshot of where data lives: the active memtable, the one being flushed, and the SSTables.
 * Flushes and compactions publish a new State instead of mutating this one. A reader pins the current State
 * (one CAS) and reads without locks; the tables it pins stay open until it releases them, even if compaction
 * has already replaced them.
 */
final class State {
    final Memtable mem;
    final Memtable imm;           // memtable being flushed, or null
    final List<SSTable> tables;   // newest first
    private final AtomicInteger refs = new AtomicInteger(1); // the engine's own reference to the current State

    State(Memtable mem, Memtable imm, List<SSTable> tables) {
        this.mem = mem;
        this.imm = imm;
        this.tables = List.copyOf(tables);
        this.tables.forEach(SSTable::retain);
    }

    /** Pins this State unless it has already been fully released (it was replaced and every reader finished). */
    boolean tryRetain() {
        for (int r = refs.get(); r > 0; r = refs.get()) {
            if (refs.compareAndSet(r, r + 1)) return true;
        }
        return false;
    }

    void release() {
        if (refs.decrementAndGet() == 0) tables.forEach(SSTable::release);
    }
}
