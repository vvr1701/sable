package io.github.vvr1701.sable;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * K-way merge of sorted sources through a min-heap ordered by (key, source age). When several sources hold
 * the same key, the newest one (lowest index) wins and the older versions are skipped.
 * Shared by range scans (which hide tombstones) and compaction (which may need to keep them).
 */
final class MergingIterator implements Iterator<Entry> {
    private record Head(Entry entry, int age, Iterator<Entry> rest) {}

    private final PriorityQueue<Head> heap = new PriorityQueue<>((a, b) -> {
        int c = Entry.KEY_ORDER.compare(a.entry.key(), b.entry.key());
        return c != 0 ? c : Integer.compare(a.age, b.age);
    });
    private final boolean skipTombstones;
    private Entry next;

    MergingIterator(List<Iterator<Entry>> newestFirst, boolean skipTombstones) {
        this.skipTombstones = skipTombstones;
        for (int i = 0; i < newestFirst.size(); i++) push(newestFirst.get(i), i);
        next = advance();
    }

    private void push(Iterator<Entry> source, int age) {
        if (source.hasNext()) heap.add(new Head(source.next(), age, source));
    }

    private Entry advance() {
        while (!heap.isEmpty()) {
            Head top = heap.poll();
            push(top.rest, top.age);
            while (!heap.isEmpty() && Entry.KEY_ORDER.compare(heap.peek().entry.key(), top.entry.key()) == 0) {
                Head older = heap.poll();
                push(older.rest, older.age);
            }
            if (!(skipTombstones && top.entry.isTombstone())) return top.entry;
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Entry next() {
        if (next == null) throw new NoSuchElementException();
        Entry e = next;
        next = advance();
        return e;
    }
}
