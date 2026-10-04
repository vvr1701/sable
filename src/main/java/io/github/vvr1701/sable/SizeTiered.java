package io.github.vvr1701.sable;

/**
 * Size-tiered compaction policy, restricted to tables that are adjacent in age (the approach RocksDB's
 * universal compaction takes). Merging only neighbours keeps "the newest table holding a key wins" true,
 * so lookups can stop at the first hit without per-entry sequence numbers. Merging an arbitrary set of
 * similar-size tables could place old data in front of newer data that sat between them.
 */
final class SizeTiered {
    static final int MAX_MERGE = 32;

    private SizeTiered() {}

    /**
     * Given table sizes newest first, returns the [from, to) run to merge: the newest run of at least
     * {@code minTables} consecutive tables, each within 2x of the run's average size. Returns null if there is none.
     */
    static int[] pick(long[] sizes, int minTables) {
        for (int from = 0; from < sizes.length; from++) {
            long total = sizes[from];
            int to = from + 1;
            while (to < sizes.length && to - from < MAX_MERGE) {
                long avg = total / (to - from);
                if (sizes[to] < avg / 2 || sizes[to] > avg * 2) break;
                total += sizes[to++];
            }
            if (to - from >= minTables) return new int[] {from, to};
        }
        return null;
    }
}
