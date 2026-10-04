package io.github.vvr1701.sable;

import java.util.concurrent.atomic.LongAdder;

/** Engine counters. LongAdder keeps hot-path increments cheap under contention. */
final class Metrics {
    final LongAdder writes = new LongAdder();
    final LongAdder walSyncs = new LongAdder();
    final LongAdder gets = new LongAdder();
    final LongAdder blockReads = new LongAdder();
    final LongAdder bloomNegatives = new LongAdder();
    final LongAdder flushes = new LongAdder();
    final LongAdder compactions = new LongAdder();
    final LongAdder writeStalls = new LongAdder();
}
