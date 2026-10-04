package io.github.vvr1701.sable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConcurrencyTest {
    static final int WRITERS = 8;
    static final int WRITES = 2000;
    static final int KEYS = 200;

    @TempDir Path dir;

    static byte[] key(int t, int k) {
        return ("w" + t + "-" + k).getBytes(StandardCharsets.UTF_8);
    }

    static byte[] value(long i) {
        return ByteBuffer.allocate(8).putLong(i).array();
    }

    @Test
    void readersAndScannersSeeConsistentDataWhileWritersFlushAndCompact() throws Exception {
        Options opts = new Options().memtableBytes(16 << 10).compactionMinTables(3).syncWrites(true).groupCommit(true);
        Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicInteger writersLeft = new AtomicInteger(WRITERS);
        List<Thread> threads = new ArrayList<>();
        try (Sable db = Sable.open(dir, opts)) {
            for (int t = 0; t < WRITERS; t++) {
                int tt = t;
                threads.add(thread(errors, () -> {
                    try {
                        for (int i = 0; i < WRITES; i++) db.put(key(tt, i % KEYS), value(i));
                    } finally {
                        writersLeft.decrementAndGet();
                    }
                }));
            }
            for (int r = 0; r < 4; r++) {
                threads.add(thread(errors, () -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    Map<String, Long> last = new HashMap<>(); // values per key only ever increase
                    while (writersLeft.get() > 0) {
                        byte[] k = key(rnd.nextInt(WRITERS), rnd.nextInt(KEYS));
                        byte[] v = db.get(k);
                        if (v == null) continue;
                        long now = ByteBuffer.wrap(v).getLong();
                        Long before = last.put(new String(k), now);
                        assertTrue(before == null || before <= now, new String(k) + " went from " + before + " to " + now);
                    }
                }));
            }
            threads.add(thread(errors, () -> {
                while (writersLeft.get() > 0) {
                    byte[] prev = null;
                    try (Sable.Scan scan = db.scan(null, null)) {
                        while (scan.hasNext()) {
                            byte[] k = scan.next().getKey();
                            assertTrue(prev == null || Arrays.compareUnsigned(prev, k) < 0, "scan out of order");
                            prev = k;
                        }
                    }
                }
            }));
            for (Thread th : threads) th.start();
            for (Thread th : threads) th.join();
            assertTrue(errors.isEmpty(), () -> "thread failures: " + errors);

            for (int t = 0; t < WRITERS; t++) {
                for (int k = 0; k < KEYS; k++) {
                    assertArrayEquals(value(WRITES - KEYS + k), db.get(key(t, k)), "w" + t + "-" + k);
                }
            }
            db.awaitCompactions();
            Sable.Stats s = db.stats();
            assertEquals((long) WRITERS * WRITES, s.writes());
            assertTrue(s.flushes() > 0, "no flush happened");
            assertTrue(s.compactions() > 0, "no compaction happened");
        }
    }

    interface Body {
        void run() throws Exception;
    }

    static Thread thread(Queue<Throwable> errors, Body body) {
        return new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                errors.add(e);
            }
        });
    }
}
