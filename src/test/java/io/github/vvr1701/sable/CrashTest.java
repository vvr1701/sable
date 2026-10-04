package io.github.vvr1701.sable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** SIGKILLs a writing process at random moments and checks that recovery keeps every acknowledged write. */
class CrashTest {
    static final int THREADS = 4;

    @TempDir Path dir;

    @Test
    void acknowledgedWritesSurviveKill() throws Exception {
        int iterations = Integer.getInteger("sable.crash.iterations", 15);
        Random rnd = new Random(42);
        List<long[]> acked = new ArrayList<>(); // per iteration: {base, maxAck of thread 0..THREADS-1}
        Options opts = new Options().memtableBytes(64 << 10).compactionMinTables(3).syncWrites(true).groupCommit(true);
        for (int iter = 0; iter < iterations; iter++) {
            long base = iter * 1_000_000_000L;
            long[] maxAck = new long[THREADS + 1];
            maxAck[0] = base;
            Arrays.fill(maxAck, 1, THREADS + 1, -1);
            Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                    System.getProperty("java.class.path"), "io.github.vvr1701.sable.CrashWorker", dir.toString(),
                    String.valueOf(THREADS), String.valueOf(base))
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            Thread reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    for (String line; (line = in.readLine()) != null; ) {
                        String[] f = line.split(" ");
                        if (f.length == 2) { // a line cut off by the kill may be a prefix; prefixes only understate
                            int t = Integer.parseInt(f[0]);
                            maxAck[t + 1] = Math.max(maxAck[t + 1], Long.parseLong(f[1]));
                        }
                    }
                } catch (Exception ignored) {
                    // unparseable tail from the kill
                }
            });
            reader.start();
            Thread.sleep(200 + rnd.nextInt(1300));
            assertTrue(p.isAlive(), "worker died on its own in iteration " + iter);
            p.destroyForcibly().waitFor();
            reader.join();
            acked.add(maxAck);
            verify(opts, acked, iter);
        }
    }

    void verify(Options opts, List<long[]> acked, int iter) throws Exception {
        try (Sable db = Sable.open(dir, opts)) {
            for (int it = 0; it < acked.size(); it++) {
                long[] a = acked.get(it);
                for (int t = 0; t < THREADS; t++) {
                    for (long i = 0; i <= a[t + 1]; i++) {
                        if (i % 5 == 4) continue; // delete ops
                        byte[] k = CrashWorker.key(t, a[0] + i);
                        byte[] got = db.get(k);
                        String msg = "after kill " + iter + ", iteration " + it + " thread " + t + " op " + i;
                        boolean deletedForSure = i % 5 == 0 && i + 4 <= a[t + 1];
                        if (deletedForSure) assertNull(got, msg + " should be deleted");
                        else if (i % 5 == 0 && i + 4 > a[t + 1]) {
                            if (got != null) assertArrayEquals(CrashWorker.value(k), got, msg);
                        } else assertArrayEquals(CrashWorker.value(k), got, msg);
                    }
                }
            }
        }
    }
}
