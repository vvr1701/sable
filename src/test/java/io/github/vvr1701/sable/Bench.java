package io.github.vvr1701.sable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/** Benchmarks printed as markdown. Usage: gradlew bench -Pargs="[numKeys=1000000] [dir=build/bench]". */
public final class Bench {
    static final int ENTRY_BYTES = 16 + 100;

    static byte[] key(long i) {
        return String.format("%016d", i).getBytes(StandardCharsets.UTF_8);
    }

    static byte[] value(Random rnd) {
        byte[] v = new byte[100];
        rnd.nextBytes(v);
        return v;
    }

    static Path fresh(Path root, String name) throws IOException {
        Path p = root.resolve(name);
        if (Files.exists(p)) {
            try (Stream<Path> s = Files.walk(p)) {
                for (Path f : s.sorted(Comparator.reverseOrder()).toList()) Files.delete(f);
            }
        }
        return p;
    }

    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        Path root = Path.of(args.length > 1 ? args[1] : "build/bench");
        System.out.printf("n = %,d keys, %d-byte entries%n%n", n, ENTRY_BYTES);

        System.out.println("### Durable writes (fsync before ack), 3 s each\n");
        System.out.println("| config | ops/s | writes per fsync |\n|---|---|---|");
        durable(root, "1 thread", 1, true);
        durable(root, "16 threads, no group commit", 16, false);
        durable(root, "16 threads, group commit", 16, true);

        System.out.println("\n### Bulk load, random order, no fsync\n");
        Sable db = Sable.open(fresh(root, "main"), new Options().syncWrites(false));
        long t0 = System.nanoTime();
        load(db, n);
        double loadSecs = (System.nanoTime() - t0) / 1e9;
        db.flush();
        db.awaitCompactions();
        Sable.Stats st = db.stats();
        System.out.printf("%,.0f ops/s (%.1f s), %d flushes, %d compactions, %d write stalls, %d sstables at end%n",
                n / loadSecs, loadSecs, st.flushes(), st.compactions(), st.writeStalls(), st.sstables());

        System.out.println("\n### Point reads, 200k random gets\n");
        System.out.println("| workload | ops/s | p50 us | p99 us | block reads/get |\n|---|---|---|---|---|");
        gets(db, "existing keys", n, 200_000, false);
        gets(db, "missing keys, bloom 10 bits", n, 200_000, true);
        db.close();
        Sable noBloom = Sable.open(fresh(root, "nobloom"), new Options().syncWrites(false).bloomBitsPerKey(0));
        load(noBloom, n);
        noBloom.flush();
        noBloom.awaitCompactions();
        gets(noBloom, "missing keys, no bloom", n, 200_000, true);
        noBloom.close();

        System.out.println("\n### Range scans, 10k scans of 100 entries\n");
        db = Sable.open(root.resolve("main"), new Options().syncWrites(false));
        Random rnd = new Random(3);
        t0 = System.nanoTime();
        long seen = 0;
        for (int i = 0; i < 10_000; i++) {
            try (Sable.Scan scan = db.scan(key(2L * rnd.nextInt(n - 100)), null)) {
                for (int j = 0; j < 100 && scan.hasNext(); j++, seen++) scan.next();
            }
        }
        System.out.printf("%,.0f scans/s (%,d entries returned)%n", 10_000 / ((System.nanoTime() - t0) / 1e9), seen);
        db.close();

        System.out.println("\n### Overwrite " + n / 4 + " keys x 5 passes, no fsync\n");
        System.out.println("| compaction | sstables | space amp | block reads/get |\n|---|---|---|---|");
        overwrite(root, n, false);
        overwrite(root, n, true);
    }

    static void durable(Path root, String label, int threads, boolean group) throws Exception {
        try (Sable db = Sable.open(fresh(root, "durable"), new Options().syncWrites(true).groupCommit(group))) {
            AtomicLong counter = new AtomicLong();
            long deadline = System.nanoTime() + 3_000_000_000L;
            List<Thread> ts = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                Thread th = new Thread(() -> {
                    Random rnd = new Random();
                    try {
                        while (System.nanoTime() < deadline) db.put(key(2 * counter.getAndIncrement()), value(rnd));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                ts.add(th);
                th.start();
            }
            for (Thread th : ts) th.join();
            Sable.Stats s = db.stats();
            System.out.printf("| %s | %,.0f | %.1f |%n", label, s.writes() / 3.0, (double) s.writes() / s.walSyncs());
        }
    }

    /** Inserts key(2i) for i in [0, n) in random order; odd keys stay absent so they are in-range misses. */
    static void load(Sable db, int n) throws IOException {
        int[] order = new int[n];
        for (int i = 0; i < n; i++) order[i] = i;
        Random rnd = new Random(1);
        for (int i = n - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int t = order[i];
            order[i] = order[j];
            order[j] = t;
        }
        for (int i : order) db.put(key(2L * i), value(rnd));
    }

    static void gets(Sable db, String label, int n, int count, boolean missing) throws IOException {
        Random rnd = new Random(2);
        long[] nanos = new long[count];
        byte[][] keys = new byte[count][];
        for (int i = 0; i < count; i++) keys[i] = key(2L * rnd.nextInt(n) + (missing ? 1 : 0));
        long blocksBefore = db.stats().blockReads();
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            long t = System.nanoTime();
            byte[] v = db.get(keys[i]);
            nanos[i] = System.nanoTime() - t;
            if ((v == null) != missing) throw new AssertionError("wrong answer for " + new String(keys[i]));
        }
        double secs = (System.nanoTime() - start) / 1e9;
        Arrays.sort(nanos);
        System.out.printf("| %s | %,.0f | %.1f | %.1f | %.2f |%n", label, count / secs, nanos[count / 2] / 1e3,
                nanos[(int) (count * 0.99)] / 1e3, (double) (db.stats().blockReads() - blocksBefore) / count);
    }

    static void overwrite(Path root, int n, boolean auto) throws Exception {
        int keys = n / 4;
        try (Sable db = Sable.open(fresh(root, "overwrite"), new Options().syncWrites(false).autoCompaction(auto))) {
            Random rnd = new Random(4);
            for (int pass = 0; pass < 5; pass++) {
                List<Integer> order = new ArrayList<>(java.util.stream.IntStream.range(0, keys).boxed().toList());
                java.util.Collections.shuffle(order, rnd);
                for (int i : order) db.put(key(2L * i), value(rnd));
            }
            db.flush();
            db.awaitCompactions();
            report(db, auto ? "auto" : "off", keys);
            db.compactAll();
            report(db, auto ? "auto, then compactAll" : "off, then compactAll", keys);
        }
    }

    static void report(Sable db, String label, int keys) throws IOException {
        Random rnd = new Random(5);
        long before = db.stats().blockReads();
        for (int i = 0; i < 50_000; i++) db.get(key(2L * rnd.nextInt(keys)));
        Sable.Stats s = db.stats();
        System.out.printf("| %s | %d | %.2f | %.2f |%n", label, s.sstables(), (double) s.sstableBytes() / ((long) keys * ENTRY_BYTES),
                (double) (s.blockReads() - before) / 50_000);
    }
}
