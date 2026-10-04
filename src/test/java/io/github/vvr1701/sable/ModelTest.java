package io.github.vvr1701.sable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.io.TempDir;

/** Random operations against a TreeMap oracle, with small memtables and blocks so every layer gets exercised. */
class ModelTest {
    @TempDir Path dir;

    static byte[] key(int i) {
        return String.format("k%03d", i).getBytes(StandardCharsets.UTF_8);
    }

    @RepeatedTest(20)
    void behavesLikeATreeMap(RepetitionInfo info) throws IOException {
        long seed = info.getCurrentRepetition();
        Random rnd = new Random(seed);
        Options opts = new Options().memtableBytes(4096).blockBytes(256).compactionMinTables(3).syncWrites(false);
        TreeMap<byte[], byte[]> model = new TreeMap<>(Arrays::compareUnsigned);
        Sable db = Sable.open(dir, opts);
        int op = 0;
        try {
            for (; op < 3000; op++) {
                int r = rnd.nextInt(100);
                byte[] k = key(rnd.nextInt(500));
                if (r < 45) {
                    byte[] v = new byte[rnd.nextInt(41)];
                    rnd.nextBytes(v);
                    db.put(k, v);
                    model.put(k, v);
                } else if (r < 60) {
                    db.delete(k);
                    model.remove(k);
                } else if (r < 75) {
                    assertArrayEquals(model.get(k), db.get(k), "get " + new String(k));
                } else if (r < 85) {
                    byte[] from = rnd.nextBoolean() ? null : key(rnd.nextInt(500));
                    byte[] to = rnd.nextBoolean() ? null : key(rnd.nextInt(500));
                    if (from != null && to != null && Arrays.compareUnsigned(from, to) > 0) {
                        byte[] t = from;
                        from = to;
                        to = t;
                    }
                    compareScan(model, db, from, to);
                } else if (r < 88) {
                    db.flush();
                } else if (r < 91) {
                    db.close();
                    db = Sable.open(dir, opts);
                } else if (r < 94) {
                    db.compactAll();
                } else {
                    assertNull(db.get("absent".getBytes(StandardCharsets.UTF_8)));
                }
            }
            op = -1;
            compareScan(model, db, null, null);
            db.close();
            db = Sable.open(dir, opts);
            compareScan(model, db, null, null);
        } catch (AssertionError | IOException | RuntimeException e) {
            throw new AssertionError("seed=" + seed + " op=" + op + ": " + e, e);
        } finally {
            db.close();
        }
    }

    static void compareScan(TreeMap<byte[], byte[]> model, Sable db, byte[] from, byte[] to) {
        NavigableMap<byte[], byte[]> range = model;
        if (from != null) range = range.tailMap(from, true);
        if (to != null) range = range.headMap(to, false);
        List<Map.Entry<byte[], byte[]>> expected = new ArrayList<>(range.entrySet());
        try (Sable.Scan scan = db.scan(from, to)) {
            for (Map.Entry<byte[], byte[]> e : expected) {
                assertTrue(scan.hasNext(), "scan ended early before " + new String(e.getKey()));
                Map.Entry<byte[], byte[]> got = scan.next();
                assertArrayEquals(e.getKey(), got.getKey());
                assertArrayEquals(e.getValue(), got.getValue(), "value of " + new String(e.getKey()));
            }
            assertFalse(scan.hasNext(), "scan returned extra entries");
        }
    }
}
