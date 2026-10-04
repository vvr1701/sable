package io.github.vvr1701.sable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAheadLogTest {
    @TempDir Path dir;
    Path file;
    List<Long> ends = new ArrayList<>(); // log offset just past each record
    List<byte[]> keys = new ArrayList<>();

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @BeforeEach
    void writeRecords() throws IOException {
        WriteAheadLog wal = WriteAheadLog.create(dir, 1, new Metrics());
        file = wal.path;
        for (int i = 0; i < 20; i++) {
            byte[] key = b("key" + i);
            keys.add(key);
            ends.add(wal.append(key, i % 4 == 3 ? Entry.TOMBSTONE : b("value-" + "x".repeat(i))));
        }
        wal.close();
    }

    @Test
    void roundTripPutsAndDeletes() throws IOException {
        List<byte[]> gotKeys = new ArrayList<>();
        List<byte[]> gotValues = new ArrayList<>();
        int n = WriteAheadLog.replay(file, (k, v) -> {
            gotKeys.add(k);
            gotValues.add(v);
        });
        assertEquals(20, n);
        for (int i = 0; i < 20; i++) {
            assertArrayEquals(keys.get(i), gotKeys.get(i));
            if (i % 4 == 3) assertSame(Entry.TOMBSTONE, gotValues.get(i));
            else assertArrayEquals(b("value-" + "x".repeat(i)), gotValues.get(i));
        }
    }

    @Test
    void tornTailKeepsOnlyCompleteRecords() throws IOException {
        byte[] all = Files.readAllBytes(file);
        assertEquals(all.length, ends.get(ends.size() - 1));
        Path copy = dir.resolve("torn.wal");
        for (int cut = 0; cut <= all.length; cut++) {
            long len = cut;
            Files.write(copy, Arrays.copyOf(all, cut));
            int expected = (int) ends.stream().filter(e -> e <= len).count();
            assertEquals(expected, WriteAheadLog.replay(copy, (k, v) -> {}), "cut=" + cut);
        }
    }

    @Test
    void corruptionStopsReplayAtTheBadRecord() throws IOException {
        byte[] all = Files.readAllBytes(file);
        Path copy = dir.resolve("flipped.wal");
        for (int k = 0; k < ends.size(); k++) {
            long start = k == 0 ? 0 : ends.get(k - 1);
            for (long pos = start; pos < ends.get(k); pos++) {
                byte[] bad = all.clone();
                bad[(int) pos] ^= 0x10;
                Files.write(copy, bad);
                assertEquals(k, WriteAheadLog.replay(copy, (key, v) -> {}), "record " + k + " byte " + pos);
            }
        }
    }
}
