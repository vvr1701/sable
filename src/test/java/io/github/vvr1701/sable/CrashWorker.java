package io.github.vvr1701.sable;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/** Child process for {@link CrashTest}: writes until it is SIGKILLed, printing "thread op" once each op is acknowledged. */
public final class CrashWorker {
    static byte[] key(int thread, long n) {
        return String.format("t%d-%013d", thread, n).getBytes(StandardCharsets.UTF_8);
    }

    /** Derived from the key so the verifier can recompute it. */
    static byte[] value(byte[] key) {
        String k = new String(key, StandardCharsets.UTF_8) + "|";
        return k.repeat(100 / k.length() + 1).substring(0, 100).getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws Exception {
        int threads = Integer.parseInt(args[1]);
        long base = Long.parseLong(args[2]);
        PrintStream acks = new PrintStream(new FileOutputStream(FileDescriptor.out), true);
        Options opts = new Options().memtableBytes(64 << 10).compactionMinTables(3).syncWrites(true).groupCommit(true);
        Sable db = Sable.open(Path.of(args[0]), opts);
        for (int t = 0; t < threads; t++) {
            int tt = t;
            new Thread(() -> {
                try {
                    for (int i = 0; ; i++) {
                        if (i % 5 == 4) {
                            db.delete(key(tt, base + i - 4));
                        } else {
                            byte[] k = key(tt, base + i);
                            db.put(k, value(k));
                        }
                        synchronized (acks) {
                            acks.println(tt + " " + i);
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    Runtime.getRuntime().halt(1);
                }
            }).start();
        }
    }
}
