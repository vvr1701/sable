package io.github.vvr1701.sable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.BiConsumer;
import java.util.zip.CRC32C;

/**
 * Append-only log of writes not yet in an SSTable.
 *
 * <p>Record: {@code [crc32c:int][len:int][type:byte][keyLen:int][key][value]}. The CRC covers the length
 * and payload, so replay can tell a torn or corrupt tail from a valid record.
 *
 * <p>Group commit: {@link #append} is serialized by the caller, while {@link #sync} is not. The first writer
 * to find no fsync in progress becomes the leader and fsyncs everything appended so far, covering every
 * writer queued behind it; the rest wait and return once their offset is durable.
 */
final class WriteAheadLog implements Closeable {
    private static final int HEADER = 8;
    private static final byte PUT = 1;
    private static final byte DELETE = 2;

    final long id;
    final Path path;
    private final FileChannel ch;
    private final Metrics metrics;
    private volatile long written; // bytes appended; appends are serialized by Sable's lock
    private long synced;           // guarded by this
    private boolean syncing;       // guarded by this

    private WriteAheadLog(long id, Path path, FileChannel ch, Metrics metrics) {
        this.id = id;
        this.path = path;
        this.ch = ch;
        this.metrics = metrics;
    }

    static String fileName(long id) {
        return String.format("%06d.wal", id);
    }

    static WriteAheadLog create(Path dir, long id, Metrics metrics) throws IOException {
        Path path = dir.resolve(fileName(id));
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        Io.syncDir(dir);
        return new WriteAheadLog(id, path, ch, metrics);
    }

    /** Appends a record (not yet durable) and returns the log offset just past it. */
    long append(byte[] key, byte[] value) throws IOException {
        boolean delete = value == Entry.TOMBSTONE;
        int len = 1 + 4 + key.length + (delete ? 0 : value.length);
        ByteBuffer buf = ByteBuffer.allocate(HEADER + len);
        buf.position(HEADER);
        buf.put(delete ? DELETE : PUT).putInt(key.length).put(key);
        if (!delete) buf.put(value);
        buf.putInt(4, len);
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 4, 4 + len);
        buf.putInt(0, (int) crc.getValue());
        buf.flip();
        while (buf.hasRemaining()) ch.write(buf);
        written += HEADER + len;
        return written;
    }

    /** Blocks until everything up to {@code offset} is on disk. */
    void sync(long offset) throws IOException {
        synchronized (this) {
            while (synced < offset && syncing) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("interrupted waiting for WAL sync");
                }
            }
            if (synced >= offset) return;
            syncing = true;
        }
        long target = written; // read before force(): every byte counted here is already in the file
        boolean ok = false;
        try {
            ch.force(false);
            metrics.walSyncs.increment();
            ok = true;
        } finally {
            synchronized (this) {
                syncing = false;
                if (ok) synced = Math.max(synced, target);
                notifyAll(); // on failure a waiter takes over as leader and retries
            }
        }
    }

    /** Makes every appended record durable, then closes. Later sync() calls return immediately. */
    @Override
    public void close() throws IOException {
        sync(written);
        ch.close();
    }

    /**
     * Feeds each intact record to {@code sink} in order, stopping at the first torn or corrupt one:
     * a crash mid-append leaves a partial record at the tail, and everything before it is still valid.
     * Returns the number of records replayed.
     */
    static int replay(Path path, BiConsumer<byte[], byte[]> sink) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(path));
        CRC32C crc = new CRC32C();
        int records = 0;
        while (buf.remaining() >= HEADER) {
            int start = buf.position();
            int expected = buf.getInt();
            int len = buf.getInt();
            if (len < 5 || len > buf.remaining()) break;
            crc.reset();
            crc.update(buf.array(), start + 4, 4 + len);
            if ((int) crc.getValue() != expected) break;
            byte type = buf.get();
            byte[] key = new byte[buf.getInt()];
            buf.get(key);
            byte[] value = type == DELETE ? Entry.TOMBSTONE : new byte[len - 5 - key.length];
            if (type != DELETE) buf.get(value);
            sink.accept(key, value);
            records++;
        }
        return records;
    }
}
