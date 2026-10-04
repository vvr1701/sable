package io.github.vvr1701.sable;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

/**
 * Immutable sorted file of entries.
 *
 * <pre>
 * [data block]... [index block] [bloom block] [footer]
 * data block : ([keyLen:int][valLen:int, -1 = tombstone][key][value])... [crc32c:int]
 * index block: ([keyLen:int][firstKey][offset:long][length:int])...     [crc32c:int]   one entry per data block
 * bloom block: [k:int][bits:long...]                                     [crc32c:int]
 * footer     : [indexOffset:long][indexLength:int][bloomOffset:long][bloomLength:int][magic:long]
 * </pre>
 *
 * The index and bloom filter are loaded into memory on open, so a point lookup costs a bloom check,
 * a binary search over the in-memory index, and at most one block read.
 *
 * <p>Reference counted: each {@link State} that lists the table holds a reference. A table that compaction
 * has replaced is closed and deleted only when the last reader's State lets go of it.
 */
final class SSTable {
    private static final long MAGIC = 0x5341424C45535354L; // "SABLESST"
    private static final int FOOTER = 32;

    private record Block(byte[] firstKey, long offset, int length) {}

    final long id;
    final Path path;
    final long sizeBytes;
    private final FileChannel ch;
    private final Block[] index;
    private final BloomFilter bloom;
    private final Metrics metrics;
    private final AtomicInteger refs = new AtomicInteger();
    private volatile boolean obsolete;

    private SSTable(long id, Path path, FileChannel ch, Block[] index, BloomFilter bloom, Metrics metrics)
            throws IOException {
        this.id = id;
        this.path = path;
        this.sizeBytes = ch.size();
        this.ch = ch;
        this.index = index;
        this.bloom = bloom;
        this.metrics = metrics;
    }

    static String fileName(long id) {
        return String.format("%06d.sst", id);
    }

    /** Writes sorted entries to a new table, or returns null if there are none. Crash-safe: tmp, fsync, rename. */
    static SSTable write(Path dir, long id, Iterator<Entry> entries, Options opts, Metrics metrics)
            throws IOException {
        if (!entries.hasNext()) return null;
        Path tmp = dir.resolve(fileName(id) + ".tmp");
        Path dst = dir.resolve(fileName(id));
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(ch), 1 << 16))) {
            ByteArrayOutputStream blockBytes = new ByteArrayOutputStream(opts.blockBytes * 2);
            DataOutputStream block = new DataOutputStream(blockBytes);
            ByteArrayOutputStream indexBytes = new ByteArrayOutputStream();
            DataOutputStream index = new DataOutputStream(indexBytes);
            long[] hashes = new long[1024];
            int count = 0;
            long offset = 0;
            byte[] firstKey = null;
            while (entries.hasNext()) {
                Entry e = entries.next();
                if (firstKey == null) firstKey = e.key();
                block.writeInt(e.key().length);
                block.writeInt(e.isTombstone() ? -1 : e.value().length);
                block.write(e.key());
                if (!e.isTombstone()) block.write(e.value());
                if (count == hashes.length) hashes = Arrays.copyOf(hashes, count * 2);
                hashes[count++] = BloomFilter.hash(e.key());
                if (blockBytes.size() >= opts.blockBytes || !entries.hasNext()) {
                    index.writeInt(firstKey.length);
                    index.write(firstKey);
                    index.writeLong(offset);
                    index.writeInt(blockBytes.size());
                    offset += writeChecked(out, blockBytes.toByteArray());
                    blockBytes.reset();
                    firstKey = null;
                }
            }
            long indexOffset = offset;
            byte[] indexBlock = indexBytes.toByteArray();
            offset += writeChecked(out, indexBlock);
            byte[] bloomBlock = BloomFilter.build(hashes, count, opts.bloomBitsPerKey).encode();
            long bloomOffset = offset;
            writeChecked(out, bloomBlock);
            out.writeLong(indexOffset);
            out.writeInt(indexBlock.length);
            out.writeLong(bloomOffset);
            out.writeInt(bloomBlock.length);
            out.writeLong(MAGIC);
            out.flush();
            ch.force(true);
        }
        Files.move(tmp, dst, StandardCopyOption.ATOMIC_MOVE);
        Io.syncDir(dir);
        return open(dst, id, metrics);
    }

    private static int writeChecked(DataOutputStream out, byte[] data) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(data);
        out.write(data);
        out.writeInt((int) crc.getValue());
        return data.length + 4;
    }

    static SSTable open(Path path, long id, Metrics metrics) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = ch.size();
            if (size < FOOTER) throw new IOException("truncated sstable " + path);
            ByteBuffer footer = ByteBuffer.allocate(FOOTER);
            readFully(ch, footer, size - FOOTER);
            footer.flip();
            long indexOffset = footer.getLong();
            int indexLength = footer.getInt();
            long bloomOffset = footer.getLong();
            int bloomLength = footer.getInt();
            if (footer.getLong() != MAGIC) throw new IOException("not an sstable: " + path);

            ByteBuffer idx = readChecked(ch, path, indexOffset, indexLength);
            List<Block> blocks = new ArrayList<>();
            while (idx.hasRemaining()) {
                byte[] key = new byte[idx.getInt()];
                idx.get(key);
                blocks.add(new Block(key, idx.getLong(), idx.getInt()));
            }
            BloomFilter bloom = BloomFilter.decode(readChecked(ch, path, bloomOffset, bloomLength));
            return new SSTable(id, path, ch, blocks.toArray(new Block[0]), bloom, metrics);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    /** The value, {@link Entry#TOMBSTONE} if deleted in this table, or null if absent. */
    byte[] get(byte[] key) throws IOException {
        if (!bloom.mightContain(key)) {
            metrics.bloomNegatives.increment();
            return null;
        }
        int b = floorBlock(key);
        if (b < 0) return null;
        metrics.blockReads.increment();
        ByteBuffer block = readBlock(b);
        while (block.hasRemaining()) {
            byte[] k = new byte[block.getInt()];
            int valueLength = block.getInt();
            block.get(k);
            int cmp = Entry.KEY_ORDER.compare(k, key);
            if (cmp > 0) return null; // entries are sorted, so we've passed where the key would be
            if (cmp == 0) {
                if (valueLength < 0) return Entry.TOMBSTONE;
                byte[] value = new byte[valueLength];
                block.get(value);
                return value;
            }
            if (valueLength > 0) block.position(block.position() + valueLength);
        }
        return null;
    }

    /** Entries in [from, to) in key order, tombstones included; a null bound is unbounded. Reads lazily. */
    Iterator<Entry> iterator(byte[] from, byte[] to) {
        return new Iterator<>() {
            private int nextBlock = from == null ? 0 : Math.max(0, floorBlock(from));
            private ByteBuffer block;
            private Entry next = advance();

            private Entry advance() {
                while (true) {
                    if (block == null || !block.hasRemaining()) {
                        if (nextBlock >= index.length) return null;
                        try {
                            block = readBlock(nextBlock++);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    byte[] key = new byte[block.getInt()];
                    int valueLength = block.getInt();
                    block.get(key);
                    byte[] value = valueLength < 0 ? Entry.TOMBSTONE : new byte[valueLength];
                    if (valueLength > 0) block.get(value);
                    if (from != null && Entry.KEY_ORDER.compare(key, from) < 0) continue;
                    if (to != null && Entry.KEY_ORDER.compare(key, to) >= 0) {
                        nextBlock = index.length;
                        block = null;
                        return null;
                    }
                    return new Entry(key, value);
                }
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
        };
    }

    /** Index of the last block whose first key is <= key, or -1 if key sorts before the whole table. */
    private int floorBlock(byte[] key) {
        int lo = 0;
        int hi = index.length - 1;
        int floor = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Entry.KEY_ORDER.compare(index[mid].firstKey, key) <= 0) {
                floor = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return floor;
    }

    private ByteBuffer readBlock(int i) throws IOException {
        return readChecked(ch, path, index[i].offset, index[i].length);
    }

    /** Reads {@code length} bytes plus their trailing CRC with a positional (thread-safe) read, and verifies it. */
    private static ByteBuffer readChecked(FileChannel ch, Path path, long offset, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length + 4);
        readFully(ch, buf, offset);
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 0, length);
        if ((int) crc.getValue() != buf.getInt(length)) {
            throw new IOException("checksum mismatch in " + path + " at offset " + offset);
        }
        return buf.flip().limit(length);
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long offset) throws IOException {
        while (buf.hasRemaining()) {
            if (ch.read(buf, offset + buf.position()) < 0) throw new EOFException("unexpected end of " + ch);
        }
    }

    void retain() {
        refs.incrementAndGet();
    }

    void release() {
        if (refs.decrementAndGet() != 0) return;
        try {
            ch.close();
            if (obsolete) Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // A file we failed to delete is no longer in the manifest, so the next open removes it.
        }
    }

    /** Called once compaction has replaced this table: delete the file when the last reader is done. */
    void markObsolete() {
        obsolete = true;
    }
}
