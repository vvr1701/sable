package io.github.vvr1701.sable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * The durable record of which files make up the database: live SSTables newest first, the oldest WAL still
 * needed, and the next file id. Every flush and compaction rewrites it with write-to-temp, fsync, then an
 * atomic rename, so after a crash it is always either the old version or the new one, never a mix.
 *
 * <p>Format: {@code [crc32c:int][nextFileId:long][walId:long][count:int][tableId:long]...}
 */
record Manifest(long nextFileId, long walId, List<Long> tableIds) {
    static final String NAME = "MANIFEST";

    /** Returns the saved manifest, or null for a new database. */
    static Manifest load(Path dir) throws IOException {
        Path path = dir.resolve(NAME);
        if (!Files.exists(path)) return null;
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(path));
        if (buf.remaining() < 24) throw new IOException("corrupt manifest: " + path);
        int expected = buf.getInt();
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 4, buf.limit() - 4);
        if ((int) crc.getValue() != expected) throw new IOException("corrupt manifest: " + path);
        long nextFileId = buf.getLong();
        long walId = buf.getLong();
        List<Long> ids = new ArrayList<>();
        for (int i = buf.getInt(); i > 0; i--) ids.add(buf.getLong());
        return new Manifest(nextFileId, walId, ids);
    }

    void save(Path dir) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 8 + 4 + 8 * tableIds.size());
        buf.position(4);
        buf.putLong(nextFileId).putLong(walId).putInt(tableIds.size());
        for (long id : tableIds) buf.putLong(id);
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 4, buf.capacity() - 4);
        buf.putInt(0, (int) crc.getValue());
        buf.flip();

        Path tmp = dir.resolve(NAME + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);
        }
        Files.move(tmp, dir.resolve(NAME), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        Io.syncDir(dir);
    }
}
