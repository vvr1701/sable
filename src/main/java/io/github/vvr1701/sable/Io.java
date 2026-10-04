package io.github.vvr1701.sable;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class Io {
    private Io() {}

    /**
     * fsyncs a directory so file creations, renames and deletions inside it survive a crash.
     * fsyncing a file alone does not make its directory entry durable.
     */
    static void syncDir(Path dir) throws IOException {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }

    /** Parses the numeric id from names like {@code 000042.sst}. */
    static long idOf(Path file) {
        String name = file.getFileName().toString();
        return Long.parseLong(name.substring(0, name.indexOf('.')));
    }
}
