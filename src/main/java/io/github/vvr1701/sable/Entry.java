package io.github.vvr1701.sable;

import java.util.Arrays;
import java.util.Comparator;

/** A key and its value. A value of {@link #TOMBSTONE} (compared by identity) marks a delete. */
record Entry(byte[] key, byte[] value) {
    static final byte[] TOMBSTONE = new byte[0];
    static final Comparator<byte[]> KEY_ORDER = Arrays::compareUnsigned;

    boolean isTombstone() {
        return value == TOMBSTONE;
    }
}
