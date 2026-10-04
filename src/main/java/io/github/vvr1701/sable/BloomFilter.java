package io.github.vvr1701.sable;

import java.nio.ByteBuffer;

/**
 * Bloom filter over SSTable keys. "No" is always correct, so a negative answer skips the table's disk read.
 * Uses Kirsch-Mitzenmacher double hashing: k probe positions derived from the two halves of one 64-bit hash.
 */
final class BloomFilter {
    private final long[] bits;
    private final int k;

    private BloomFilter(long[] bits, int k) {
        this.bits = bits;
        this.k = k;
    }

    static BloomFilter build(long[] hashes, int count, int bitsPerKey) {
        if (bitsPerKey <= 0) return new BloomFilter(new long[0], 0);
        long nbits = Math.max(64, (long) count * bitsPerKey);
        // k = bitsPerKey * ln 2 minimizes the false-positive rate for a given size.
        int k = (int) Math.max(1, Math.min(30, Math.round(bitsPerKey * Math.log(2))));
        BloomFilter filter = new BloomFilter(new long[(int) ((nbits + 63) / 64)], k);
        for (int i = 0; i < count; i++) filter.add(hashes[i]);
        return filter;
    }

    boolean mightContain(byte[] key) {
        if (k == 0) return true;
        long h = hash(key);
        long nbits = bits.length * 64L;
        long h1 = (int) h;
        long h2 = (int) (h >>> 32);
        for (int i = 0; i < k; i++) {
            long bit = Math.floorMod(h1 + i * h2, nbits);
            if ((bits[(int) (bit >>> 6)] & (1L << bit)) == 0) return false;
        }
        return true;
    }

    private void add(long h) {
        long nbits = bits.length * 64L;
        long h1 = (int) h;
        long h2 = (int) (h >>> 32);
        for (int i = 0; i < k; i++) {
            long bit = Math.floorMod(h1 + i * h2, nbits);
            bits[(int) (bit >>> 6)] |= 1L << bit;
        }
    }

    /** FNV-1a, then MurmurHash3's fmix64 finalizer so both 32-bit halves are well mixed. */
    static long hash(byte[] key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h ^= b & 0xff;
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(4 + bits.length * 8).putInt(k);
        for (long word : bits) buf.putLong(word);
        return buf.array();
    }

    static BloomFilter decode(ByteBuffer buf) {
        int k = buf.getInt();
        long[] bits = new long[buf.remaining() / 8];
        for (int i = 0; i < bits.length; i++) bits[i] = buf.getLong();
        return new BloomFilter(bits, k);
    }
}
