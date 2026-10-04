package io.github.vvr1701.sable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * An embedded, persistent, ordered key-value store: a log-structured merge tree with a write-ahead log,
 * SSTables, bloom filters and background size-tiered compaction. Keys and values are byte arrays, ordered
 * as unsigned bytes. Thread-safe; one process may have a directory open at a time.
 */
public final class Sable implements Closeable {
    // Closing any channel on a file drops the whole JVM's POSIX lock on it, so same-JVM double opens are caught here first.
    private static final Set<Path> OPEN = ConcurrentHashMap.newKeySet();

    private final Path dir;
    private final Options opts;
    private final Metrics metrics = new Metrics();
    private final FileChannel lockChannel; // closing it releases the directory lock
    private final ExecutorService flusher = Executors.newSingleThreadExecutor(daemon("sable-flush"));
    private final ExecutorService compactor = Executors.newSingleThreadExecutor(daemon("sable-compact"));

    // ponytail: one lock serializes writers, state swaps and manifest writes; reads never take it
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition flushed = lock.newCondition();
    private volatile State state;
    private WriteAheadLog wal;
    private WriteAheadLog immWal; // log of the memtable being flushed, or null
    private long nextFileId;
    private boolean compactionScheduled;
    private Exception backgroundError;
    private volatile boolean closed;

    public static Sable open(Path dir) throws IOException {
        return open(dir, new Options());
    }

    public static Sable open(Path dir, Options opts) throws IOException {
        return new Sable(dir, opts);
    }

    private Sable(Path dir, Options opts) throws IOException {
        this.dir = dir.toAbsolutePath().normalize();
        this.opts = opts;
        if (!OPEN.add(this.dir)) throw new IOException("database already open: " + dir);
        List<SSTable> tables = new ArrayList<>();
        FileChannel lockChannel = null;
        try {
            Files.createDirectories(dir);
            lockChannel = FileChannel.open(dir.resolve("LOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            this.lockChannel = lockChannel;
            if (lockChannel.tryLock() == null) throw new IOException("database open in another process: " + dir);
            recover(tables);
            lock.lock();
            try {
                maybeScheduleCompaction();
            } finally {
                lock.unlock();
            }
        } catch (IOException | RuntimeException e) {
            new State(null, null, tables).release(); // closes the tables opened so far
            flusher.shutdownNow();
            compactor.shutdownNow();
            if (lockChannel != null) lockChannel.close();
            OPEN.remove(this.dir);
            throw e;
        }
    }

    /** Rebuilds state from the manifest, replays surviving WALs into a new SSTable, and removes stray files. */
    private void recover(List<SSTable> tables) throws IOException {
        Manifest m = Manifest.load(dir);
        if (m == null) m = new Manifest(1, 0, List.of());
        Set<Long> live = new HashSet<>(m.tableIds());
        long maxId = 0;
        for (long id : m.tableIds()) {
            tables.add(SSTable.open(dir.resolve(SSTable.fileName(id)), id, metrics));
            maxId = Math.max(maxId, id);
        }
        List<Path> wals = new ArrayList<>();
        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = s.toList();
        }
        for (Path f : files) {
            String name = f.getFileName().toString();
            if (name.endsWith(".tmp")) {
                Files.delete(f); // an interrupted flush, compaction or manifest write
            } else if (name.endsWith(".sst")) {
                maxId = Math.max(maxId, Io.idOf(f));
                if (!live.contains(Io.idOf(f))) Files.delete(f); // orphan: written but never made it into the manifest
            } else if (name.endsWith(".wal")) {
                maxId = Math.max(maxId, Io.idOf(f));
                if (Io.idOf(f) < m.walId()) Files.delete(f); // already flushed
                else wals.add(f);
            }
        }
        nextFileId = Math.max(m.nextFileId(), maxId + 1);

        wals.sort(Comparator.comparingLong(Io::idOf));
        Memtable replayed = new Memtable();
        for (Path w : wals) WriteAheadLog.replay(w, replayed::put);
        if (!replayed.isEmpty()) {
            SSTable t = SSTable.write(dir, nextFileId++, replayed.iterator(null, null), opts, metrics);
            tables.add(0, t);
        }
        wal = WriteAheadLog.create(dir, nextFileId++, metrics);
        saveManifest(tables, wal.id);
        for (Path w : wals) Files.delete(w);
        state = new State(new Memtable(), null, tables);
    }

    public void put(byte[] key, byte[] value) throws IOException {
        Objects.requireNonNull(value);
        write(key, value.clone());
    }

    public void delete(byte[] key) throws IOException {
        write(key, Entry.TOMBSTONE);
    }

    private void write(byte[] key, byte[] value) throws IOException {
        key = Objects.requireNonNull(key).clone(); // callers may reuse their arrays after we return
        WriteAheadLog log;
        long end;
        lock.lock();
        try {
            checkUsable();
            makeRoomForWrite();
            log = wal;
            try {
                end = log.append(key, value);
            } catch (IOException e) {
                fail(e); // a failed append may leave a torn record that would hide every later one on replay
                throw e;
            }
            state.mem.put(key, value);
            metrics.writes.increment();
            if (opts.syncWrites && !opts.groupCommit) sync(log, end);
        } finally {
            lock.unlock();
        }
        if (opts.syncWrites && opts.groupCommit) sync(log, end);
    }

    /** A failed fsync may have dropped the dirty pages, and a retry can then falsely succeed, so stop accepting writes. */
    private void sync(WriteAheadLog log, long end) throws IOException {
        try {
            log.sync(end);
        } catch (IOException e) {
            fail(e);
            throw e;
        }
    }

    private void makeRoomForWrite() throws IOException {
        while (state.mem.approximateBytes() >= opts.memtableBytes) {
            if (state.imm != null) {
                metrics.writeStalls.increment();
                flushed.awaitUninterruptibly();
                checkUsable();
                continue;
            }
            rotate();
        }
    }

    /** Freezes the active memtable and its WAL, starts fresh ones, and queues the flush. Lock held. */
    private void rotate() throws IOException {
        WriteAheadLog old = wal;
        WriteAheadLog next = WriteAheadLog.create(dir, nextFileId++, metrics);
        immWal = old;
        wal = next;
        install(new State(new Memtable(), state.mem, state.tables));
        flusher.execute(() -> flushImm(old));
    }

    private void flushImm(WriteAheadLog old) {
        try {
            old.close(); // makes its writes durable, including those whose writers are still waiting in sync()
            Memtable imm;
            long id;
            lock.lock();
            try {
                imm = state.imm;
                id = nextFileId++;
            } finally {
                lock.unlock();
            }
            SSTable t = SSTable.write(dir, id, imm.iterator(null, null), opts, metrics);
            lock.lock();
            try {
                List<SSTable> tables = new ArrayList<>(state.tables);
                if (t != null) tables.add(0, t);
                saveManifest(tables, wal.id);
                immWal = null;
                install(new State(state.mem, null, tables));
                metrics.flushes.increment();
                maybeScheduleCompaction();
                flushed.signalAll();
            } finally {
                lock.unlock();
            }
            Files.deleteIfExists(old.path);
        } catch (IOException | RuntimeException e) {
            fail(e);
        }
    }

    /** Lock held. While a flush is pending the manifest must keep the frozen memtable's WAL, or a crash loses it. */
    private void saveManifest(List<SSTable> tables, long walId) throws IOException {
        new Manifest(nextFileId, walId, tables.stream().map(t -> t.id).toList()).save(dir);
    }

    private long oldestWalId() {
        return immWal != null ? immWal.id : wal.id;
    }

    /** Lock held. */
    private void install(State next) {
        State prev = state;
        state = next;
        prev.release();
    }

    private void fail(Exception e) {
        lock.lock();
        try {
            if (backgroundError == null) backgroundError = e;
            compactionScheduled = false;
            flushed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void checkUsable() throws IOException {
        if (closed) throw new IllegalStateException("closed");
        if (backgroundError != null) throw new IOException("background flush/compaction failed", backgroundError);
    }

    /** Returns the value, or null if the key is absent or deleted. */
    public byte[] get(byte[] key) throws IOException {
        Objects.requireNonNull(key);
        metrics.gets.increment();
        State s = pin();
        try {
            byte[] v = s.mem.get(key);
            if (v == null && s.imm != null) v = s.imm.get(key);
            // newest first, so the first table that knows the key decides (compaction only merges neighbours)
            for (int i = 0; v == null && i < s.tables.size(); i++) v = s.tables.get(i).get(key);
            return v == null || v == Entry.TOMBSTONE ? null : v.clone();
        } finally {
            s.release();
        }
    }

    private State pin() {
        while (true) {
            State s = state;
            if (s.tryRetain()) return s;
            if (closed) throw new IllegalStateException("closed");
        }
    }

    /**
     * Iterates entries with {@code from <= key < to} in key order; a null bound is unbounded. Not a snapshot:
     * the memtable part reflects writes made while iterating. Close it to let compaction reclaim old files.
     */
    public Scan scan(byte[] from, byte[] to) {
        State s = pin();
        try {
            List<Iterator<Entry>> sources = new ArrayList<>();
            sources.add(s.mem.iterator(from, to));
            if (s.imm != null) sources.add(s.imm.iterator(from, to));
            for (SSTable t : s.tables) sources.add(t.iterator(from, to));
            return new Scan(s, new MergingIterator(sources, true));
        } catch (RuntimeException e) {
            s.release();
            throw e;
        }
    }

    public static final class Scan implements Iterator<Map.Entry<byte[], byte[]>>, AutoCloseable {
        private final State pinned;
        private final Iterator<Entry> it;
        private boolean closed;

        private Scan(State pinned, Iterator<Entry> it) {
            this.pinned = pinned;
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return !closed && it.hasNext();
        }

        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Entry e = it.next();
            return Map.entry(e.key().clone(), e.value().clone());
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            pinned.release();
        }
    }

    /** Flushes the memtable to an SSTable and waits for it. */
    public void flush() throws IOException {
        lock.lock();
        try {
            checkUsable();
            awaitNoImm();
            if (state.mem.isEmpty()) return;
            rotate();
            awaitNoImm();
        } finally {
            lock.unlock();
        }
    }

    private void awaitNoImm() throws IOException {
        while (state.imm != null) {
            flushed.awaitUninterruptibly();
            checkUsable();
        }
    }

    /** Merges every SSTable into one, dropping tombstones and overwritten values. */
    public void compactAll() throws IOException {
        lock.lock();
        try {
            checkUsable();
        } finally {
            lock.unlock();
        }
        Future<Void> done = compactor.submit(() -> {
            List<SSTable> all;
            lock.lock();
            try {
                all = List.copyOf(state.tables);
            } finally {
                lock.unlock();
            }
            if (!all.isEmpty()) compact(all);
            return null;
        });
        try {
            done.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) throw io;
            throw new IOException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted waiting for compaction");
        }
    }

    /** Lock held. */
    private int[] pickRun() {
        if (!opts.autoCompaction || closed || backgroundError != null) return null;
        return SizeTiered.pick(state.tables.stream().mapToLong(t -> t.sizeBytes).toArray(), opts.compactionMinTables);
    }

    /** Lock held. */
    private void maybeScheduleCompaction() {
        if (compactionScheduled || pickRun() == null) return;
        compactionScheduled = true;
        compactor.execute(this::backgroundCompaction);
    }

    private void backgroundCompaction() {
        try {
            List<SSTable> run;
            while ((run = nextRun()) != null) compact(run);
        } catch (IOException | RuntimeException e) {
            fail(e);
        }
    }

    private List<SSTable> nextRun() {
        lock.lock();
        try {
            int[] r = pickRun();
            if (r == null) {
                compactionScheduled = false; // same critical section as the pick, so a flush can't slip between
                return null;
            }
            return List.copyOf(state.tables.subList(r[0], r[1]));
        } finally {
            lock.unlock();
        }
    }

    /** Merges a run of age-adjacent tables into one. Only the compactor thread removes tables. */
    private void compact(List<SSTable> run) throws IOException {
        boolean dropTombstones;
        long id;
        lock.lock();
        try {
            // a tombstone is only safe to drop if no older table outside the run might still hold the key it hides
            dropTombstones = run.get(run.size() - 1) == state.tables.get(state.tables.size() - 1);
            id = nextFileId++;
        } finally {
            lock.unlock();
        }
        List<Iterator<Entry>> sources = new ArrayList<>();
        for (SSTable t : run) sources.add(t.iterator(null, null));
        SSTable out = SSTable.write(dir, id, new MergingIterator(sources, dropTombstones), opts, metrics);
        lock.lock();
        try {
            List<SSTable> tables = new ArrayList<>(state.tables); // flushes may have prepended since we started
            int at = tables.indexOf(run.get(0));
            tables.subList(at, at + run.size()).clear();
            if (out != null) tables.add(at, out);
            saveManifest(tables, oldestWalId());
            run.forEach(SSTable::markObsolete);
            install(new State(state.mem, state.imm, tables));
            metrics.compactions.increment();
        } finally {
            lock.unlock();
        }
    }

    /** Test and benchmark hook: blocks until background compaction has gone idle. */
    void awaitCompactions() throws InterruptedException {
        while (true) {
            lock.lock();
            try {
                if (!compactionScheduled) return;
            } finally {
                lock.unlock();
            }
            Thread.sleep(20);
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            flushed.signalAll();
        } finally {
            lock.unlock();
        }
        try {
            flusher.shutdown(); // queued flushes still run, so everything frozen reaches an SSTable
            flusher.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            compactor.shutdown();
            compactor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted during close");
        } finally {
            lock.lock();
            try {
                try {
                    wal.close();
                } finally {
                    state.release();
                }
            } finally {
                lock.unlock();
                lockChannel.close();
                OPEN.remove(dir);
            }
        }
    }

    public record Stats(long writes, long walSyncs, long gets, long blockReads, long bloomNegatives, long flushes,
                        long compactions, long writeStalls, int sstables, long sstableBytes) {}

    public Stats stats() {
        List<SSTable> tables = state.tables;
        return new Stats(metrics.writes.sum(), metrics.walSyncs.sum(), metrics.gets.sum(), metrics.blockReads.sum(),
                metrics.bloomNegatives.sum(), metrics.flushes.sum(), metrics.compactions.sum(),
                metrics.writeStalls.sum(), tables.size(), tables.stream().mapToLong(t -> t.sizeBytes).sum());
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
