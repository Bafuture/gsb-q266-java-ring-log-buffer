package com.example.gsb.ringlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class RingLogBufferTest {

    // ---------- 1) 覆盖策略与最旧序号推进 ----------

    @Test
    void overwriteAdvancesOldestSequence() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(4);
        assertThat(buffer.oldestSequence()).isZero();
        assertThat(buffer.nextSequence()).isZero();

        for (int i = 0; i < 4; i++) {
            buffer.add("msg-" + i);
        }
        assertThat(buffer.oldestSequence()).isZero();
        assertThat(buffer.nextSequence()).isEqualTo(4);
        assertThat(buffer.liveCount()).isEqualTo(4);

        buffer.add("msg-4");
        assertThat(buffer.oldestSequence()).isEqualTo(1);
        assertThat(buffer.liveCount()).isEqualTo(4);

        buffer.addAll(List.of("msg-5", "msg-6", "msg-7", "msg-8"));
        assertThat(buffer.oldestSequence()).isEqualTo(5);
        assertThat(buffer.nextSequence()).isEqualTo(9);
        assertThat(buffer.liveCount()).isEqualTo(4);

        // 有效窗口内的条目内容正确（按序号）
        for (long seq = 5; seq < 9; seq++) {
            assertThat(buffer.read(seq))
                    .isEqualTo(new ReadOutcome.Hit<>(new LogEntry<>(seq, "msg-" + seq)));
        }
    }

    @Test
    void bulkOverwriteLargerThanCapacityKeepsOnlyNewest() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(3);
        buffer.addAll(List.of("a", "b", "c", "d", "e"));
        assertThat(buffer.oldestSequence()).isEqualTo(2);
        assertThat(buffer.liveCount()).isEqualTo(3);
        assertThat(buffer.get(0)).isEqualTo(new LogEntry<>(2, "c"));
        assertThat(buffer.get(2)).isEqualTo(new LogEntry<>(4, "e"));
    }

    @Test
    void indexAccessFollowsOverwriteWindow() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(2);
        buffer.addAll(List.of("x", "y", "z"));
        assertThat(buffer.get(0)).isEqualTo(new LogEntry<>(1, "y"));
        assertThat(buffer.get(1)).isEqualTo(new LogEntry<>(2, "z"));
        assertThatThrownBy(() -> buffer.get(2)).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> buffer.get(-1)).isInstanceOf(NoSuchElementException.class);
    }

    // ---------- 2) 失效读取返回明确结果 ----------

    @Test
    void expiredSequenceReturnsExpiredOutcome() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(2);
        buffer.addAll(List.of("m0", "m1", "m2", "m3"));

        ReadOutcome<String> expired = buffer.read(0);
        assertThat(expired).isEqualTo(new ReadOutcome.Expired<>(0, 2));
        assertThat(expired.isHit()).isFalse();

        assertThat(buffer.read(1)).isEqualTo(new ReadOutcome.Expired<>(1, 2));
        assertThat(buffer.read(2)).isEqualTo(new ReadOutcome.Hit<>(new LogEntry<>(2, "m2")));
        assertThat(buffer.read(3)).isEqualTo(new ReadOutcome.Hit<>(new LogEntry<>(3, "m3")));
        assertThat(buffer.read(4)).isEqualTo(new ReadOutcome.NotYetWritten<>(4, 4));
        assertThat(buffer.read(99)).isEqualTo(new ReadOutcome.NotYetWritten<>(99, 4));
    }

    @Test
    void emptyBufferReadsAreNotYetWritten() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(2);
        assertThat(buffer.read(0)).isEqualTo(new ReadOutcome.NotYetWritten<>(0, 0));
        assertThat(buffer.liveCount()).isZero();
    }

    // ---------- 3) 并发不撕裂 ----------

    /**
     * 载荷是一个必须保持自洽的复合对象：所有字段由同一个种子派生。
     * 若读线程看到"半个条目"（跨槽位拼接或写中途状态），不变量必然被破坏。
     */
    record FatPayload(long seed, long a, long b, long c, String tag) {
        static FatPayload of(long seed) {
            return new FatPayload(seed, seed * 3 + 1, seed * 5 + 2, seed * 7 + 3, "tag-" + seed);
        }

        boolean isConsistent() {
            return a == seed * 3 + 1
                    && b == seed * 5 + 2
                    && c == seed * 7 + 3
                    && tag.equals("tag-" + seed);
        }
    }

    @Test
    void concurrentReadersNeverSeeTornEntries() throws InterruptedException {
        int capacity = 16;
        RingLogBuffer<FatPayload> buffer = new RingLogBuffer<>(capacity);
        int writers = 3;
        int readers = 4;
        int writesPerWriter = 100_000;
        ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> allSequences = new ConcurrentLinkedQueue<>();

        for (int w = 0; w < writers; w++) {
            pool.submit(() -> {
                await(start);
                for (int i = 0; i < writesPerWriter; i++) {
                    LogEntry<FatPayload> entry = buffer.add(FatPayload.of(i));
                    allSequences.add(entry.sequence());
                }
            });
        }
        for (int r = 0; r < readers; r++) {
            pool.submit(() -> {
                await(start);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (System.nanoTime() < deadline
                        && buffer.nextSequence() < (long) writers * writesPerWriter) {
                    int live = buffer.liveCount();
                    if (live == 0) {
                        continue;
                    }
                    long seq = buffer.oldestSequence() + (long) (Math.random() * live);
                    ReadOutcome<FatPayload> outcome = buffer.read(seq);
                    if (outcome instanceof ReadOutcome.Hit<FatPayload> hit) {
                        LogEntry<FatPayload> entry = hit.entry();
                        if (entry.sequence() != seq || !entry.payload().isConsistent()) {
                            violations.add("torn entry at seq " + seq + ": " + entry);
                        }
                    } else if (outcome instanceof ReadOutcome.NotYetWritten<FatPayload>) {
                        violations.add("seq " + seq + " inside window reported not-yet-written");
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        assertThat(violations).isEmpty();
        // 写锁内分配序号：全局无重复、无丢失
        Set<Long> unique = new HashSet<>(allSequences);
        assertThat(unique).hasSize(writers * writesPerWriter);
        assertThat(unique).allMatch(seq -> seq >= 0 && seq < (long) writers * writesPerWriter);
        assertThat(buffer.stats().totalWritten()).isEqualTo((long) writers * writesPerWriter);
    }

    @Test
    void concurrentBatchWritersAllocateContiguousSequences() throws InterruptedException {
        RingLogBuffer<Integer> buffer = new RingLogBuffer<>(64);
        int writers = 4;
        int batches = 500;
        int batchSize = 8;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<List<LogEntry<Integer>>> written = new ConcurrentLinkedQueue<>();

        for (int w = 0; w < writers; w++) {
            pool.submit(() -> {
                await(start);
                for (int b = 0; b < batches; b++) {
                    List<Integer> payloads = new ArrayList<>();
                    for (int i = 0; i < batchSize; i++) {
                        payloads.add(i);
                    }
                    written.add(buffer.addAll(payloads));
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        // 每次批量写入分配到的序号必须内部连续
        for (List<LogEntry<Integer>> batch : written) {
            for (int i = 1; i < batch.size(); i++) {
                assertThat(batch.get(i).sequence()).isEqualTo(batch.get(i - 1).sequence() + 1);
            }
        }
        long total = (long) writers * batches * batchSize;
        assertThat(buffer.stats().totalWritten()).isEqualTo(total);
        assertThat(buffer.stats().liveCount()).isEqualTo(64);
        assertThat(buffer.stats().overwrittenCount()).isEqualTo(total - 64);
    }

    // ---------- 4) 快照隔离 ----------

    @Test
    void snapshotIsIsolatedFromLaterWrites() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(4);
        buffer.addAll(List.of("s0", "s1", "s2"));

        LogSnapshot<String> snapshot = buffer.snapshot();
        assertThat(snapshot.oldestSequence()).isZero();
        assertThat(snapshot.nextSequence()).isEqualTo(3);
        assertThat(snapshot.entries()).containsExactly(
                new LogEntry<>(0, "s0"), new LogEntry<>(1, "s1"), new LogEntry<>(2, "s2"));

        // 快照后继续写入并触发覆盖
        buffer.addAll(List.of("s3", "s4", "s5", "s6", "s7"));
        assertThat(buffer.oldestSequence()).isEqualTo(4);

        // 快照内容不受任何影响
        assertThat(snapshot.entries()).containsExactly(
                new LogEntry<>(0, "s0"), new LogEntry<>(1, "s1"), new LogEntry<>(2, "s2"));
        assertThat(snapshot.size()).isEqualTo(3);
        assertThatThrownBy(() -> snapshot.entries().add(new LogEntry<>(9, "x")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void snapshotDuringConcurrentWritesStaysConsistent() throws InterruptedException {
        RingLogBuffer<FatPayload> buffer = new RingLogBuffer<>(32);
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();

        pool.submit(() -> {
            await(start);
            for (int i = 0; i < 50_000; i++) {
                buffer.add(FatPayload.of(i));
            }
        });
        pool.submit(() -> {
            await(start);
            for (int i = 0; i < 2_000; i++) {
                LogSnapshot<FatPayload> snap = buffer.snapshot();
                // 快照内部必须连续且自洽：序号严格递增、载荷一致
                long expected = snap.oldestSequence();
                if (snap.entries().size() != snap.nextSequence() - snap.oldestSequence()) {
                    violations.add("snapshot size mismatch: " + snap);
                    continue;
                }
                for (LogEntry<FatPayload> entry : snap.entries()) {
                    if (entry.sequence() != expected || !entry.payload().isConsistent()) {
                        violations.add("inconsistent snapshot at seq " + expected + ": " + entry);
                        break;
                    }
                    expected++;
                }
            }
        });
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        assertThat(violations).isEmpty();
        assertThat(buffer.stats().exportCount()).isEqualTo(2_000);
    }

    // ---------- 5) 批量操作 ----------

    @Test
    void bulkWriteReturnsAssignedEntries() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(8);
        List<LogEntry<String>> written = buffer.addAll(List.of("a", "b", "c"));
        assertThat(written).containsExactly(
                new LogEntry<>(0, "a"), new LogEntry<>(1, "b"), new LogEntry<>(2, "c"));
        assertThat(buffer.addAll(List.of())).isEmpty();
    }

    @Test
    void bulkReadByIndexAndBySequence() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(4);
        buffer.addAll(List.of("m0", "m1", "m2", "m3", "m4", "m5"));

        assertThat(buffer.range(0, 4)).containsExactly(
                new LogEntry<>(2, "m2"), new LogEntry<>(3, "m3"),
                new LogEntry<>(4, "m4"), new LogEntry<>(5, "m5"));
        assertThat(buffer.range(1, 3)).containsExactly(
                new LogEntry<>(3, "m3"), new LogEntry<>(4, "m4"));
        assertThat(buffer.range(0, 0)).isEmpty();
        assertThatThrownBy(() -> buffer.range(0, 5)).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> buffer.range(-1, 2)).isInstanceOf(IllegalArgumentException.class);

        List<ReadOutcome<String>> outcomes =
                buffer.readAll(LongStream.of(1, 2, 5, 6).boxed().toList());
        assertThat(outcomes).containsExactly(
                new ReadOutcome.Expired<>(1, 2),
                new ReadOutcome.Hit<>(new LogEntry<>(2, "m2")),
                new ReadOutcome.Hit<>(new LogEntry<>(5, "m5")),
                new ReadOutcome.NotYetWritten<>(6, 6));
    }

    // ---------- 6) 统计 ----------

    @Test
    void statsTrackWritesLiveOverwrittenAndExports() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(4);
        assertThat(buffer.stats()).isEqualTo(new BufferStats(0, 0, 0, 0));

        buffer.addAll(List.of("a", "b", "c"));
        assertThat(buffer.stats()).isEqualTo(new BufferStats(3, 3, 0, 0));

        buffer.addAll(List.of("d", "e", "f", "g", "h"));
        assertThat(buffer.stats()).isEqualTo(new BufferStats(8, 4, 4, 0));

        buffer.snapshot();
        buffer.snapshot();
        assertThat(buffer.stats()).isEqualTo(new BufferStats(8, 4, 4, 2));
        assertThat(buffer.stats().overwrittenCount())
                .isEqualTo(buffer.stats().totalWritten() - buffer.stats().liveCount());
    }

    // ---------- 参数校验 ----------

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> new RingLogBuffer<>(0)).isInstanceOf(IllegalArgumentException.class);
        RingLogBuffer<String> buffer = new RingLogBuffer<>(2);
        assertThatThrownBy(() -> buffer.add(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> buffer.read(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
