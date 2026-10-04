package com.example.gsb.ringlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class RingLogBufferTest {

    // ------------------------------------------------------------ 覆盖策略

    @Test
    void overwritesOldestAndAdvancesOldestSequenceWhenFull() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(4);

        for (int i = 0; i < 4; i++) {
            assertThat(buffer.append("log-" + i)).isEqualTo(i);
        }
        assertThat(buffer.stats().oldestSequence()).isZero();
        assertThat(buffer.stats().size()).isEqualTo(4);

        // 写满后继续写：每条覆盖最旧条目，最旧序号逐条推进
        assertThat(buffer.append("log-4")).isEqualTo(4);
        assertThat(buffer.stats().oldestSequence()).isEqualTo(1);
        assertThat(buffer.append("log-5")).isEqualTo(5);
        assertThat(buffer.stats().oldestSequence()).isEqualTo(2);

        BufferStats stats = buffer.stats();
        assertThat(stats.size()).isEqualTo(4);
        assertThat(stats.evictedCount()).isEqualTo(2);
        assertThat(stats.totalWritten()).isEqualTo(6);

        // 有效窗口内按序读取，最旧 → 最新
        for (int i = 0; i < 4; i++) {
            LogEntry<String> entry = buffer.readAt(i);
            assertThat(entry.sequence()).isEqualTo(2 + i);
            assertThat(entry.payload()).isEqualTo("log-" + (2 + i));
        }
    }

    @Test
    void capacityOfOneKeepsOnlyLatestEntry() {
        RingLogBuffer<Integer> buffer = new RingLogBuffer<>(1);
        buffer.append(1);
        buffer.append(2);
        buffer.append(3);

        assertThat(buffer.stats().size()).isEqualTo(1);
        assertThat(buffer.stats().evictedCount()).isEqualTo(2);
        assertThat(buffer.readAt(0).payload()).isEqualTo(3);
        assertThat(buffer.read(1)).isInstanceOf(ReadResult.Evicted.class);
        assertThat(buffer.read(2)).isInstanceOf(ReadResult.Found.class);
    }

    @Test
    void rejectsInvalidCapacity() {
        assertThatThrownBy(() -> new RingLogBuffer<>(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RingLogBuffer<>(-3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------ 失效读取

    @Test
    void evictedSequenceYieldsExplicitEvictedResultNotStaleData() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(3);
        buffer.append("a");
        buffer.append("b");
        buffer.append("c");
        buffer.append("d"); // 覆盖序号 0

        ReadResult<String> evicted = buffer.read(0);
        assertThat(evicted).isEqualTo(new ReadResult.Evicted<>(0, 1));
        assertThat(evicted.isFound()).isFalse();

        // 序号 0 的槽位已被序号 3 复用，绝不能把 "d" 当成序号 0 的数据返回
        ReadResult<String> found = buffer.read(3);
        assertThat(found).isEqualTo(new ReadResult.Found<>(new LogEntry<>(3, "d")));
    }

    @Test
    void notYetWrittenSequenceYieldsExplicitResult() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(4);
        buffer.append("a");

        assertThat(buffer.read(1)).isEqualTo(new ReadResult.NotYetWritten<>(1, 1));
        assertThat(buffer.read(99)).isEqualTo(new ReadResult.NotYetWritten<>(99, 1));
    }

    @Test
    void readAtRejectsOutOfRangeIndex() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(2);
        buffer.append("a");

        assertThatThrownBy(() -> buffer.readAt(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> buffer.readAt(1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> buffer.read(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------ 并发不撕裂

    @Test
    void concurrentReadersNeverObserveTornOrMismatchedEntries() throws Exception {
        int capacity = 64;
        int writers = 4;
        int writesPerWriter = 20_000;
        RingLogBuffer<String> buffer = new RingLogBuffer<>(capacity);
        // 每条序号只被写入一次，记录 序号 -> 应有负载，用于校验读到的条目是否张冠李戴
        Map<Long, String> expectedBySequence = new ConcurrentHashMap<>();
        AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        AtomicBoolean writersDone = new AtomicBoolean();
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(writers + 3);
        List<Future<?>> writerFutures = new ArrayList<>();
        List<Future<?>> readerFutures = new ArrayList<>();

        for (int w = 0; w < writers; w++) {
            int writerId = w;
            writerFutures.add(pool.submit(() -> {
                await(start);
                for (int i = 0; i < writesPerWriter; i++) {
                    String payload = "writer-" + writerId + "-" + i;
                    long sequence = buffer.append(payload);
                    expectedBySequence.put(sequence, payload);
                }
                return null;
            }));
        }

        // 读线程 1/2：随机按序号读，命中的条目负载必须与该序号的写入一致
        for (int r = 0; r < 2; r++) {
            readerFutures.add(pool.submit(() -> {
                await(start);
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while (!writersDone.get() && readerFailure.get() == null) {
                    long next = buffer.stats().nextSequence();
                    if (next == 0) {
                        continue;
                    }
                    long sequence = random.nextLong(next);
                    if (buffer.read(sequence) instanceof ReadResult.Found<String> found) {
                        String expected = expectedBySequence.get(sequence);
                        // expected 为 null 只可能是"写入后尚未登记"的竞态，跳过；
                        // 非 null 却不相等即为撕裂/张冠李戴
                        if (expected != null && !expected.equals(found.entry().payload())) {
                            readerFailure.compareAndSet(null, new AssertionError(
                                    "torn read at sequence " + sequence + ": got '"
                                            + found.entry().payload() + "', expected '" + expected + "'"));
                        }
                        if (found.entry().sequence() != sequence) {
                            readerFailure.compareAndSet(null, new AssertionError(
                                    "entry sequence mismatch at " + sequence));
                        }
                    }
                }
                return null;
            }));
        }

        // 读线程 3：持续按下标扫描，验证窗口内序号严格连续
        readerFutures.add(pool.submit(() -> {
            await(start);
            while (!writersDone.get() && readerFailure.get() == null) {
                int size = buffer.stats().size();
                for (int i = 0; i < size; i++) {
                    LogEntry<String> entry = buffer.readAt(i);
                    String expected = expectedBySequence.get(entry.sequence());
                    if (expected != null && !expected.equals(entry.payload())) {
                        readerFailure.compareAndSet(null, new AssertionError(
                                "torn readAt(" + i + "): sequence " + entry.sequence()));
                    }
                }
            }
            return null;
        }));

        start.countDown();
        for (Future<?> future : writerFutures) {
            future.get(60, TimeUnit.SECONDS);
        }
        writersDone.set(true);
        for (Future<?> future : readerFutures) {
            future.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(readerFailure.get()).isNull();
        BufferStats stats = buffer.stats();
        assertThat(stats.totalWritten()).isEqualTo((long) writers * writesPerWriter);
        assertThat(stats.size()).isEqualTo(capacity);
        assertThat(stats.evictedCount()).isEqualTo((long) writers * writesPerWriter - capacity);
    }

    // ------------------------------------------------------------ 快照隔离

    @Test
    void exportedSnapshotIsImmutableAndUnaffectedByLaterWrites() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(8);
        for (int i = 0; i < 8; i++) {
            buffer.append("v" + i);
        }

        List<LogEntry<String>> snapshot = buffer.export();
        assertThat(snapshot).hasSize(8);
        assertThat(snapshot.get(0)).isEqualTo(new LogEntry<>(0, "v0"));
        assertThat(snapshot.get(7)).isEqualTo(new LogEntry<>(7, "v7"));

        // 快照之后大量覆盖写入
        for (int i = 8; i < 100; i++) {
            buffer.append("v" + i);
        }

        // 快照内容保持导出时刻的原样
        assertThat(snapshot).hasSize(8);
        for (int i = 0; i < 8; i++) {
            assertThat(snapshot.get(i)).isEqualTo(new LogEntry<>(i, "v" + i));
        }
        // 快照不可变
        assertThatThrownBy(() -> snapshot.set(0, new LogEntry<>(99, "x")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void exportRangeClipsToValidWindow() {
        RingLogBuffer<Integer> buffer = new RingLogBuffer<>(4);
        for (int i = 0; i < 6; i++) {
            buffer.append(i); // 有效窗口 [2, 6)
        }

        List<LogEntry<Integer>> range = buffer.exportRange(0, 3);
        assertThat(range).containsExactly(
                new LogEntry<>(2, 2), new LogEntry<>(3, 3), new LogEntry<>(4, 4));

        assertThat(buffer.exportRange(4, 100)).containsExactly(
                new LogEntry<>(4, 4), new LogEntry<>(5, 5));
        assertThat(buffer.exportRange(0, 0)).isEmpty();
    }

    @Test
    void exportsDuringConcurrentWritesAreAlwaysContiguous() throws Exception {
        int capacity = 32;
        RingLogBuffer<Integer> buffer = new RingLogBuffer<>(capacity);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(3);
        Future<?> writer = pool.submit(() -> {
            await(start);
            for (int i = 0; i < 50_000 && failure.get() == null; i++) {
                buffer.append(i);
            }
            return null;
        });
        // 并发批量写入，增加导出与覆盖交错的概率
        Future<?> batchWriter = pool.submit(() -> {
            await(start);
            for (int i = 0; i < 2_000 && failure.get() == null; i++) {
                buffer.appendAll(List.of(-1, -2, -3));
            }
            return null;
        });
        Future<?> exporter = pool.submit(() -> {
            await(start);
            while (!stop.get() && failure.get() == null) {
                List<LogEntry<Integer>> snapshot = buffer.export();
                for (int i = 1; i < snapshot.size(); i++) {
                    if (snapshot.get(i).sequence() != snapshot.get(i - 1).sequence() + 1) {
                        failure.compareAndSet(null, new AssertionError(
                                "non-contiguous snapshot at index " + i));
                        break;
                    }
                }
            }
            return null;
        });

        start.countDown();
        writer.get(60, TimeUnit.SECONDS);
        batchWriter.get(60, TimeUnit.SECONDS);
        stop.set(true);
        exporter.get(60, TimeUnit.SECONDS);
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(failure.get()).isNull();
        assertThat(buffer.stats().exportCount()).isPositive();
    }

    // ------------------------------------------------------------ 批量操作

    @Test
    void appendAllAssignsContiguousSequencesAndEvictsAsNeeded() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(5);

        long first = buffer.appendAll(List.of("a", "b", "c"));
        assertThat(first).isZero();
        assertThat(buffer.stats().size()).isEqualTo(3);

        // 批量写入 5 条，总容量 5：最早的 3 条被覆盖
        long second = buffer.appendAll(List.of("d", "e", "f", "g", "h"));
        assertThat(second).isEqualTo(3);

        BufferStats stats = buffer.stats();
        assertThat(stats.totalWritten()).isEqualTo(8);
        assertThat(stats.size()).isEqualTo(5);
        assertThat(stats.evictedCount()).isEqualTo(3);
        assertThat(stats.oldestSequence()).isEqualTo(3);

        assertThat(buffer.export()).containsExactly(
                new LogEntry<>(3, "d"), new LogEntry<>(4, "e"),
                new LogEntry<>(5, "f"), new LogEntry<>(6, "g"),
                new LogEntry<>(7, "h"));
    }

    @Test
    void readBatchReturnsPerPositionResults() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(3);
        buffer.appendAll(List.of("a", "b", "c", "d", "e")); // 窗口 [2, 5)

        List<ReadResult<String>> results = buffer.readBatch(0, 7);
        assertThat(results).containsExactly(
                new ReadResult.Evicted<>(0, 2),
                new ReadResult.Evicted<>(1, 2),
                new ReadResult.Found<>(new LogEntry<>(2, "c")),
                new ReadResult.Found<>(new LogEntry<>(3, "d")),
                new ReadResult.Found<>(new LogEntry<>(4, "e")),
                new ReadResult.NotYetWritten<>(5, 5),
                new ReadResult.NotYetWritten<>(6, 5));
    }

    @Test
    void emptyBatchOperationsAreHarmless() {
        RingLogBuffer<String> buffer = new RingLogBuffer<>(2);
        assertThat(buffer.appendAll(List.of())).isZero();
        assertThat(buffer.readBatch(0, 0)).isEmpty();
        assertThat(buffer.stats().totalWritten()).isZero();
    }

    // ------------------------------------------------------------ 统计

    @Test
    void statsTrackWritesSizeEvictionsAndExports() {
        RingLogBuffer<Integer> buffer = new RingLogBuffer<>(4);

        BufferStats empty = buffer.stats();
        assertThat(empty.totalWritten()).isZero();
        assertThat(empty.size()).isZero();
        assertThat(empty.evictedCount()).isZero();
        assertThat(empty.exportCount()).isZero();
        assertThat(empty.capacity()).isEqualTo(4);

        IntStream.range(0, 10).forEach(buffer::append);
        buffer.export();
        buffer.exportRange(0, 2);

        BufferStats stats = buffer.stats();
        assertThat(stats.totalWritten()).isEqualTo(10);
        assertThat(stats.size()).isEqualTo(4);
        assertThat(stats.evictedCount()).isEqualTo(6);
        assertThat(stats.exportCount()).isEqualTo(2);
        assertThat(stats.oldestSequence()).isEqualTo(6);
        assertThat(stats.nextSequence()).isEqualTo(10);
        // 不变式：写入总数 = 有效条数 + 被覆盖条数
        assertThat(stats.totalWritten()).isEqualTo(stats.size() + stats.evictedCount());
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
