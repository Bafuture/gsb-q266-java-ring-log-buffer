package com.example.gsb.ringlog;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 固定容量、覆盖式的线程安全环形日志缓冲。
 *
 * <p>语义约定：
 * <ul>
 *   <li>全局序号从 0 开始单调递增，由写入路径在同一把写锁内分配；</li>
 *   <li>缓冲写满后，新写入会覆盖最旧槽位，{@code oldestSequence} 随之推进，
 *       被覆盖的序号在按序号读取时返回 {@link ReadOutcome.Expired}，绝不返回错误数据；</li>
 *   {@code entries} 存放不可变的 {@link LogEntry}，对槽位与序号元数据的读写都在
 *   {@link ReadWriteLock} 保护下进行，读线程看到的要么是完整旧条目、要么是完整新条目，
 *   不可能读到正在被覆盖的半个条目；</li>
 *   <li>快照在持锁期间把条目复制到独立列表，导出后继续写入不影响已生成的快照。</li>
 * </ul>
 *
 * <p>与无界队列的取舍见仓库 README：本实现用固定内存上限换取"只保留最近历史"，
 * 无界队列保留全部历史但内存随写入线性增长，存在 OOM 风险。
 *
 * @param <T> 日志载荷类型
 */
public final class RingLogBuffer<T> {

    private final int capacity;
    private final Object[] slots;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** 下一个待分配序号；已写入总数 = nextSequence（从 0 起）。 */
    private long nextSequence;
    /** 当前最旧有效序号；缓冲为空时等于 nextSequence。 */
    private long oldestSequence;
    private long exportCount;

    public RingLogBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.slots = new Object[capacity];
    }

    public int capacity() {
        return capacity;
    }

    private int slotOf(long sequence) {
        return (int) (sequence % capacity);
    }

    /**
     * 批量写入：一次加锁分配一批连续序号并依次放入槽位，减少多次单写的同步开销。
     *
     * @param payloads 载荷列表，元素不允许为 null
     * @return 本次写入分配到的条目（含序号）
     */
    public List<LogEntry<T>> addAll(List<T> payloads) {
        Objects.requireNonNull(payloads, "payloads must not be null");
        if (payloads.isEmpty()) {
            return List.of();
        }
        List<LogEntry<T>> written = new ArrayList<>(payloads.size());
        lock.writeLock().lock();
        try {
            for (T payload : payloads) {
                Objects.requireNonNull(payload, "payload must not be null");
                long seq = nextSequence++;
                LogEntry<T> entry = new LogEntry<>(seq, payload);
                slots[slotOf(seq)] = entry;
                if (nextSequence - oldestSequence > capacity) {
                    oldestSequence = nextSequence - capacity;
                }
                written.add(entry);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return List.copyOf(written);
    }

    /** 写入单条，等价于单元素的 {@link #addAll(List)}。 */
    public LogEntry<T> add(T payload) {
        return addAll(List.of(payload)).get(0);
    }

    /**
     * 按序号读取：显式区分命中、已失效（被覆盖）与未就绪（尚未写入）。
     */
    public ReadOutcome<T> read(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must not be negative: " + sequence);
        }
        lock.readLock().lock();
        try {
            if (sequence < oldestSequence) {
                return new ReadOutcome.Expired<>(sequence, oldestSequence);
            }
            if (sequence >= nextSequence) {
                return new ReadOutcome.NotYetWritten<>(sequence, nextSequence);
            }
            @SuppressWarnings("unchecked")
            LogEntry<T> entry = (LogEntry<T>) slots[slotOf(sequence)];
            return new ReadOutcome.Hit<>(entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 按下标读取当前有效区间内的条目，下标 0 对应最旧条目。
     *
     * @throws NoSuchElementException 下标越界（超出当前有效条数）
     */
    public LogEntry<T> get(int index) {
        lock.readLock().lock();
        try {
            int live = liveCountLocked();
            if (index < 0 || index >= live) {
                throw new NoSuchElementException(
                        "index " + index + " out of bounds for live count " + live);
            }
            long seq = oldestSequence + index;
            @SuppressWarnings("unchecked")
            LogEntry<T> entry = (LogEntry<T>) slots[slotOf(seq)];
            return entry;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 批量按下标读取当前有效区间内的一段连续条目，一次加锁完成。
     */
    public List<LogEntry<T>> range(int fromIndex, int toIndexExclusive) {
        if (fromIndex < 0 || toIndexExclusive < fromIndex) {
            throw new IllegalArgumentException(
                    "invalid range: [" + fromIndex + ", " + toIndexExclusive + ")");
        }
        lock.readLock().lock();
        try {
            int live = liveCountLocked();
            if (toIndexExclusive > live) {
                throw new NoSuchElementException(
                        "range end " + toIndexExclusive + " exceeds live count " + live);
            }
            List<LogEntry<T>> result = new ArrayList<>(toIndexExclusive - fromIndex);
            for (long seq = oldestSequence + fromIndex; seq < oldestSequence + toIndexExclusive; seq++) {
                @SuppressWarnings("unchecked")
                LogEntry<T> entry = (LogEntry<T>) slots[slotOf(seq)];
                result.add(entry);
            }
            return List.copyOf(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 批量按序号读取，一次加锁返回每个序号的三态结果，结果顺序与入参一致。
     */
    public List<ReadOutcome<T>> readAll(List<Long> sequences) {
        Objects.requireNonNull(sequences, "sequences must not be null");
        lock.readLock().lock();
        try {
            List<ReadOutcome<T>> result = new ArrayList<>(sequences.size());
            for (Long boxed : sequences) {
                long sequence = Objects.requireNonNull(boxed, "sequence must not be null");
                if (sequence < 0) {
                    throw new IllegalArgumentException("sequence must not be negative: " + sequence);
                }
                if (sequence < oldestSequence) {
                    result.add(new ReadOutcome.Expired<>(sequence, oldestSequence));
                } else if (sequence >= nextSequence) {
                    result.add(new ReadOutcome.NotYetWritten<>(sequence, nextSequence));
                } else {
                    @SuppressWarnings("unchecked")
                    LogEntry<T> entry = (LogEntry<T>) slots[slotOf(sequence)];
                    result.add(new ReadOutcome.Hit<>(entry));
                }
            }
            return List.copyOf(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 导出某一时刻的连续快照：持锁复制全部当前有效条目。快照与后续写入完全隔离。
     */
    public LogSnapshot<T> snapshot() {
        lock.readLock().lock();
        try {
            List<LogEntry<T>> copy = new ArrayList<>(liveCountLocked());
            for (long seq = oldestSequence; seq < nextSequence; seq++) {
                @SuppressWarnings("unchecked")
                LogEntry<T> entry = (LogEntry<T>) slots[slotOf(seq)];
                copy.add(entry);
            }
            LogSnapshot<T> snapshot = new LogSnapshot<>(oldestSequence, nextSequence, copy);
            exportCount++;
            return snapshot;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 当前最旧有效序号；缓冲为空时等于 {@link #nextSequence()}。 */
    public long oldestSequence() {
        lock.readLock().lock();
        try {
            return oldestSequence;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 下一个待分配序号（也等于累计写入总数）。 */
    public long nextSequence() {
        lock.readLock().lock();
        try {
            return nextSequence;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 当前有效条数。 */
    public int liveCount() {
        lock.readLock().lock();
        try {
            return liveCountLocked();
        } finally {
            lock.readLock().unlock();
        }
    }

    private int liveCountLocked() {
        return (int) (nextSequence - oldestSequence);
    }

    /** 统计快照：写入总数、有效条数、被覆盖条数、导出次数。 */
    public BufferStats stats() {
        lock.readLock().lock();
        try {
            long written = nextSequence;
            int live = liveCountLocked();
            return new BufferStats(written, live, written - live, exportCount);
        } finally {
            lock.readLock().unlock();
        }
    }
}
