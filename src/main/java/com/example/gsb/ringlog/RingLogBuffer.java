package com.example.gsb.ringlog;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.StampedLock;

/**
 * 固定容量的覆盖式环形日志缓冲。
 *
 * <p>语义：
 * <ul>
 *   <li>每条写入分配一个单调递增的序号（从 0 开始），序号即逻辑时间轴；</li>
 *   <li>写满后新条目覆盖最旧条目，最旧序号 {@code oldestSequence} 随之推进，
 *       有效窗口始终为 {@code [oldestSequence, nextSequence)}；</li>
 *   <li>按序号读取时，已被覆盖的序号返回 {@link ReadResult.Evicted}，
 *       尚未写入的序号返回 {@link ReadResult.NotYetWritten}，绝不返回错误数据；</li>
 *   <li>并发安全：读写通过 {@link StampedLock} 保护，读侧优先走乐观读，
 *       绝不观察到被覆盖到一半的条目；</li>
 *   <li>快照导出在锁内一次性拷贝为不可变列表，导出后任意写入都不影响该快照。</li>
 * </ul>
 *
 * <p>与无界队列的取舍：本结构以"历史长度"换"内存上限"——内存占用恒为
 * O(capacity)，但只能保留最近 capacity 条，更早的历史被永久丢弃。
 */
public final class RingLogBuffer<T> {

    private final int capacity;
    /** 槽位数组，序号 seq 固定落在槽位 (int)(seq % capacity)。 */
    private final LogEntry<T>[] slots;
    private final StampedLock lock = new StampedLock();

    /** 下一个待分配序号，同时等于历史写入总数。 */
    private long nextSequence;
    /** 当前有效条数，0..capacity。 */
    private int size;
    /** 被覆盖淘汰的总条数。 */
    private long evictedCount;
    /** 快照导出次数。 */
    private long exportCount;

    @SuppressWarnings("unchecked")
    public RingLogBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.slots = new LogEntry[capacity];
    }

    public int capacity() {
        return capacity;
    }

    // ---------------------------------------------------------------- 写入

    /** 追加一条日志，返回分配到的序号。写满时覆盖最旧条目。 */
    public long append(T payload) {
        Objects.requireNonNull(payload, "payload");
        long stamp = lock.writeLock();
        try {
            return appendLocked(payload);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 批量追加，整批只获取一次写锁以降低同步开销。
     *
     * @return 本批第一条的序号，后续序号连续递增；空批次返回当前 nextSequence
     */
    public long appendAll(List<? extends T> payloads) {
        Objects.requireNonNull(payloads, "payloads");
        long stamp = lock.writeLock();
        try {
            long first = nextSequence;
            for (T payload : payloads) {
                appendLocked(Objects.requireNonNull(payload, "payload"));
            }
            return first;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private long appendLocked(T payload) {
        long sequence = nextSequence++;
        int slot = slotOf(sequence);
        if (size == capacity) {
            evictedCount++; // 覆盖最旧条目，最旧序号隐式推进（oldest = next - size）
        } else {
            size++;
        }
        slots[slot] = new LogEntry<>(sequence, payload);
        return sequence;
    }

    // ---------------------------------------------------------------- 读取

    /**
     * 按序号读取。优先乐观读：若读取期间无写入发生且槽位序号匹配，直接返回；
     * 否则升级为悲观读锁给出确定结论。任何情况下都不会返回被覆盖到一半的条目。
     */
    public ReadResult<T> read(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must be non-negative: " + sequence);
        }
        long stamp = lock.tryOptimisticRead();
        LogEntry<T> entry = slots[slotOf(sequence)];
        if (lock.validate(stamp) && entry != null && entry.sequence() == sequence) {
            return new ReadResult.Found<>(entry);
        }
        stamp = lock.readLock();
        try {
            return readLocked(sequence);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * 按下标读取：下标 0 为当前最旧条目，下标 size-1 为最新条目。
     *
     * @throws IndexOutOfBoundsException 下标不在 [0, size) 内
     */
    public LogEntry<T> readAt(int index) {
        long stamp = lock.readLock();
        try {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(
                        "index " + index + " out of bounds for size " + size);
            }
            return slots[slotOf(oldestSequenceLocked() + index)];
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * 批量按序号读取 [fromSequence, fromSequence+count)，整批只获取一次读锁。
     * 每个位置独立返回 Found / Evicted / NotYetWritten。
     */
    public List<ReadResult<T>> readBatch(long fromSequence, int count) {
        if (fromSequence < 0) {
            throw new IllegalArgumentException("fromSequence must be non-negative: " + fromSequence);
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative: " + count);
        }
        long stamp = lock.readLock();
        try {
            List<ReadResult<T>> results = new ArrayList<>(count);
            for (long sequence = fromSequence; sequence < fromSequence + count; sequence++) {
                results.add(readLocked(sequence));
            }
            return results;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private ReadResult<T> readLocked(long sequence) {
        if (sequence < oldestSequenceLocked()) {
            return new ReadResult.Evicted<>(sequence, oldestSequenceLocked());
        }
        if (sequence >= nextSequence) {
            return new ReadResult.NotYetWritten<>(sequence, nextSequence);
        }
        LogEntry<T> entry = slots[slotOf(sequence)];
        // 窗口不变式：有效窗口内槽位序号必然匹配
        assert entry != null && entry.sequence() == sequence;
        return new ReadResult.Found<>(entry);
    }

    // ---------------------------------------------------------------- 快照导出

    /** 导出当前全部有效条目（最旧 → 最新）的不可变快照。 */
    public List<LogEntry<T>> export() {
        return exportRange(Long.MIN_VALUE, Integer.MAX_VALUE);
    }

    /**
     * 导出 [fromSequence, fromSequence+maxCount) 与有效窗口交集的不可变快照。
     * 范围被裁剪到当前有效窗口内，因此结果中序号严格连续。
     * 导出在锁内完成拷贝，返回后任何并发写入都不会影响这份快照。
     */
    public List<LogEntry<T>> exportRange(long fromSequence, int maxCount) {
        if (maxCount < 0) {
            throw new IllegalArgumentException("maxCount must be non-negative: " + maxCount);
        }
        long stamp = lock.writeLock(); // 写锁：拷贝期间顺带推进 exportCount，保证统计准确
        try {
            long from = Math.max(fromSequence, oldestSequenceLocked());
            long to = Math.min(from + maxCount, nextSequence);
            List<LogEntry<T>> snapshot = new ArrayList<>((int) (to - from));
            for (long sequence = from; sequence < to; sequence++) {
                snapshot.add(slots[slotOf(sequence)]);
            }
            exportCount++;
            return List.copyOf(snapshot);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    // ---------------------------------------------------------------- 统计

    /** 当前统计快照。 */
    public BufferStats stats() {
        long stamp = lock.readLock();
        try {
            return new BufferStats(
                    nextSequence, size, capacity, evictedCount, exportCount,
                    oldestSequenceLocked(), nextSequence);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    // ---------------------------------------------------------------- 内部

    private int slotOf(long sequence) {
        return (int) (sequence % capacity);
    }

    /** 当前最旧有效序号；覆盖发生时通过 nextSequence 推进而隐式前移。 */
    private long oldestSequenceLocked() {
        return nextSequence - size;
    }
}
