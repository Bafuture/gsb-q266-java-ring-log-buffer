package com.example.gsb.ringlog;

/**
 * 按序号读取的结果。被覆盖的序号返回 {@link Evicted}，
 * 尚未写入的序号返回 {@link NotYetWritten}，命中返回 {@link Found}。
 * 调用方可以据此明确区分"已失效"与"错误数据"。
 */
public sealed interface ReadResult<T> {

    /** 读取命中，返回该序号对应的条目。 */
    record Found<T>(LogEntry<T> entry) implements ReadResult<T> {
    }

    /** 该序号已被覆盖（小于当前最旧序号），数据已失效。 */
    record Evicted<T>(long sequence, long oldestSequence) implements ReadResult<T> {
    }

    /** 该序号尚未写入（大于等于下一个待分配序号）。 */
    record NotYetWritten<T>(long sequence, long nextSequence) implements ReadResult<T> {
    }

    default boolean isFound() {
        return this instanceof Found<T>;
    }
}
