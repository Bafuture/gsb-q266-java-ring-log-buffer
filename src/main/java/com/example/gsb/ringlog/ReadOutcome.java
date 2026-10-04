package com.example.gsb.ringlog;

/**
 * 按序号读取的结果。三态之一，调用方必须显式区分"命中 / 已被覆盖 / 尚未写入"，
 * 被覆盖的过期序号绝不会返回错误数据。
 */
public sealed interface ReadOutcome<T> {

    /** 是否命中一条有效条目。 */
    default boolean isHit() {
        return this instanceof Hit<T>;
    }

    /** 命中：序号有效，携带条目。 */
    record Hit<T>(LogEntry<T> entry) implements ReadOutcome<T> {
    }

    /** 已失效：该序号已被覆盖，数据不可恢复。 */
    record Expired<T>(long requestedSequence, long oldestSequence) implements ReadOutcome<T> {
    }

    /** 未就绪：该序号尚未写入（超过当前最新序号）。 */
    record NotYetWritten<T>(long requestedSequence, long nextSequence) implements ReadOutcome<T> {
    }
}
