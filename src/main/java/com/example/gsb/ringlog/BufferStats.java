package com.example.gsb.ringlog;

/**
 * 缓冲区的统计快照。
 *
 * @param totalWritten   历史写入总数（等于下一个待分配序号）
 * @param size           当前有效条数（不超过容量）
 * @param capacity       固定容量
 * @param evictedCount   被覆盖（淘汰）的总条数
 * @param exportCount    快照导出次数
 * @param oldestSequence 当前最旧有效序号（窗口下界，含）
 * @param nextSequence   下一个待分配序号（窗口上界，不含）
 */
public record BufferStats(
        long totalWritten,
        int size,
        int capacity,
        long evictedCount,
        long exportCount,
        long oldestSequence,
        long nextSequence) {
}
