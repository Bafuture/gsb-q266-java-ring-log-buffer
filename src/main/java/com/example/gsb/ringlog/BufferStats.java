package com.example.gsb.ringlog;

/**
 * 缓冲运行统计的不可变快照。
 *
 * @param totalWritten    累计写入条目总数（等于下一个待分配序号）
 * @param liveCount       当前有效（未被覆盖）条数，不超过容量
 * @param overwrittenCount 累计被覆盖条数（= totalWritten - liveCount）
 * @param exportCount     累计快照导出次数
 */
public record BufferStats(long totalWritten, int liveCount, long overwrittenCount, long exportCount) {
}
