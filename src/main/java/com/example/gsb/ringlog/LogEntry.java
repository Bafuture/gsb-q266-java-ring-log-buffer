package com.example.gsb.ringlog;

/**
 * 一条日志条目：单调递增的序号 + 负载。不可变对象，可安全地跨线程发布。
 */
public record LogEntry<T>(long sequence, T payload) {
}
