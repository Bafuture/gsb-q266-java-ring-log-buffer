package com.example.gsb.ringlog;

import java.util.Objects;

/**
 * 一条已写入缓冲的日志条目。不可变，发布后可被任意线程安全共享。
 *
 * @param sequence 单调递增的全局序号，从 0 开始，由缓冲在写入时分配
 * @param payload  日志内容，不允许为 null
 */
public record LogEntry<T>(long sequence, T payload) {

    public LogEntry {
        Objects.requireNonNull(payload, "payload must not be null");
    }
}
