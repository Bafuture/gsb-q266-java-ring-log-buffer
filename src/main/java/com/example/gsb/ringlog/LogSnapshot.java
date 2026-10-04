package com.example.gsb.ringlog;

import java.util.List;

/**
 * 某一时刻缓冲内容的一份隔离快照。条目在导出时已复制到独立列表，
 * 快照生成后缓冲被继续写入或覆盖都不会影响本对象。
 *
 * @param oldestSequence 快照时刻的最旧有效序号
 * @param nextSequence   快照时刻的下一个待分配序号
 * @param entries        从 {@code oldestSequence} 起连续的有效条目
 */
public record LogSnapshot<T>(long oldestSequence, long nextSequence, List<LogEntry<T>> entries) {

    public LogSnapshot {
        entries = List.copyOf(entries);
    }

    /** 快照内有效条目数。 */
    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }
}
