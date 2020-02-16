package com.pyg.report.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// 实现分区接口
public class RoundRobinPartitioner implements Partitioner {

    // 分布式环境下多线程，Integer 是不安全的
    // Integer counter = new Integer(0);
    // 并发包下的 线程安全的 整型类
    AtomicInteger counter = new AtomicInteger(0);

    // 返回值为分区号 0 1 2
    // 需要让消息轮询的发送到不同的分区里面
    @Override
//    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        // 获取分区数量
        Integer partitions = cluster.partitionCountForTopic(topic);

        // 对消息进行计数然后对分区数取模
        int curpartition = counter.incrementAndGet() % partitions;

        if (counter.get() > 65535) {
            // 修正为0
            counter.set(0);
        }

        return curpartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
