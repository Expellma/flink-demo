package com.flink.lfxu.partition;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

public class MyPartitioner extends FlinkKafkaPartitioner {
    @Override
    public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return 0;
    }
}
