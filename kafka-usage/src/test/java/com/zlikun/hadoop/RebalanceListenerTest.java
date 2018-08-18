package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;

/**
 * 消费者再平衡监听器测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 19:50
 */
@Slf4j
public class RebalanceListenerTest extends TestBase {

    private final String TOPIC = "kafka-example-serialize";

    @Test
    public void consume() {

        // 订阅 Topic ，并设置Rebalance监听器
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("consumer rebalance - onPartitionsAssigned");
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("consumer rebalance - onPartitionsRevoked");
            }

        });

        // 这里仅供测试，持续10秒
        for (int i = 0; i < 50; i++) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (ConsumerRecord<String, String> record : records) {
                log.info("{} => TOPIC = {}, timestamp = {}, partition = {}, offset = {}, key = {}, value = {}",
                        i,
                        record.topic(),
                        record.timestamp(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value());
            }
        }

    }

}
