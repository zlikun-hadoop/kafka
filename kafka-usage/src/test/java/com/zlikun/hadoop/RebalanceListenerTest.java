package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        // 订阅 Topic ，并设置Rebalance监听器
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {

            /**
             * 重新分配分区之后和消费者开始读取消息前被调用
             * @param partitions
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("consumer rebalance - onPartitionsAssigned");
                // 假如把offset写入数据库或者缓存，可以在再均衡时重新设置消费者的offset以实现不漏消息
//                for (TopicPartition partition : partitions) {
//                    consumer.seek(partition, getOffsetFromDb(partition));
//                }
            }

            /**
             * 在再均衡开始前和消费者停止读取消息后被调用
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("consumer rebalance - onPartitionsRevoked");
                consumer.commitSync(offsets);
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
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                // 与 onPartitionsAssigned() 中代码配合使用
                // setOffsetFromDb(record.topic(), record.partition(), record.offset());
            }
            consumer.commitAsync(offsets, null);
        }

        // 该方法是consumer惟一可以被其它线程安全调用的方法
        // 作用是使用poll()抛出一个异常，从而终止消费者
        // 写在这里仅为了说明，实际并不应在此处使用
        // consumer.wakeup();

    }

}
