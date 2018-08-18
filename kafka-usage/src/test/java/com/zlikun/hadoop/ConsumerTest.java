package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * 消息消费者API测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:56
 */
@Slf4j
public class ConsumerTest extends TestBase {

    private final String TOPIC = "kafka-example-logs";

    /**
     * 只消息一组消息即停止，配合ProducerTest类来测试
     */
    @Test
    public void consume() {

        // 订阅Topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // 这里仅供测试，持续10秒
        ConsumerRecords<String, String> records = null;
        while (records == null || records.isEmpty()) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            records = consumer.poll(3000);
            for (ConsumerRecord<String, String> record : records) {
                // topic = kafka-example-logs, timestamp = 1534582516995, partition = 2, offset = 2, key = K002, value = V002
                // topic = kafka-example-logs, timestamp = 1534582516983, partition = 0, offset = 7, key = null, value = V001
                log.info("topic = {}, timestamp = {}, partition = {}, offset = {}, key = {}, value = {}",
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
