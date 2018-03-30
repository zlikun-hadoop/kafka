package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;

import java.util.Arrays;

/**
 * 消息消费者API测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:56
 */
@Slf4j
public class KafkaConsumerTest extends TestBase {

    @Test
    public void consume() {

        // 订阅Topic
        consumer.subscribe(Arrays.asList(topic));

        // 这里仅供测试，持续10秒
        for (int i = 0; i < 50; i++) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (ConsumerRecord<String, String> record : records) {
                // topic = logs, timestamp = 1522404498736, partition = 0, offset = 14, key = null, value = hello
                // topic = logs, timestamp = 1522404503372, partition = 0, offset = 15, key = null, value = kafka
                // topic = logs, timestamp = 1522404560593, partition = 0, offset = 17, key = 00001, value = U_00001
                log.info("{} => topic = {}, timestamp = {}, partition = {}, offset = {}, key = {}, value = {}",
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
