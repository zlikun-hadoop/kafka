package com.zlikun.hadoop;

import com.zlikun.hadoop.serialization.AvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消息消费者API测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:56
 */
@Slf4j
public class ConsumerTest extends TestBase {

    private static KafkaConsumer<String, String> consumer;

    @BeforeAll
    public static void initConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
    }

    @AfterAll
    public static void destroy() {
        consumer.close();
    }

    @Test
    public void consume() {

        // 订阅Topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // 这里仅供测试，持续10秒
        for (int i = 0; i < 50; i++) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (ConsumerRecord<String, String> record : records) {
                // topic = topic-user-logs, timestamp = 1522827051422, partition = 0, offset = 0, key = 10000, value = {"id": 10000, "name": "zlikun", "birthday": 17532}
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
