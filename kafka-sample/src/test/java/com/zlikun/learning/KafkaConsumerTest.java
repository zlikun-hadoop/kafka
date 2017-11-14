package com.zlikun.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 * http://kafka.apache.org/10/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-13 20:01
 */
public class KafkaConsumerTest {

    KafkaConsumer<String, String> consumer ;
    String servers = "192.168.120.74:9092" ;
    String topic = "my-replicated-topic" ;

    @Before
    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        // consumer group id
        props.put("group.id", "test");
        // 自动提交 offset
        props.put("enable.auto.commit", "true");
        // 自动提交 offset 的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // 与 producer 刚好相反，这里使用 Deserializer
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    @Test
    public void test() {

        // 订阅 Topic
        consumer.subscribe(Arrays.asList(topic, "test"));
        while (true) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }

    @After
    public void destroy() {
        consumer.close();
    }

}
