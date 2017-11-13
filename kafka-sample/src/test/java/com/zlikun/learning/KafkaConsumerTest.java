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

    @Before
    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    @Test
    public void test() {

        consumer.subscribe(Arrays.asList("my-replicated-topic"));
        while (true) {
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
