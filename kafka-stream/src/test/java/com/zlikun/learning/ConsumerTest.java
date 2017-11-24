package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 * 数据的生产和消费(代替控制台)
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-24 14:08
 */
@Slf4j
public class ConsumerTest {

    KafkaConsumer<String, String> consumer;
    String servers = "kafka.zlikun.com:9092";

    String outputTopic = "streams-wordcount-output" ;

    @Before
    public void init() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Arrays.asList(outputTopic));
    }

    @Test
    public void consume() throws InterruptedException {
        while (true) {
            log.info("--------------------------------------------------");
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                log.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
            }
        }
    }

    @After
    public void destroy() {
        consumer.close();
    }

}
