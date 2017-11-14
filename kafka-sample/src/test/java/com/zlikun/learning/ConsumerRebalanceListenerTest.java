package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-14 17:20
 */
@Slf4j
public class ConsumerRebalanceListenerTest {

    KafkaConsumer<String, String> consumer ;
    String servers = "192.168.120.74:9092" ;
    String topic = "my-replicated-topic" ;

    @Before
    public void init() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    @Test
    public void consume() {

        // 订阅 Topic ，并设置Rebalance监听器
        consumer.subscribe(Arrays.asList(topic) ,new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("consumer rebalance - onPartitionsAssigned");
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("consumer rebalance - onPartitionsRevoked");
            }
        });

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
