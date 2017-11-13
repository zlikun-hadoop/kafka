package com.zlikun.learning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * http://kafka.apache.org/10/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-13 19:38
 */
public class KafkaProducerTest {

    Producer<String, String> producer = null;
    String servers = "192.168.120.74:9092" ;

    @Before
    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        producer = new KafkaProducer<>(props);
        producer = new KafkaProducer<>(props , new StringSerializer() ,new StringSerializer());
    }

    @Test
    public void test() {

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("my-replicated-topic", Integer.toString(i), Integer.toString(i)));
        }

    }

    @After
    public void destroy() {
        producer.close();
    }

}
