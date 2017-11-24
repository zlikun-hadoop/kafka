package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * 数据生产者
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-24 14:08
 */
@Slf4j
public class ProducerTest {

    Producer<String, String> producer ;
    String servers = "kafka.zlikun.com:9092";

    String inputTopic = "streams-plaintext-input" ;

    @Before
    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    @Test
    public void produce() throws InterruptedException {

        // 执行消息生产
        // 《Shall I Compare Thee To A Summer's Day?》 by William Shakespeare (1564-1616)
        String[] messages = {
                "Shall I compare thee to a summer's day ?",
                "Thou art more lovely and more temperate .",
                "Rough winds do shake the darling buds of May ,",
                "And summer's lease hath all too short a date .",
                "Sometime too hot the eye of heaven shines ,",
                "And often is his gold complexion dimm'd ;",
                "And every fair from fair sometime declines ,",
                "By chance or nature's changing course untrimm'd ;",
                "But thy eternal summer shall not fade .",
                "Nor lose possession of that fair thou ow'st ;",
                "Nor shall Death brag thou wander'st in his shade ,",
                "When in eternal lines to time thou grow'st :",
                "So long as men can breathe or eyes can see ,",
                "So long lives this, and this gives life to thee .",
        };
        for (int i = 0; i < 14; i++) {
            log.info("send the No.{} message ." ,i + 1);
            producer.send(new ProducerRecord<String, String>(inputTopic, messages[i]));
            Thread.sleep(200);
        }

    }

    @After
    public void destroy() {
        producer.close();
    }

}
