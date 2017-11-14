package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * http://kafka.apache.org/10/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-13 19:38
 */
@Slf4j
public class KafkaProducerTest {

    Producer<String, String> producer = null;
    String servers = "192.168.120.74:9092" ;

    @Before
    public void init() {
        Properties props = new Properties();
        // Kafka 服务端的主机名及端口号
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
//        // 指定序列化器，也可以在KafkaProducer构造方法中指定
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        producer = new KafkaProducer<>(props);
        producer = new KafkaProducer<>(props , new StringSerializer() ,new StringSerializer());
    }

    /**
     * 异步发送
     */
    @Test
    public void async() {

        for (int i = 0; i < 10; i++) {
            // 发送消息方法
            // 第一个参数(必填)：ProducerRecord类型的对象，封装了Topic、Key、Value信息
            // 第二个参数(可选)：Callback对象，当生产者接收到Kafka发来的ACK确认消息时，会调用其#onCompletion()方法
            // 参考：org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
            final String key = "key-" + i ;
            final String value = "value-" + i ;
            producer.send(
                    new ProducerRecord<String, String>(
                            "my-replicated-topic", key, value)
                    , new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            log.info("callback info : { partition : {} ,offset : {} ,key : {} ,value : {} }"
                                    ,recordMetadata.partition() ,recordMetadata.offset() ,key ,value);
                        }
                    });

        }

    }

    /**
     * 同步发送
     */
    @Test
    public void sync() {

        for (int i = 0; i < 10; i++) {
            final String key = "key-" + i ;
            final String value = "value-" + i ;
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<String, String>(
                            "my-replicated-topic", key, value));
            try {
                RecordMetadata metadata = future.get() ;
                log.info("future.get() info : { partition : {} ,offset : {} ,key : {} ,value : {} }"
                        ,metadata.partition() ,metadata.offset() ,key ,value);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

    }

    @After
    public void destroy() {
        producer.close();
    }

}
