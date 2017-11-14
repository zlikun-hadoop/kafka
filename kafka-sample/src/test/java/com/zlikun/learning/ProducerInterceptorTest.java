package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-14 15:14
 */
@Slf4j
public class ProducerInterceptorTest {

    Producer<String, String> producer = null;
    String servers = "192.168.120.74:9092" ;
    String topic = "my-replicated-topic" ;

    @Before
    public void init() {
        Properties props = new Properties();
        // Kafka 服务端的主机名及端口号
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // ProducerInterceptors
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG , Arrays.asList(DefaultProducerInterceptor.class)) ;

        producer = new KafkaProducer<>(props);
    }

    /**
     * 仅供测试
     */
    public static class DefaultProducerInterceptor implements ProducerInterceptor<String ,String> {
        @Override
        public void configure(Map<String, ?> configs) {
            log.info("producer interceptor - configure");
        }

        @Override
        public ProducerRecord onSend(ProducerRecord record) {
            log.info("producer interceptor - onSend : { key : {} ,value : {} ,topic : {} }" ,record.key() ,record.value() ,record.topic());
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            log.info("producer interceptor - onAcknowledgement");
        }

        @Override
        public void close() {
            log.info("producer interceptor - close");
        }
    }

    @Test
    public void sync() {

        final String key = "name" ;
        final String value = "zlikun" ;
        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topic, key, value));
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

    @After
    public void destroy() {
        producer.close();
    }

}
