package com.zlikun.learning.configure;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-14 18:58
 */
@Configuration
@EnableKafka
public class AppConfigure {

    static final String servers = "192.168.120.74:9092" ;
    static final String topic = "my-replicated-topic" ;
    static final String group = "test" ;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer ,String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer ,String> factory =
                new ConcurrentKafkaListenerContainerFactory<>() ;
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.setBatchListener(true);
        return factory ;
    }

    @Bean
    public ConsumerFactory<Integer ,String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<Integer, String>(props) ;
    }

    @Bean
    public Listener listener() {
        return new Listener() ;
    }

    public static class Listener {
        private final CountDownLatch latch = new CountDownLatch(1);
        @KafkaListener(id = group, topics = topic)
        public void listen(String foo) {
            this.latch.countDown();
        }
    }

    @Bean
    public ProducerFactory<Integer ,String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<Integer, String>(props) ;
    }

    @Bean
    public KafkaTemplate<Integer ,String> kafkaTemplate() {
        return new KafkaTemplate<Integer, String>(producerFactory()) ;
    }

}
