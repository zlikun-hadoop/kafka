package com.zlikun.hadoop.conf;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.ProducerListenerAdapter;

import java.util.HashMap;
import java.util.Map;

/**
 * https://docs.spring.io/spring-kafka/reference/htmlsingle/#_with_java_configuration
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 15:00
 */
@Slf4j
@Configuration
@EnableKafka
public class AppConfigure {

    public static final String SERVERS = "kafka.zlikun.com:9092";
    public static final String DEFAULT_TOPIC = "logs";
    public static final String DEFAULT_GROUP = "user";

    /**
     * 配置消费容器实例
     *
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<String, String>(config));
        factory.setConcurrency(3);
//        factory.setBatchListener(true);

        ContainerProperties props = factory.getContainerProperties();
        props.setPollTimeout(1500);
        props.setGroupId(DEFAULT_GROUP);
        props.setAckMode(AbstractMessageListenerContainer.AckMode.BATCH);
        props.setAckCount(16);

        return factory;
    }

    /**
     * 配置Kafka发送模板实例
     *
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(config));
        template.setDefaultTopic(DEFAULT_TOPIC);
        // 非必要，仅供测试使用
        template.setProducerListener(new ProducerListenerAdapter() {
            @Override
            public void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata metadata) {
                log.info("topic = {}, key = {}, value = {}, partition = {}, metadata = {}",
                        topic, key, value, partition, metadata);
            }

            @Override
            public void onError(String topic, Integer partition, Object key, Object value, Exception exception) {
                log.error("topic = {}, key = {}, value = {}, partition = {}",
                        topic, key, value, partition, exception);
            }
        });
        return template;
    }

    /**
     * 管理Kafka
     * https://docs.spring.io/spring-kafka/reference/htmlsingle/#_configuring_topics
     *
     * @return
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        return new KafkaAdmin(config);
    }

    @Bean(destroyMethod = "close")
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfig());
    }

}