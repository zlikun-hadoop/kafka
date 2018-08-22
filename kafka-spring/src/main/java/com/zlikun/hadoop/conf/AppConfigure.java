package com.zlikun.hadoop.conf;

import com.zlikun.hadoop.util.ExternalConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;

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

    public static final String SERVERS = ExternalConfigUtil.getString("bootstrap.servers");
    public static final String DEFAULT_TOPIC = "kafka-spring-logs";
    public static final String DEFAULT_GROUP = "kafka-spring";

    /**
     * 配置Admin管理Kafka
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

    /**
     * 配置Admin客户端，可以用来手动维护Topic
     *
     * @return
     */
    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin admin) {
        return AdminClient.create(admin.getConfig());
    }

    /**
     * 也可以通过这种方式自动生成Topic（如果没有就生成）
     *
     * @return
     */
    @Bean
    public NewTopic topicM() {
        return new NewTopic("m", 1, (short) 1);
    }

    @Bean
    public NewTopic topicN() {
        return new NewTopic("n", 1, (short) 2);
    }


    /**
     * 配置Kafka发送模板Bean，由于配置时需要指定序列化和反序列化器，所以实际对不同类型消息，该Bean并不能复用，需要配置多个
     *
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(config));
        // 设置默认Topic，可以在调用发送API时指定实际Topic
        template.setDefaultTopic(DEFAULT_TOPIC);
        // 非必要，仅供测试使用，打印消息发送日志
        template.setProducerListener(new ProducerListener() {
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
     * 配置消费容器实例
     *
     * @return
     */
    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        factory.setConcurrency(3);
//        factory.setBatchListener(true);

        ContainerProperties props = factory.getContainerProperties();
        props.setPollTimeout(1500);
        props.setGroupId(DEFAULT_GROUP);
//        props.setAckMode(AbstractMessageListenerContainer.AckMode.BATCH);
//        props.setAckCount(16);

        return factory;
    }

    /**
     * 批量消费
     *
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        factory.setConcurrency(3);
        factory.setBatchListener(true);

        ContainerProperties props = factory.getContainerProperties();
        props.setPollTimeout(1500);
        props.setGroupId(DEFAULT_GROUP);

        return factory;
    }

}
