package com.zlikun.hadoop.conf;

import com.zlikun.hadoop.util.ExternalConfigUtil;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessagingMessageConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/22 15:12
 */
@Configuration
public class KReplyingConfigure {

    public static final String SERVERS = ExternalConfigUtil.getString("bootstrap.servers");
    public static final String DEFAULT_GROUP = "kafka-spring-reply";

    public static final String REQUEST_TOPIC = "kafka-spring-request";
    public static final String REPLY_TOPIC = "kafka-spring-response";

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * https://docs.spring.io/spring-kafka/reference/htmlsingle/#replying-template
     *
     * @return
     */
    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate() {
        return new ReplyingKafkaTemplate<>(producerFactory(), replyContainer());
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> containerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // NOTE - set up of reply template 设置响应模板
        factory.setReplyTemplate(replyingKafkaTemplate());
        return factory;
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> replyContainer() {
        ContainerProperties containerProperties = new ContainerProperties(REPLY_TOPIC);
        return new KafkaMessageListenerContainer<>(consumerFactory(), containerProperties);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public NewTopic kRequest() {
        return new NewTopic(REQUEST_TOPIC, 3, (short) 1);
    }

    @Bean
    public NewTopic kResponse() {
        return new NewTopic(REPLY_TOPIC, 3, (short) 1);
    }

    @Bean // not required if Jackson is on the classpath
    public MessagingMessageConverter simpleMapperConverter() {
        MessagingMessageConverter messagingMessageConverter = new MessagingMessageConverter();
        messagingMessageConverter.setHeaderMapper(new SimpleKafkaHeaderMapper());
        return messagingMessageConverter;
    }

}
