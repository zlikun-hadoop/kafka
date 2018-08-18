package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * 消费容器测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 15:35
 */
@Slf4j
public class KafkaContainerTest extends TestBase {

    private String group = "user";
    private KafkaMessageListenerContainer container;

    @BeforeEach
    public void init() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(config);

        ContainerProperties props = new ContainerProperties(topic);
        props.setMessageListener((MessageListener<String, String>) data -> {
                log.info("key = {}, value = {}, offset = {}, topic = {}, timestamp = {}, headers = {}",
                        data.key(), data.value(), data.offset(), data.topic(), data.timestamp(), data.headers());
        });

        container = new KafkaMessageListenerContainer(factory, props);
        container.setAutoStartup(true);
    }

    @AfterEach
    public void destroy() {
        container.stop(() -> {
            log.info("--stop--");
        });
    }

    @Test
    public void consume() throws InterruptedException {

        container.start();
        // 主线程休眠30秒，观察消费情况
        Thread.currentThread().join(30000L);

    }

}
