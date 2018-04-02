package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 消息发送API测试，配合控制台消费命令确认发送结果
 * $ ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logs --from-beginning
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 15:00
 */
@Slf4j
public class KafkaTemplateTest extends TestBase {

    KafkaTemplate<String, String> kafkaTemplate;

    @Before
    public void init() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        ProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);

        kafkaTemplate = new KafkaTemplate<>(factory);
    }

    @Test
    public void produce() throws ExecutionException, InterruptedException {

        // 设置默认Topic，否则仅能使用明确指定Topic的API
        kafkaTemplate.setDefaultTopic(topic);

        ListenableFuture<SendResult<String, String>> future = null;
        future = kafkaTemplate.send(topic, "nginx");
        future = kafkaTemplate.send(new GenericMessage("kafka"));
        future = kafkaTemplate.send(new ProducerRecord<String, String>(topic, "name", "zlikun"));
        // 指定分区ID，不能超过预分区数，本例：[0, 1)
        future = kafkaTemplate.send(topic, 0, "age", "180");
        future = kafkaTemplate.sendDefault("apache");
        future = kafkaTemplate.sendDefault("language", "java");
        future = kafkaTemplate.sendDefault(0, "gender", "male");
        future = kafkaTemplate.sendDefault(0, System.currentTimeMillis(), "title", "architect");

        SendResult<String, String> result = future.get();
        RecordMetadata metadata = result.getRecordMetadata();
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

}
