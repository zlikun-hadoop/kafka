package com.zlikun.hadoop;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 消息发送API测试，配合控制台消费命令确认发送结果 <br>
 * $ ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-spring-logs --from-beginning <br>
 * $ ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-spring-logs2 --from-beginning <br>
 * <br>
 * <a href="https://docs.spring.io/spring-kafka/reference/htmlsingle/#kafka-template"> https://docs.spring.io/spring-kafka/reference/htmlsingle/#kafka-template </a>
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 15:00
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AppConfigure.class)
public class KafkaTemplateTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private String topic = "kafka-spring-logs2";

    @Test
    public void send() throws ExecutionException, InterruptedException {

        // 测试多种消息发送API
        ListenableFuture<SendResult<String, String>> future;
        future = kafkaTemplate.send(topic, "nginx");
        future = kafkaTemplate.send(new GenericMessage("kafka"));
        future = kafkaTemplate.send(new ProducerRecord<>(topic, "name", "zlikun"));
        // 指定分区ID，不能超过预分区数，本例：[0, 1)
        future = kafkaTemplate.send(topic, 0, "age", "180");
        // sendDefault只是对send方法的简单封装，向defaultTopic发送消息
        future = kafkaTemplate.sendDefault("apache");
        future = kafkaTemplate.sendDefault("language", "java");
        future = kafkaTemplate.sendDefault(0, "gender", "male");
        future = kafkaTemplate.sendDefault(0, 1534918655678L, "title", "architect");

        // 打印最后一次发送返回的信息
        SendResult<String, String> result = future.get();
        RecordMetadata metadata = result.getRecordMetadata();
        // topic = kafka-spring-logs, timestamp = 1534918655678, partition = 0, offset = 4
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

    /**
     * 通过消息头指定Topic
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void header() throws ExecutionException, InterruptedException {

        Map<String, Object> headers = new HashMap<>();
        headers.put(KafkaHeaders.TOPIC, topic);

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(new GenericMessage<>("Hello kafka !", headers));

        SendResult<String, String> result = future.get();
        RecordMetadata metadata = result.getRecordMetadata();
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());
    }

}
