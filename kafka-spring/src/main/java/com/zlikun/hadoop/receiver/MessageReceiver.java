package com.zlikun.hadoop.receiver;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.zlikun.hadoop.conf.AppConfigure.DEFAULT_TOPIC;

/**
 * 使用两个group可以实现一对多的情形(典型的发布订阅模式)
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 16:05
 */
@Slf4j
@Component
// 可以用在Bean上，表示针对该Bean的全局控制
@KafkaListener(id = "multi", topics = DEFAULT_TOPIC)
public class MessageReceiver {

    @KafkaListener(topics = DEFAULT_TOPIC, groupId = "user")
    public void user(ConsumerRecord<?, ?> record) {
        log.info("topic = {}, group = user, key = {}, value = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = DEFAULT_TOPIC, partitions = { "0" })
    }, groupId = "course")
    public void course(ConsumerRecord<?, ?> record) {
        log.info("topic = {}, group = course, key = {}, value = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    @KafkaListener(id = "qux", topicPattern = DEFAULT_TOPIC, groupId = "qux")
    public void listen(@Payload String value,
                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        log.info("topic = {}, group = qux, key = {}, value = {}, partition = {}, timestamp = {}",
                topic, key, value, partition, ts);
    }

    /**
     * 批量消费
     * @param list
     * @param ack，如果设置为非自动应答，可以使用该参数来应答
     */
    @KafkaListener(topics = DEFAULT_TOPIC, groupId = "batch", containerFactory = "batchKafkaListenerContainerFactory")
    public void batch(List<String> list, Acknowledgment ack) {
        list.stream().map(line -> "-->" + line).forEach(log::info);
//        ack.acknowledge();
    }

}
