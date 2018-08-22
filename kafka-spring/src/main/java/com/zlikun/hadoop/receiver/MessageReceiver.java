package com.zlikun.hadoop.receiver;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.zlikun.hadoop.conf.AppConfigure.DEFAULT_GROUP;
import static com.zlikun.hadoop.conf.AppConfigure.DEFAULT_TOPIC;

/**
 * 使用两个group可以实现一对多的情形(典型的发布订阅模式) <br>
 * <a href="https://docs.spring.io/spring-kafka/reference/htmlsingle/#message-listener-container">https://docs.spring.io/spring-kafka/reference/htmlsingle/#message-listener-container</a>
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 16:05
 */
@Slf4j
@Component
public class MessageReceiver {

    @KafkaListener(topics = DEFAULT_TOPIC, groupId = DEFAULT_GROUP)
    public void user(ConsumerRecord<?, ?> record) {
        // topic = kafka-spring-logs, group = kafka-spring, key = gender, value = male, partition = 2, offset = 11, timestamp = 1534919809440
        // topic = kafka-spring-logs, group = kafka-spring, key = name, value = zlikun, partition = 1, offset = 7, timestamp = 1534919809433
        // topic = kafka-spring-logs, group = kafka-spring, key = age, value = 120, partition = 0, offset = 14, timestamp = 1534919915220
        log.info("topic = {}, group = kafka-spring, key = {}, value = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = DEFAULT_TOPIC, partitions = {"0", "1"})
    }, groupId = "group-x")
    public void course(ConsumerRecord<?, ?> record) {
        // topic = kafka-spring-logs, group = group-x, key = age, value = 120, partition = 0, offset = 8, timestamp = 1534919562631
        // ...
        // topic = kafka-spring-logs, group = group-x, key = age, value = 120, partition = 0, offset = 14, timestamp = 1534919915220
        // topic = kafka-spring-logs, group = group-x, key = name, value = zlikun, partition = 1, offset = 11, timestamp = 1534920045125
        log.info("topic = {}, group = group-x, key = {}, value = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    @KafkaListener(id = "qux", topicPattern = DEFAULT_TOPIC, groupId = "group-y")
    public void listen(@Payload String value,
                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        // topic = kafka-spring-logs, group = group-y, key = name, value = zlikun, partition = 1, timestamp = 1534920264945
        // topic = kafka-spring-logs, group = group-y, key = gender, value = male, partition = 2, timestamp = 1534920264951
        // topic = kafka-spring-logs, group = group-y, key = age, value = 120, partition = 0, timestamp = 1534920264951
        log.info("topic = {}, group = group-y, key = {}, value = {}, partition = {}, timestamp = {}",
                topic, key, value, partition, ts);
    }

    /**
     * 批量消费
     */
    @KafkaListener(topics = DEFAULT_TOPIC, groupId = "batch", containerFactory = "batchKafkaListenerContainerFactory")
    public void batch(List<Message> list) {
        // GenericMessage [payload=zlikun, headers={kafka_offset=16, kafka_nativeHeaders=RecordHeaders(headers = [], isReadOnly = false), kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@2c8fe697, kafka_timestampType=CREATE_TIME, kafka_receivedMessageKey=name, kafka_receivedPartitionId=1, kafka_receivedTopic=kafka-spring-logs, kafka_receivedTimestamp=1534920584931}]
        // GenericMessage [payload=male, headers={kafka_offset=20, kafka_nativeHeaders=RecordHeaders(headers = [], isReadOnly = false), kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@2c8fe697, kafka_timestampType=CREATE_TIME, kafka_receivedMessageKey=gender, kafka_receivedPartitionId=2, kafka_receivedTopic=kafka-spring-logs, kafka_receivedTimestamp=1534920584937}]
        // GenericMessage [payload=120, headers={kafka_offset=21, kafka_nativeHeaders=RecordHeaders(headers = [], isReadOnly = false), kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@2c8fe697, kafka_timestampType=CREATE_TIME, kafka_receivedMessageKey=age, kafka_receivedPartitionId=0, kafka_receivedTopic=kafka-spring-logs, kafka_receivedTimestamp=1534920584937}]
        list.stream().forEach(System.out::println);
    }

//    /**
//     * 批量消费
//     *
//     * @param list
//     * @param ack，如果设置为非自动应答（enable.auto.commit=false），可以使用该参数来应答
//     */
//    @KafkaListener(topics = DEFAULT_TOPIC, groupId = "batch2", containerFactory = "batchKafkaListenerContainerFactory")
//    public void batch2(List<Message> list, Acknowledgment ack) {
//        // GenericMessage [payload=zlikun, headers={kafka_offset=16, kafka_nativeHeaders=RecordHeaders(headers = [], isReadOnly = false), kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@2c8fe697, kafka_timestampType=CREATE_TIME, kafka_receivedMessageKey=name, kafka_receivedPartitionId=1, kafka_receivedTopic=kafka-spring-logs, kafka_receivedTimestamp=1534920584931}]
//        // GenericMessage [payload=male, headers={kafka_offset=20, kafka_nativeHeaders=RecordHeaders(headers = [], isReadOnly = false), kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@2c8fe697, kafka_timestampType=CREATE_TIME, kafka_receivedMessageKey=gender, kafka_receivedPartitionId=2, kafka_receivedTopic=kafka-spring-logs, kafka_receivedTimestamp=1534920584937}]
//        // GenericMessage [payload=120, headers={kafka_offset=21, kafka_nativeHeaders=RecordHeaders(headers = [], isReadOnly = false), kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@2c8fe697, kafka_timestampType=CREATE_TIME, kafka_receivedMessageKey=age, kafka_receivedPartitionId=0, kafka_receivedTopic=kafka-spring-logs, kafka_receivedTimestamp=1534920584937}]
//        list.stream().forEach(System.out::println);
//        ack.acknowledge();
//    }

}
