package com.zlikun.hadoop.receiver;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.zlikun.hadoop.conf.AppConfigure.DEFAULT_TOPIC;

/**
 * 使用两个group可以实现一对多的情形(典型的发布订阅模式)
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 16:05
 */
@Slf4j
@Component
public class MessageReceiver {

    @KafkaListener(topics = DEFAULT_TOPIC, groupId = "user")
    public void user(ConsumerRecord<?, ?> record) {
        log.info("topic = {}, group = user, key = {}, value = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    @KafkaListener(topics = DEFAULT_TOPIC, groupId = "course")
    public void course(ConsumerRecord<?, ?> record) {
        log.info("topic = {}, group = course, key = {}, value = {}, partition = {}, offset = {}, timestamp = {}",
                record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

}
