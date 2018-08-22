package com.zlikun.hadoop.template;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * 使用异步回调实现异步发送消息
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-03 13:11
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AppConfigure.class)
public class FutureCallbackTest {

    @Autowired
    private KafkaTemplate<String, String> template;

    @Test
    public void callback() {

        // 执行发送（异步，此时并未真正发送）
        ListenableFuture<SendResult<String, String>> future = template.sendDefault("name", "zlikun");
        // 注册回调函数
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.info("failure", throwable);
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                ProducerRecord<String, String> record = result.getProducerRecord();
                // topic = kafka-spring-logs, key = name, value = zlikun, timestamp = null, partition = null
                log.info("topic = {}, key = {}, value = {}, timestamp = {}, partition = {}",
                        record.topic(), record.key(), record.value(), record.timestamp(), record.partition());
                RecordMetadata metadata = result.getRecordMetadata();
                // topic = kafka-spring-logs, timestamp = 1534920840738, partition = 1, offset = 18
                log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                        metadata.topic(),
                        metadata.timestamp(),
                        metadata.partition(),
                        metadata.offset());
            }
        });

        // 待任务执行完成(异步)，这里仅用于观察程序执行情况
        while (!future.isDone()) ;

    }

}
