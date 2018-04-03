package com.zlikun.hadoop.template;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-03 13:11
 */
@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AppConfigure.class)
public class FutureCallbackTest {

    @Autowired
    private KafkaTemplate<String, String> template;

    @Test
    public void test() {

        ListenableFuture<SendResult<String, String>> future = template.send(AppConfigure.DEFAULT_TOPIC, "name", "zlikun");
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.info("failure", throwable);
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                ProducerRecord<String, String> record = result.getProducerRecord();
                // topic = logs, key = name, value = zlikun, timestamp = null, partition = null
                log.info("topic = {}, key = {}, value = {}, timestamp = {}, partition = {}",
                        record.topic(), record.key(), record.value(), record.timestamp(), record.partition());
                RecordMetadata metadata = result.getRecordMetadata();
                // topic = logs, timestamp = 1522732897303, partition = 0, offset = 103
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
