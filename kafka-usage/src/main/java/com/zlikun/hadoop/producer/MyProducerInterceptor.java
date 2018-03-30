package com.zlikun.hadoop.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义生产者拦截器测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 19:44
 */
@Slf4j
public class MyProducerInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("producer interceptor - configure");
    }

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        // producer interceptor - onSend : { key : 10086 ,value : User(id=10086, name=zlikun, birthday=2000-01-01) ,topic : logs }
        log.info("producer interceptor - onSend : { key : {} ,value : {} ,topic : {} }",
                record.key(), record.value(), record.topic());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // producer interceptor - onAcknowledgement
        log.info("producer interceptor - onAcknowledgement");
    }

    @Override
    public void close() {
        // producer interceptor - close
        log.info("producer interceptor - close");
    }

}
