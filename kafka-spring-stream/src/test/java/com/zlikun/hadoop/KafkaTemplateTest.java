package com.zlikun.hadoop;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.ExecutionException;

/**
 * 消息发送API测试，配合控制台消费命令确认发送结果
 * $ ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spring-topic-wc-counts --from-beginning
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 15:00
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AppConfigure.class)
public class KafkaTemplateTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送一组数据，观察词频统计结果
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void send() throws ExecutionException, InterruptedException {

        String [] messages = {
          "I have a dream !",
          "Hello, apache .",
          "Hello, kafka ."
        };

        for (int i = 0; i < messages.length; i++) {
            kafkaTemplate.sendDefault(messages[i]);
        }

        Thread.currentThread().join(30_000L);

    }

}
