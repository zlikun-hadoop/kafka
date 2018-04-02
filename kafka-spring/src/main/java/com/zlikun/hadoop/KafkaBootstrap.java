package com.zlikun.hadoop;

import com.zlikun.hadoop.conf.AppConfigure;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.annotation.KafkaBootstrapConfiguration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 测试使用Java配置用法
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 16:24
 */
public class KafkaBootstrap {

    public static void main(String[] args) throws InterruptedException {

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan("com.zlikun.hadoop");
        context.register(AppConfigure.class, KafkaBootstrapConfiguration.class);
        context.refresh();

        // 测试发送消息
        KafkaTemplate<String, String> kafkaTemplate = context.getBean("kafkaTemplate", KafkaTemplate.class);
        kafkaTemplate.sendDefault("name", "zlikun");
        kafkaTemplate.sendDefault("age", "120");
        kafkaTemplate.sendDefault("gender", "male");

        // 主线程休眠30秒，观察程序执行效果(监听器部分)
        Thread.currentThread().join(30_000L);

        context.stop();
        context.close();
    }

}
