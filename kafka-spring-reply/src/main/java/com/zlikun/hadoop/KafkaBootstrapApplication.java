package com.zlikun.hadoop;

import com.zlikun.hadoop.conf.KReplyingConfigure;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.ExecutionException;

import static com.zlikun.hadoop.conf.KReplyingConfigure.REQUEST_TOPIC;

/**
 * 测试ReplyingKafkaTemplate
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-08-21 16:49
 */
public class KafkaBootstrapApplication {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan("com.zlikun.hadoop");
        context.register(KReplyingConfigure.class);
        context.refresh();

        try {

            // 测试发送消息
            ReplyingKafkaTemplate<String, String, String> template = context.getBean(ReplyingKafkaTemplate.class);
            ProducerRecord record = new ProducerRecord(REQUEST_TOPIC, "ReplyingKafkaTemplate !");
            record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, KReplyingConfigure.REPLY_TOPIC.getBytes()));
            RequestReplyFuture<String, String, String> replyFuture = template.sendAndReceive(record);
            SendResult<String, String> sendResult = replyFuture.getSendFuture().get();
            System.out.println("Sent ok: " + sendResult.getRecordMetadata());
            ConsumerRecord<String, String> consumerRecord = replyFuture.get();
            System.out.println("Return value: " + consumerRecord.value());

            // 主线程休眠5秒，观察程序执行效果(监听器部分)
            Thread.currentThread().join(5_000L);

        } finally {
            context.stop();
            context.close();
        }

    }

}
