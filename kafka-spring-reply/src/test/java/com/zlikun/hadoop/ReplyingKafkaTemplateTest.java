package com.zlikun.hadoop;

import com.zlikun.hadoop.conf.KReplyingConfigure;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.ExecutionException;

import static com.zlikun.hadoop.conf.KReplyingConfigure.REQUEST_TOPIC;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/22 14:58
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {KReplyingConfigure.class})
public class ReplyingKafkaTemplateTest {

    @Autowired
    private ReplyingKafkaTemplate template;

    /**
     * 只是把消息转换为全大写
     *
     * @param in
     * @return
     */
    @KafkaListener(id = "server", topics = REQUEST_TOPIC)
    @SendTo // use default replyTo expression
    public String listen(String in) {
        System.out.println("Server received: " + in);
        return in.toUpperCase();
    }


    @Test
    public void send_and_receive() throws ExecutionException, InterruptedException {
        ProducerRecord record = new ProducerRecord(REQUEST_TOPIC, "ReplyingKafkaTemplate !");
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, KReplyingConfigure.REPLY_TOPIC.getBytes()));
        RequestReplyFuture<String, String, String> replyFuture = template.sendAndReceive(record);
        SendResult<String, String> sendResult = replyFuture.getSendFuture().get();
        System.out.println("Sent ok: " + sendResult.getRecordMetadata());
        ConsumerRecord<String, String> consumerRecord = replyFuture.get();
        System.out.println("Return value: " + consumerRecord.value());

    }

}
