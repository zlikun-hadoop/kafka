package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * 消息消费者API测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:56
 */
@Slf4j
public class ConsumerTest extends TestBase {

    private final String TOPIC = "kafka-example-logs";

    /**
     * 只消息一组消息即停止，配合ProducerTest类来测试（使用自动提交方式提交偏移量）
     */
    @Test
    public void auto_commit() {

        // 订阅Topic，可以一次订阅多个主题
        consumer.subscribe(Arrays.asList(TOPIC));
        // 还可以通过一个正则来匹配多个主题
        // consumer.subscribe(Pattern.compile("kafka-example-*"));

        // 这里仅供测试，持续10秒
        ConsumerRecords<String, String> records = null;
        // 这里为了测试，设定了终止条件，实际应用中消费者是一个长期运行的程序，可以使用死循环实现
        while (records == null || records.isEmpty()) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            // 该行代码很重要，消费者必须不停地轮循Kafka服务端，否则会被认为已死亡（会触发再均衡），所以即使没有消息也要轮询（设置一个合理的轮询间隔时间）
            records = consumer.poll(100);
            // 遍历拉取的消息列表
            for (ConsumerRecord<String, String> record : records) {
                // topic = kafka-example-logs, timestamp = 1534582516995, partition = 2, offset = 2, key = K002, value = V002
                // topic = kafka-example-logs, timestamp = 1534582516983, partition = 0, offset = 7, key = null, value = V001
                log.info("topic = {}, timestamp = {}, partition = {}, offset = {}, key = {}, value = {}",
                        record.topic(),
                        record.timestamp(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value());
                // 当auto.commit.offset设置为false（不使用自动提交），可以通过下面API手动提交偏移量
                // 下面是同步提交，对吞吐量有影响（但会更精确）
                // consumer.commitSync();
                // 下面是异步提交，但会有延时，可能后一次提交的先于本次到达，而且失败也不会重试（同步的方式会）
                // consumer.commitAsync();
            }
        }

    }

}
