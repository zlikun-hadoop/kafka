package com.zlikun.hadoop;

import com.zlikun.hadoop.serialization.UserDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.zlikun.hadoop.TestBase.SERVERS;
import static com.zlikun.hadoop.TestBase.TOPIC;
import static org.junit.Assert.assertEquals;

/**
 * 自定义反序列化测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 19:04
 */
@Slf4j
public class DeserializerTest {

    UserDeserializer deserializer;
    private KafkaConsumer<Long, User> consumer;

    @Before
    public void init() {
        deserializer = new UserDeserializer();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumer = new KafkaConsumer<>(props, new LongDeserializer(), deserializer);

    }

    @After
    public void destroy() {
        consumer.close();
    }

    @Test
    public void serialize() throws UnsupportedEncodingException {

        User user = deserializer.deserialize(TOPIC, "10086,zlikun,10957".getBytes("UTF-8"));
        assertEquals(Long.valueOf(10086L), user.getId());
        assertEquals("zlikun", user.getName());
        assertEquals(LocalDate.of(2000, 1, 1), user.getBirthday());

    }

    @Test
    public void consume() throws ExecutionException, InterruptedException {

        // 订阅Topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // 这里仅供测试，持续10秒
        for (int i = 0; i < 50; i++) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            ConsumerRecords<Long, User> records = consumer.poll(200);
            for (ConsumerRecord<Long, User> record : records) {
                // topic = logs, timestamp = 1522407793650, partition = 0, offset = 19, key = 10086, value = User(id=10086, name=zlikun, birthday=2000-01-01)
                // topic = logs, timestamp = 1522407825884, partition = 0, offset = 20, key = 10086, value = User(id=10086, name=zlikun, birthday=null)
                log.info("{} => topic = {}, timestamp = {}, partition = {}, offset = {}, key = {}, value = {}",
                        i,
                        record.topic(),
                        record.timestamp(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value());
            }
        }

    }

}
