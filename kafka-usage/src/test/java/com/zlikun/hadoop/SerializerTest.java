package com.zlikun.hadoop;

import com.zlikun.hadoop.serialization.User;
import com.zlikun.hadoop.serialization.UserDeserializer;
import com.zlikun.hadoop.serialization.UserSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.zlikun.hadoop.AppConstants.SERVERS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 自定义序列化测试，Kafka 提供了基本的Java类型序列化(反序列化)器，其它类型则可以通过实现 Serializer 接口来实现
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 18:27
 */
@Slf4j
public class SerializerTest {

    private Producer<Long, User> producer;
    private UserSerializer serializer;
    private UserDeserializer deserializer;
    private KafkaConsumer<Long, User> consumer;
    private final String TOPIC = "kafka-example-serialize";

    @BeforeEach
    public void init() {
        initProducer();
        initConsumer();
    }

    private void initProducer() {
        serializer = new UserSerializer();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producer = new KafkaProducer<>(props, new LongSerializer(), serializer);
    }

    private void initConsumer() {
        deserializer = new UserDeserializer();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumer = new KafkaConsumer<>(props, new LongDeserializer(), deserializer);
    }

    @AfterEach
    public void destroy() {
        producer.close();
    }

    @Test
    public void serialize() {
        // 序列化测试
        byte[] data = serializer.serialize(TOPIC,
                new User(10086L, "zlikun", LocalDate.of(2000, 1, 1)));
        assertEquals(87, data.length);
        // 反序列化测试
        User user = deserializer.deserialize(TOPIC, data);
        assertEquals(Long.valueOf(10086L), user.getId());
        assertEquals("zlikun", user.getName());
        assertEquals(LocalDate.of(2000, 1, 1), user.getBirthday());

    }

    @Test
    public void produce() throws ExecutionException, InterruptedException {

        User user = new User(10086L, "zlikun", LocalDate.of(2000, 1, 1));
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>(TOPIC, user.getId(), user));

        RecordMetadata metadata = future.get();
        // topic = kafka-example-serialize, timestamp = 1534581939008, partition = 2, offset = 2
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

    @Test
    public void consume() throws ExecutionException, InterruptedException {

        // 订阅Topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // 这里仅供测试，持续10秒
        ConsumerRecords<Long, User> records = null;
        while (records == null || records.isEmpty()) {
            // 从 Topic 拉取消息，每次拉取可以是多条
            records = consumer.poll(3000);
            for (ConsumerRecord<Long, User> record : records) {
                // topic = kafka-example-serialize, timestamp = 1534581939008, partition = 2, offset = 2, key = 10086, value = User(id=10086, name=zlikun, birthday=2000-01-01)
                log.info("topic = {}, timestamp = {}, partition = {}, offset = {}, key = {}, value = {}",
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
