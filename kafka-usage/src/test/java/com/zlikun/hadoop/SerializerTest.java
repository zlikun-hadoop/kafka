package com.zlikun.hadoop;

import com.zlikun.hadoop.serialization.UserSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.zlikun.hadoop.TestBase.SERVERS;
import static com.zlikun.hadoop.TestBase.TOPIC;
import static org.junit.Assert.assertEquals;

/**
 * 自定义序列化测试，Kafka 提供了基本的Java类型序列化(反序列化)器，其它类型则可以通过实现 Serializer 接口来实现
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 18:27
 */
@Slf4j
public class SerializerTest {

    Producer<Long, User> producer;
    UserSerializer serializer ;

    @Before
    public void init() {
        serializer = new UserSerializer();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producer = new KafkaProducer<>(props, new LongSerializer(), serializer);

    }

    @After
    public void destroy() {
        producer.close();
    }

    @Test
    public void serialize() {

        byte [] bytes = serializer.serialize(TOPIC,
                new User(10086L, "zlikun", LocalDate.of(2000, 1, 1)));
        assertEquals(18, bytes.length);

    }

    @Test
    public void produce() throws ExecutionException, InterruptedException {

        User user = new User(10086L, "zlikun", LocalDate.of(2000, 1, 1));
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<Long, User>(TOPIC, user.getId(), user));

        RecordMetadata metadata = future.get();
        // topic = logs, timestamp = 1522408664870, partition = 0, offset = 21
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

}
