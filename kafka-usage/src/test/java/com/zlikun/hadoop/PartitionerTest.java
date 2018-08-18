package com.zlikun.hadoop;

import com.zlikun.hadoop.partitioner.UserPartitioner;
import com.zlikun.hadoop.serialization.User;
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

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 19:22
 */
@Slf4j
public class PartitionerTest {

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

        // 设置Partitioner类
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, UserPartitioner.class.getName());

        producer = new KafkaProducer<>(props, new LongSerializer(), serializer);

    }

    @After
    public void destroy() {
        producer.close();
    }

    @Test
    public void partition () throws ExecutionException, InterruptedException {

        producer.partitionsFor(TOPIC)
                .stream()
                .forEach(info -> {
                    // leader = zlikun:9092 (id: 0 rack: null), topic = logs, partition = 0, replicas = [zlikun:9092 (id: 0 rack: null)]
                    log.info("leader = {}, topic = {}, partition = {}, replicas = {}",
                            info.leader(), info.topic(), info.partition(), info.replicas());
                });

        User user = new User(10086L, "zlikun", LocalDate.of(2000, 1, 1));
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<Long, User>(TOPIC, user.getId(), user));

        RecordMetadata metadata = future.get();
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

}
