package com.zlikun.hadoop;

import com.zlikun.hadoop.partitioner.UserPartitioner;
import com.zlikun.hadoop.serialization.User;
import com.zlikun.hadoop.serialization.UserSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.zlikun.hadoop.TestBase.SERVERS;

/**
 * 分区测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 19:22
 */
@Slf4j
public class PartitionerTest {

    private final String TOPIC = "kafka-example-serialize";

    private Producer<Long, User> producer;
    private UserSerializer serializer ;

    @BeforeEach
    public void init() {
        serializer = new UserSerializer();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // partitioner.class参数：设定分区类
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, UserPartitioner.class.getName());

        producer = new KafkaProducer<>(props, new LongSerializer(), serializer);

    }

    @AfterEach
    public void destroy() {
        producer.close();
    }

    @Test
    public void partition () throws ExecutionException, InterruptedException {

        // 遍历分区信息
        producer.partitionsFor(TOPIC)
                .stream()
                .forEach(info -> {
                    // leader = v163.zlikun.com:9092 (id: 3 rack: null), topic = kafka-example-serialize, partition = 2, replicas = [v163.zlikun.com:9092 (id: 3 rack: null)]
                    // leader = v162.zlikun.com:9092 (id: 2 rack: null), topic = kafka-example-serialize, partition = 1, replicas = [v162.zlikun.com:9092 (id: 2 rack: null)]
                    // leader = v161.zlikun.com:9092 (id: 1 rack: null), topic = kafka-example-serialize, partition = 0, replicas = [v161.zlikun.com:9092 (id: 1 rack: null)]
                    log.info("leader = {}, topic = {}, partition = {}, replicas = {}",
                            info.leader(), info.topic(), info.partition(), info.replicas());
                });

        // 使用指定分区器发送消息
        User user = new User(10086L, "zlikun", LocalDate.of(2000, 1, 1));
        // 如果不指定键，且使用了默认分区器，那么记录将被随机地发送到主题内各个可用分区上，使用轮询（Round Robin）算法将消息均衡地分布到各个分区上
        // 如果键不为空，但使用了默认分区器，那么kafka会对键进行散列，根据散列值把消息映射到特定的分区上，同一个键总会被映射到同个分区
        // 通常不建议动态调整分区（一开始就应该规划好），否则会造成新的消息（使用相同的键）被写到不同分区上
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>(TOPIC, user.getId(), user));

        RecordMetadata metadata = future.get();
        // topic = kafka-example-serialize, timestamp = 1534583972139, partition = 2, offset = 3
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

}
