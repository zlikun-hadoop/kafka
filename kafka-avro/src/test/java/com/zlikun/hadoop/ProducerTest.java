package com.zlikun.hadoop;

import com.zlikun.hadoop.avro.User;
import com.zlikun.hadoop.serialization.AvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 消息生产者API测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:29
 */
@Slf4j
public class ProducerTest extends TestBase {

    private static Producer<Long, User> producer;

    @BeforeClass
    public static void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @AfterClass
    public static void destroy() {
        producer.close();
    }

    @Test
    public void produce() throws ExecutionException, InterruptedException {

        // 准备数据
        User data = User.newBuilder()
                .setId(10000L)
                .setName("zlikun")
                .setBirthday(LocalDate.of(2018, 1, 1).toEpochDay())
                .build();
        // 执行同步发送消息
        producer.send(
                new ProducerRecord<>(TOPIC, data.getId(), data),
                (metadata, exception) -> {
                    log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                            metadata.topic(),
                            metadata.timestamp(),
                            metadata.partition(),
                            metadata.offset());
                });

    }

}
