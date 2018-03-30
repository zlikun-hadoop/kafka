package com.zlikun.hadoop;

import com.zlikun.hadoop.producer.MyProducerInterceptor;
import com.zlikun.hadoop.serialization.UserSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Arrays;
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
public class InterceptorTest {

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

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG , Arrays.asList(MyProducerInterceptor.class)) ;

        producer = new KafkaProducer<>(props, new LongSerializer(), serializer);

    }

    @After
    public void destroy() {
        producer.close();
    }

    @Test
    public void produce () throws ExecutionException, InterruptedException {

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
