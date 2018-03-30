package com.zlikun.hadoop;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Properties;

/**
 * 提供者、消费者客户端配置
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:32
 */
public abstract class TestBase {

    public static String SERVERS = "kafka.zlikun.com:9092";
    public static String TOPIC = "logs";

    static Producer<String, String> producer;
    static KafkaConsumer<String, String> consumer;

    @BeforeClass
    public static final void init() {

        initConsumer();
        initProducer();

    }

    /**
     * 初始化生产者
     * http://kafka.apache.org/documentation.html#brokerconfigs
     */
    private static final void initProducer() {
        Properties props = new Properties();
        // Kafka 服务端的主机名及端口号
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 指定序列化器，也可以在KafkaProducer构造方法中指定
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        producer = new KafkaProducer<>(props);
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    /**
     * 初始化消费者
     * http://kafka.apache.org/documentation.html#consumerconfigs
     */
    private static final void initConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
    }

    @AfterClass
    public static final void destroy() {
        producer.close();
        consumer.close();
    }

}
