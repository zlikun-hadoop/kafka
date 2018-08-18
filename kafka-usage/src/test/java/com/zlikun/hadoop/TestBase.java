package com.zlikun.hadoop;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.Properties;

/**
 * 提供者、消费者客户端配置
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:32
 */
public abstract class TestBase {

    public static String SERVERS = AppConstants.SERVERS;
    public static String TOPIC = "logs";

    static Producer<String, String> producer;
    static KafkaConsumer<String, String> consumer;

    @BeforeAll
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
        // Kafka 服务端的主机名及端口号，并不要求提供所有broker信息，生产者会从给定broker中发现其它broker
        // 但建议至少提供两个以上，以防止其中一台宕机而无法发现其它正常的broker，这里给出了三个broker的配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        // client.id参数：任意字符串，用于标识消息来源，非必选项
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "example");
        // acks参数：指定了必须有多少个分区副本收到该消息，生产者才认为消息是写入成功的
        // acks = 0，生产者在成功写入消息 之前不会等待任何来自服务器的响应，性能最高，但消息丢失生产者会无感知
        // acks = 1，只要集群的Leader节点收到消息即认为成功，至少保证消息到达，如果此时Leader节点挂掉，重新选举出Leader，生产者会重发消息
        // acks = all，所有参与复制的节点全部收到消息时，生产才会收到来自服务器的成功响应，这种模式最安全，但会影响kafka吞吐量
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // retries参数：生产者重试次数，当生产者从服务器收到的错误可能是临时性错误，这里生产者会尝试重试发送消息，重试次数即为该参数配置
        // 但如果生产者得到的异常不是临时性的，如：消息过大等，则不会触发重试
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        // 默认每次重试间隔100ms，但可以通过retry.backoff.ms参数来改变这个间隔
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        // batch.size参数：当有多个消息 需要被发送到同一个分区时，生产者把它们放在同一个批次里，该参数指定了一个批次可使用内存大小，单位：字节
        // 这里设置为16KB（16 * 1024 == 16384），该参数与linger.ms参数条件任意一个满足都会发送
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger.ms参数：指定了生产者在发送批次之前等待更多消息加入的批次时间，0表示无延迟，直接发送，大于0则表示间隔linger.ms毫秒发送一次
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // buffer.memory参数：设置生产者内存缓冲区大小，单位：字节，这里为32MB（32 * 1024 * 1024 == 33554432）
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // compression.type参数：设置消息压缩类型，默认不压缩，可选值：snappy、gzip、lz4
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // key.serializer参数：指定序列化器，也可以在KafkaProducer构造方法中指定
        // 其中键的序列化器必须指定，可以在构造方法中指定
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value.serializer参数：通常序列化要求根据实际需要来设置，kafka默认提供了Java基本类的序列化器，也可以自行实现
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 其它参数（暂略）
        // max.in.flight.requests.per.connection：该参数指定了生产者在收到服务器晌应之前可以发送多少个消息。
        // 它的值越高，就会占用越多的内存，不过也会提升吞吐量。 把它设为 1 以保证消息是按照发送的顺序写入服务器的，即使发生了重试。
        // 如果对消息顺序（分区级别）有要求，应打开retries配置，保证消息一定发送成功，配合该参数可以保证消息一定有序（分区级别）
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        // request.timeout.ms
        // props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 0);
        // transaction.timeout.ms
        // props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 0);
        // max.block.ms
        // props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
        // max.request.size
        // props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 0);
        // receive.buffer.bytes
        // props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 0);
        // send.buffer.bytes
        // props.put(ProducerConfig.SEND_BUFFER_CONFIG, 0);

        producer = new KafkaProducer<>(props);
//        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
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

    @AfterAll
    public static final void destroy() {
        producer.close();
        consumer.close();
    }

}
