package com.zlikun.hadoop;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
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
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "example-producer");
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
        // bootstrap.servers参数：与生产者相同，配置kafka服务器（连接）信息
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        // group.id参数：消费者多了分组参数，一个组里的消费者订阅的是同个主题，每个消费者接收主题一部分分区消息（相当于JMS中的点对点机制）
        // 如果不同消费者指定了不同的分组，那么这两个消费者可以重复消费同一条消息（相当于JMS中的发布订阅机制）
        // 需要注意的是群组里的消费者应与主题的分区数一致，如果多于分区数则多出的消费者会闲置，如果少于分区则一个消费者可以读取多个分区（这个并不会造成浪费，视应用需求而定）
        // 群组里的消费者读取共同主题的分区，当一个新的消费者加入群组时，新的消费者读取的是之前其它消费者读取的分区，这时分区的从属权发生了变化，这样的行为被称为再均衡
        // 再均衡提供了动态伸缩能力，但再均衡期间消费者无法读取消息，会造成一小段时间整个群组不可用，另外当分区被重新分配给另一个消费者时，消费者当前的读取状态会丢失，
        // 它需要去刷新缓存，在它重新恢复状态之前会拖慢应用程序
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user");
        // fetch.min.bytes参数：指定消费者从服务器获取记录的最小字节数，如果broker在收到消费者的数据请求时，
        // 如果可用数据量小于该值，它会等到有足够的可用数据时才返回。这样会降低消费者和broker的负载，但会造成消息处理延时（需要权衡）
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        // fetch.max.bytes参数：
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800);
        // fetch.max.wait.ms参数：指定broker等待时间，与fetch.min.bytes任意满足broker即返回消息
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        // max.partition.fetch.bytes参数：指定服务器从每个分区里返回给消费者的最大字节数，默认：1MB
        // 也就是说poll()方法从每个分区返回的记录最多不超过该参数指定的字节数
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024);
        // session.timeout.ms参数：指定了消费者在被认为死亡之前可以与服务器断开连接的时间，默认：3秒
        // 如果消费者在该参数指定时间内没有发送心跳给群组协调器，就会被认为已经死亡，协调器就会触发再均衡
        // 该属性与heartbeat.interval.ms参数关系紧密，后者必须比前者小，一般为三分之一
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        // props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        // auto.offset.reset参数：指定消费者在读取一个没有偏移量的分区或者偏移师无效的情况下该作的处理
        // 默认：latest（消费者将从最新的记录开始读取数据，最新指消费者启动后生成的记录），另一个值为earliest（消费者从起始位置读取分区的记录）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // enable.auto.commit参数：指定消费者是否自动提交偏移量，默认：true
        // 为了避免出现重复数据和数据丢失，可以设为false，由程序控制何时提交偏移量（这时可以检查数据重复或数据丢失）
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // auto.commit.interval.ms参数：当自动提交设置为true时，指定提交的频率
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 60 * 1000);
        // partition.assignment.strategy参数：分区分配给群组里的消费者的分配策略
        // Range：该策略会把主题若干个连续的分区分配给消费者，如果无法整分，第一个消费者将会分得更多的分区
        // RoundRobin：该策略把分区逐一分配给消费者，这种方式相对比较平均，但分区上并不连续（分区连续有啥意义么？）
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        // client.id参数：任意字符串，broker用它来标识从客户端过来的消息
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "example-consumer");
        // max.poll.records参数：控制单次调用call()方法能够返回的记录数量，可以控制在轮询里需要处理的数据量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        // receive.buffer.bytes参数：
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 32768);
        // send.buffer.bytes参数：
        props.put(ConsumerConfig.SEND_BUFFER_CONFIG, 131072);
        // 与生产者相反，配置键、值反序列化器
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
