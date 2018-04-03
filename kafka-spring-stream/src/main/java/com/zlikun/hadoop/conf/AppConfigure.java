package com.zlikun.hadoop.conf;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-03 17:30
 */
@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class AppConfigure {

    public static final String SERVERS = "kafka.zlikun.com:9092";
    public static final String INPUT_TOPIC = "topic-wc-streams";
    public static final String OUTPUT_TOPIC = "topic-wc-counts";

    /**
     * KafkaStreamsDefaultConfiguration类会自动装配StreamsBuilderFactoryBean实例，但需要注入StreamsConfig实例
     *
     * @return
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "spring-kafka-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new StreamsConfig(props);
    }

    @Bean
    public KStream<?, ?> kStream(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream(INPUT_TOPIC);
        stream
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.as("counts_store"))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        stream.print(Printed.toSysOut());
        return stream;
    }

    /**
     * 配置Kafka发送模板实例
     *
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(config));
        template.setDefaultTopic(INPUT_TOPIC);
        return template;
    }

}
