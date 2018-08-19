package com.zlikun.hadoop;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 词频统计应用
 * <p>
 * https://kafka.apache.org/documentation/streams/
 * https://kafka.apache.org/11/documentation/streams/quickstart
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 14:09
 */
public class WordCount {

    private static final String SERVERS = "192.168.0.161:9092,192.168.0.162:9092,192.168.0.163:9092";
    private static final String TOPIC_STREAM = "topic-wc-streams";
    private static final String TOPIC_TARGET = "topic-wc-counts";

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        // 每个Stream应用程序必须有一个应用ID，用于协调应用实例，也用于命名内部的要地存储和相差主题，对集群中的Stream而言，必须惟一
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-application");
        // 配置Kafka集群地址
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        // 配置序列化类和反序列化类，kafka提供了默认实现
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 构建拓扑
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(TOPIC_STREAM);
        KTable<String, Long> table = source
                // 拆分出行中全部单词
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
                // 过滤不必要的单词
                .filter((key, value) -> !value.equals("the"))
                // 按键分组
                .groupBy((key, value) -> value)
                // 计算每个单词出现次数
                .count(Materialized.as("counts_store"));
        // 把计算结果写回kafka（不同的Topic）
        table.toStream().to(TOPIC_TARGET, Produced.with(Serdes.String(), Serdes.Long()));

        // 运行上述流程
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 一般情况下，stream应用程序会一直运行，这里仅用于演示，使其等待60秒后结束
        TimeUnit.SECONDS.sleep(60L);

        streams.close();

    }

}
