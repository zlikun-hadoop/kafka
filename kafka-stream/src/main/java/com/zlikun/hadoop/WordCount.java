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

/**
 * 词频统计应用
 *
 * https://kafka.apache.org/documentation/streams/
 * https://kafka.apache.org/11/documentation/streams/quickstart
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 14:09
 */
public class WordCount {

    private static final String SERVERS = "kafka.zlikun.com:9092";
    private static final String TOPIC_STREAM = "topic-wc-streams";
    private static final String TOPIC_TARGET = "topic-wc-counts";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(TOPIC_STREAM);
        KTable<String, Long> table = lines
                // Split each text line, by whitespace, into words.
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
                // Group the text words as message keys
                .groupBy((key, value) -> value)
                // Count the occurrences of each word (message key).
                .count(Materialized.as("counts_store"));
        // Store the running counts as a changelog stream to the output topic.
        table.toStream().to(TOPIC_TARGET, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }

}
