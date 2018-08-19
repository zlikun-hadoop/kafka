package com.zlikun.hadoop.stock;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 模拟股票实时交易，计算一支股票当前价相对于开盘价的涨跌幅
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/19 16:27
 */
public class StockTrade {

    private static final String SERVERS = "192.168.0.161:9092,192.168.0.162:9092,192.168.0.163:9092";
    private static final String TOPIC_STREAM = "topic-stock-streams";
    private static final String TOPIC_TARGET = "topic-stock-results";

    public static void main(String[] args) throws InterruptedException {

        // 配置Stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stock-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class);


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Trade> source = builder.stream(TOPIC_STREAM);

        KTable<String, Double> table = source
                // 计算涨跌幅
                .mapValues(trade -> (trade.getCurrentPrice() - trade.getOpeningPrice()) / trade.getOpeningPrice())
                // 按code分组（只有一个，有啥好分的？）
                .groupBy((key, value) -> key)
                // 再聚合（取最新的值计算即可）
                .reduce((x, y) -> y);

        table.toStream().to(TOPIC_TARGET, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 启动生产者
        produce();

        TimeUnit.SECONDS.sleep(60L);

        streams.close();

    }

    /**
     * 使用一个独立线程生产消息
     */
    private static final void produce() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        new Thread(() -> {
            KafkaProducer<String, Trade> producer = new KafkaProducer<>(props, new StringSerializer(), new TradeSerde().serializer());
            for (int i = 0; i < 100; i++) {
                Trade trade = new Trade();
                trade.setCode("100001");
                trade.setOpeningPrice(12.5);
                trade.setCurrentPrice(12.5 + ((int) (Math.random()) & 3));
                producer.send(new ProducerRecord<>(TOPIC_STREAM, trade.getCode(), trade));
                System.out.printf("%03d - 发送一条数据%n", i + 1);
                try {
                    TimeUnit.MILLISECONDS.sleep(200L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            producer.close();
        }).start();
    }

}
