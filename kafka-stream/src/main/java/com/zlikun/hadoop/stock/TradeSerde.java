package com.zlikun.hadoop.stock;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/19 16:20
 * @see org.apache.kafka.common.serialization.Serdes.WrapperSerde
 */
public class TradeSerde implements Serde<Trade> {

    private Serializer<Trade> serializer;
    private Deserializer<Trade> deserializer;

    public TradeSerde() {
        serializer = new Serializer<Trade>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public byte[] serialize(String topic, Trade data) {
                // 为了方便在控制台输入，这里直接使用字符串拼接实现
                return String.format("%s,%f,%f", data.getCode(), data.getOpeningPrice(), data.getCurrentPrice()).getBytes();
            }

            @Override
            public void close() {

            }
        };
        deserializer = new Deserializer<Trade>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public Trade deserialize(String topic, byte[] data) {
                String text = new String(data);
                String[] parts = text.split(",");
                Trade trade = new Trade();
                trade.setCode(parts[0]);
                trade.setOpeningPrice(Double.valueOf(parts[1]));
                trade.setCurrentPrice(Double.valueOf(parts[2]));
                return trade;
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Trade> serializer() {
        return this.serializer;
    }

    @Override
    public Deserializer<Trade> deserializer() {
        return this.deserializer;
    }
}
