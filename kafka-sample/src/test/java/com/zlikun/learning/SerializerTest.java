package com.zlikun.learning;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Serializer or Deserializer (与前者思路一致，不作演示)
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-14 15:37
 */
public class SerializerTest {

    @Test
    public void serialize() {

        // Kafka 提供了基本的Java类型序列化(反序列化)器，其它类型则可以通过实现 Serializer 接口来实现
        // 下面实现实际是：org.apache.kafka.common.serialization.StringSerializer 的代码，仅作参考
        Serializer serializer = new Serializer<String>() {
            private String encoding = "UTF8";
            @Override
            public void configure(Map configs, boolean isKey) {
                String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
                Object encodingValue = configs.get(propertyName);
                if (encodingValue == null)
                    encodingValue = configs.get("serializer.encoding");
                if (encodingValue != null && encodingValue instanceof String)
                    encoding = (String) encodingValue;
            }

            @Override
            public byte[] serialize(String topic, String data) {
                try {
                    if (data == null)
                        return null;
                    else
                        return data.getBytes(encoding);
                } catch (UnsupportedEncodingException e) {
                    throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
                }
            }

            @Override
            public void close() {

            }
        } ;

        byte [] data = serializer.serialize("sample" ,"Hello Kafka !") ;
        assertNotNull(data) ;
        assertEquals(13 ,data.length);

    }

}
