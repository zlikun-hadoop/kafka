package com.zlikun.hadoop.serialization;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * 实现Avro序列化器，参考：
 * https://unmi.cc/kafka-produce-consume-avro-data/
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-04 14:20
 */
public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        DatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        try {
            datumWriter.write(data, binaryEncoder);
        } catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
        return outputStream.toByteArray();
    }

    @Override
    public void close() {

    }

}
