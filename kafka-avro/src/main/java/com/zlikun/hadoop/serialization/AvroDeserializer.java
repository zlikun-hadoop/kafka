package com.zlikun.hadoop.serialization;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-04 14:28
 */
public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {
        DatumReader<T> datumReader = new SpecificDatumReader<>(Topic.matchFor(topic).topicType.getSchema());
        BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);
        try {
            return datumReader.read(null, binaryDecoder);
        } catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
    }

    @Override
    public void close() {

    }
}
