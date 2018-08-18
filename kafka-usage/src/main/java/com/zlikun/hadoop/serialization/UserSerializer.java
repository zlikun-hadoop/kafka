package com.zlikun.hadoop.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * 自定义序列化实现，这里借助Externalizable序列化机制来实现（性能比直接使用Serializable要好一引起）
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 18:35
 * @see org.apache.kafka.common.serialization.StringSerializer
 */
@Slf4j
public class UserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.info("--configure--");
    }

    @Override
    public byte[] serialize(String topic, User data) {
        if (data != null && data.getId() != null) {
            try (
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ) {
                this.writeExternal(new ObjectOutputStream(baos), data, topic);
                return baos.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new byte[0];
    }

    @Override
    public void close() {
        log.info("--close--");
    }

    private void writeExternal(ObjectOutput out, User data, String topic) throws IOException {
        out.writeUTF(topic);
        out.writeLong(data.getId());
        out.writeUTF(data.getName());
        out.writeObject(data.getBirthday());
    }

}
