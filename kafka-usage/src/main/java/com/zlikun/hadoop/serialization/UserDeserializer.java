package com.zlikun.hadoop.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.time.LocalDate;
import java.util.Map;

/**
 * 自定义反序列化实现
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 19:06
 */
@Slf4j
public class UserDeserializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.info("--configure--");
    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if (data != null && data.length != 0) {
            try (
                    ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ) {
                User user = new User();
                String topic2 = readExternal(new ObjectInputStream(bais), user);
                // 验证是否同一个Topic，否则返回空
                if (!topic.equals(topic2)) {
                    return null;
                } else {
                    return user;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void close() {
        log.info("--close--");
    }

    private String readExternal(ObjectInput in, User data) throws IOException, ClassNotFoundException {
        String topic = in.readUTF();
        data.setId(in.readLong());
        data.setName(in.readUTF());
        data.setBirthday((LocalDate) in.readObject());
        return topic;
    }
}
