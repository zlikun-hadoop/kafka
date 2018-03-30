package com.zlikun.hadoop.serialization;

import com.zlikun.hadoop.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * 自定义序列化实现
 * @see org.apache.kafka.common.serialization.StringSerializer
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 18:35
 */
@Slf4j
public class UserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.info("--configure--");
    }

    @Override
    public byte[] serialize(String topic, User data) {
        if (data == null || data.getId() == null) {
            return new byte[0];
        }

        // 这里采取最简单的方法，将字段用英文逗号拼起来，组成一个字符串，再转换为字节数组，没有值的时候，使用"_"代替
        StringBuilder builder = new StringBuilder();
        builder.append(data.getId()).append(",")
                .append(data.getName() != null ? data.getName() : "_").append(",")
                .append(data.getBirthday() != null ? data.getBirthday().toEpochDay() : "_");

        try {
            return builder.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    @Override
    public void close() {
        log.info("--close--");
    }

}
