package com.zlikun.hadoop.serialization;

import com.zlikun.hadoop.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.util.Map;

/**
 * 自定义反序列化实现
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
        if (data == null || data.length == 0) {
            return new User();
        }
        try {
            String message = new String(data, "UTF-8");
            String [] array = message.split(",");
            if (array.length == 3) {
                User user = new User();
                user.setId(Long.valueOf(array[0]));
                if (array[1] != null && !array[1].equals("_")) {
                    user.setName(array[1]);
                }
                if (array[2] != null && !array[2].equals("_")) {
                    user.setBirthday(LocalDate.ofEpochDay(Integer.valueOf(array[2])));
                }
                return user;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new User();
    }

    @Override
    public void close() {
        log.info("--close--");
    }

}
