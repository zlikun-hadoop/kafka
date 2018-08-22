package com.zlikun.hadoop.template;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-03 13:24
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AppConfigure.class)
public class TransactionTest {

    @Autowired
    private KafkaTemplate<String, String> template;

    @Test
    public void transaction() {

        // 将多次发送逻辑放在一个事务中
        boolean result = template.executeInTransaction(operations -> {
            operations.sendDefault("name", "zlikun");
            operations.sendDefault("age", "120");
            return true;
        });
        assertTrue(result);

    }

}
