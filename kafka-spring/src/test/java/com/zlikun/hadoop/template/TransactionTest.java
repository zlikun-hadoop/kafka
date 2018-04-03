package com.zlikun.hadoop.template;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertTrue;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-03 13:24
 */
@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AppConfigure.class)
public class TransactionTest {

    @Autowired
    private KafkaTemplate<String, String> template;

    @Test
    public void test() {

        boolean result = template.executeInTransaction(t -> {
            t.sendDefault("name",  "zlikun");
            t.sendDefault("age",  "120");
            return true;
        });
        assertTrue(result);

    }

}
