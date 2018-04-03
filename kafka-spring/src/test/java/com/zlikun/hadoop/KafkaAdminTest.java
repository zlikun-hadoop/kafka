package com.zlikun.hadoop;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 17:06
 */
@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AppConfigure.class)
public class KafkaAdminTest {

    @Autowired
    private AdminClient client;

    @Test
    public void test() throws ExecutionException, InterruptedException {
        // 批量创建Topic
        CreateTopicsResult createTopicsResult = client.createTopics(Arrays.asList(
                new NewTopic("a", 1, (short) 1),
                new NewTopic("b", 1, (short) 1),
                new NewTopic("c", 1, (short) 1)
        ));
        createTopicsResult.all().get(); // Void

        // 查询Topic列表
        ListTopicsResult listTopicsResult = client.listTopics();
        listTopicsResult.names().get().forEach(log::info);

        // 删除之前创建的Topic
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList("a", "b", "c"));
        deleteTopicsResult.all().get(); // Void
    }

}
