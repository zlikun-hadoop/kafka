package com.zlikun.hadoop;

import com.zlikun.hadoop.conf.AppConfigure;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 管理Kafka，这里主要针对Topic维护
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 17:06
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AppConfigure.class)
public class KafkaAdminTest {

    @Autowired
    private AdminClient client;

    /**
     * 使用AdminClient手动维护Topic
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void topic() throws ExecutionException, InterruptedException {
        // 批量创建Topic，NewTopic对象需要指定名称、分区数、副本数（复制因子）
        CreateTopicsResult createTopicsResult = client.createTopics(Arrays.asList(
                new NewTopic("a", 1, (short) 2),
                new NewTopic("b", 3, (short) 1),
                new NewTopic("c", 2, (short) 3)
        ));
        // 因为是异步的（Future），所以这里执行get()方法阻塞直到Topic创建完成，实际无有效返回值
        assertNull(createTopicsResult.all().get());

        // 遍历Topic，这里简单打印Topic名称
        ListTopicsResult listTopicsResult = client.listTopics();
        /*
        a
        b
        kafka-stream-application-counts_store-repartition
        c
        topic-user-logs
        kafka-stock-application-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog
        kafka-stock-application-KSTREAM-REDUCE-STATE-STORE-0000000003-repartition
        kafka-example-logs
        kafka-example-serialize
        topic-stock-results
        topic-wc-counts
        topic-wc-streams
        kafka-stream-application-counts_store-changelog
        topic-stock-streams
         */
        listTopicsResult.names().get().forEach(System.out::println);

        // Topic详细信息
        DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList("a", "b", "c", "m", "n"));
        // topic = a, internal = false, partitions = 1
        // topic = b, internal = false, partitions = 3
        // topic = c, internal = false, partitions = 2
        // topic = m, internal = false, partitions = 1
        // topic = n, internal = false, partitions = 1
        describeTopicsResult.all().get().forEach((name, topic) -> {
            log.info("topic = {}, internal = {}, partitions = {}",
                    topic.name(), topic.isInternal(), topic.partitions().size());
        });

        // 删除之前创建的Topic
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList("a", "b", "c"));
        // 与创建性质相同
        assertNull(deleteTopicsResult.all().get());
    }

}
