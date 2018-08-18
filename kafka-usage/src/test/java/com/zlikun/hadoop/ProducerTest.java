package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 发送消息，主要有三种方式：
 * 1、发送并忘记（fire-and-forget），我们把消息发送给服务器，但并不关注其是否到达
 * （通常会到达，极少情况未到达生产者默认会重发，但仍有一定概率会丢失一些消息）
 * 2、同步发送，使用send()方法发送消息，它会返回一个Future对象，调用get()方法阻塞直到消息到达后返回
 * 3、异步发送，同样使用send()方法发送消息，并指定一个回调函数，当服务器返回时调用该回调函数
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:29
 */
@Slf4j
public class ProducerTest extends TestBase {

    private final String TOPIC = "kafka-example-logs";

    @Test
    public void fire_and_forget() {
        // key并非必要，但其它序列化器仍必须配置
        producer.send(new ProducerRecord<>(TOPIC, "V001"));
        producer.send(new ProducerRecord<>(TOPIC, "K002", "V002"));
    }

    /**
     * 同步发送消息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    // 表示重复执行测试（不算原本的一次，所以这里会执行4次）
    @RepeatedTest(3)
    public void sync() throws ExecutionException, InterruptedException {

        // 执行同步发送消息（实际该过程也是异步，但通过future.get()获取返回信息是同步的）
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>(TOPIC, "K003", "V003"));

        // 获取执行结果(下面是执行三次后的结果)
        RecordMetadata metadata = future.get();
        // 获取的信息包含topic、时间戳、分区、偏移量等
        // topic = kafka-example-logs, timestamp = 1534578308140, partition = 0, offset = 3
        // topic = kafka-example-logs, timestamp = 1534578308185, partition = 0, offset = 4
        // topic = kafka-example-logs, timestamp = 1534578308190, partition = 0, offset = 5
        log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

    /**
     * 异步发送消息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void async() throws ExecutionException, InterruptedException {

        // 执行异步发送消息，需要传入callback实例
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>(TOPIC, "K004", "V004"),
                (metadata, ex) -> {
                    // [15:47:16.769] ---> topic = kafka-example-logs, timestamp = 1534578436691, partition = 2, offset = 0
                    log.info("---> topic = {}, timestamp = {}, partition = {}, offset = {}",
                            metadata.topic(),
                            metadata.timestamp(),
                            metadata.partition(),
                            metadata.offset(),
                            ex);
                });

        // 使用回调时，不需要在这里等待执行结果，回调函数会自行处理，如果这里等待则为同步
        RecordMetadata metadata = future.get();
        // [15:47:16.769] ===> topic = kafka-example-logs, timestamp = 1534578436691, partition = 2, offset = 0
        log.info("===> topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

}
