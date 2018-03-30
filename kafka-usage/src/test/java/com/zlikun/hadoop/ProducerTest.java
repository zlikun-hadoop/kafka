package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 消息生产者API测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:29
 */
@Slf4j
public class ProducerTest extends TestBase {

    /**
     * 同步发送消息
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void sync() throws ExecutionException, InterruptedException {

        // 执行同步发送消息
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<String, String>(TOPIC, "00001", "U_00001"));

        // 获取执行结果(下面是执行三次后的结果)
        RecordMetadata metadata = future.get();
        // topic = logs, timestamp = 1522403228662, partition = 0, offset = 3
        // topic = logs, timestamp = 1522403270441, partition = 0, offset = 4
        // topic = logs, timestamp = 1522403300083, partition = 0, offset = 5
        log.info("0 : topic = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

    /**
     * 异步发送消息
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void async() throws ExecutionException, InterruptedException {

        // 执行异步发送消息，需要传入callback实例
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<String, String>(TOPIC, "00002", "U_00002"),
                (metadata, ex) -> {
                    // TOPIC = logs, timestamp = 1522403633966, partition = 0, offset = 11
                    // TOPIC = logs, timestamp = 1522403722898, partition = 0, offset = 13
                    log.info("1 : TOPIC = {}, timestamp = {}, partition = {}, offset = {}",
                            metadata.topic(),
                            metadata.timestamp(),
                            metadata.partition(),
                            metadata.offset(),
                            ex);
                });

        // 获取执行结果，与回调函数中取得的信息一致
        RecordMetadata metadata = future.get();
        // TOPIC = logs, timestamp = 1522403633966, partition = 0, offset = 11
        // TOPIC = logs, timestamp = 1522403722898, partition = 0, offset = 13
        log.info("2 : TOPIC = {}, timestamp = {}, partition = {}, offset = {}",
                metadata.topic(),
                metadata.timestamp(),
                metadata.partition(),
                metadata.offset());

    }

}
