package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 09:16
 */
@Slf4j
public class AccumulatorTest extends TestBase {

    private final String TOPIC = "kafka-example-serialize";

    @Test
    public void send() {

        // 异步发送消息
        log.info("send ...");
        Callback callback = (metadata, ex) -> {
            // topic = logs, timestamp = 1522640715952, partition = 0, offset = 0
            log.info("topic = {}, timestamp = {}, partition = {}, offset = {}",
                    metadata.topic(),
                    metadata.timestamp(),
                    metadata.partition(),
                    metadata.offset(),
                    ex);
        };
        // 观察打印日志可以看出，消息发送是异步的，且异步响应的时间几乎一致
        producer.send(new ProducerRecord<String, String>(TOPIC, "name", "zlikun"), callback);
        producer.send(new ProducerRecord<String, String>(TOPIC, "gender", "male"), callback);
        producer.send(new ProducerRecord<String, String>(TOPIC, "birthday", "2018/1/1"), callback);

        /* -------------------------------------------------------------------------------------------------------
         * 异步发送过程：
         * 1、计算剩余等待毫秒数
         * 2、执行键、值序列化
         * 3、执行分区
         * 4、执行：
         * RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
         *      serializedValue, headers, interceptCallback, remainingWaitMs);
         * 5、org.apache.kafka.clients.producer.internals.RecordAccumulator#append()
         * 6、后续具体逻辑比较复杂，暂不研究 。。。
         ------------------------------------------------------------------------------------------------------- */

        log.info("complete ...");

    }

}
