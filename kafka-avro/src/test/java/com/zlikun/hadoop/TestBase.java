package com.zlikun.hadoop;

import com.zlikun.hadoop.serialization.Topic;

/**
 * 提供者、消费者客户端配置
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 17:32
 */
public abstract class TestBase {

    static String SERVERS = "192.168.0.161:9092,192.168.0.162:9092,192.168.0.163:9092";
    static String TOPIC = Topic.USER.topicName;

}
