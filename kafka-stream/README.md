# kafka-stream

- <https://kafka.apache.org/10/documentation/streams/>
- <https://kafka.apache.org/10/documentation/streams/quickstart>
- <https://kafka.apache.org/10/documentation/streams/tutorial>
- <https://kafka.apache.org/10/documentation/streams/developer-guide>
- <https://kafka.apache.org/10/documentation/streams/core-concepts>
- <https://kafka.apache.org/10/documentation/streams/architecture>
- <https://kafka.apache.org/10/documentation/streams/upgrade-guide>

#### 创建Topic
```
# input topic
$ sudo bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-plaintext-input
Created topic "streams-plaintext-input".

# output topic
$ sudo bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-wordcount-output
Created topic "streams-wordcount-output".

# The created topic can be described with the same kafka-topics tool
$ sudo bin/kafka-topics.sh --zookeeper localhost:2181 --describe
Topic:streams-plaintext-input	PartitionCount:1	ReplicationFactor:1	Configs:
	Topic: streams-plaintext-input	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
Topic:streams-wordcount-output	PartitionCount:1	ReplicationFactor:1	Configs:
	Topic: streams-wordcount-output	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
```

#### 运行应用
```
# 上传 zlikun-kafka-stream.jar 到kafka/libs/目录下
$ sudo bin/kafka-run-class.sh com.zlikun.hadoop.WordCount

# 测试测试，在IDE上直接运行程序也是可以的
```

#### 启动Producer
```
# 此处理输入语句，消费端将持续输出统计单词数
$ sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic-wc-streams
```

#### 启动Consumer
```
# 注意统计数字会一直累加（实际是每隔一段时间获取一次）
$ sudo bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic topic-wc-counts \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

#### 测试应用
```
# 生产者命令行下输入：
hello kafka
hello apache

# 命令行会得到输出：
hello 1
kafka 1
hello 2
apache 1
```