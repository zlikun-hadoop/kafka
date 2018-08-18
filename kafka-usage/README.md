#### 遍历Topic
```
$ ./bin/kafka-topics.sh --list --zookeeper 192.168.0.161:2181
__consumer_offsets
kafka-example-logs
```

#### 查看Topic
```
$ ./bin/kafka-topics.sh --describe --zookeeper 192.168.0.161:2181 --topic kafka-example-logs
  Topic:kafka-example-logs        PartitionCount:3        ReplicationFactor:1     Configs:
          Topic: kafka-example-logs       Partition: 0    Leader: 1       Replicas: 1     Isr: 1
          Topic: kafka-example-logs       Partition: 1    Leader: 2       Replicas: 2     Isr: 2
          Topic: kafka-example-logs       Partition: 2    Leader: 3       Replicas: 3     Isr: 3
```

#### 测试生产者
```
$ ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic kafka-example-logs
>kafka
```

#### 测试消费者
```
$ ./bin/kafka-console-consumer.sh --zookeeper 192.168.0.161:2181 --topic kafka-example-logs
kafka
```

#### Q & A
- Java客户端向Kafka发送数据不成功（连接失败）
```
目前的解决办法是在客户端机器上配置Kafka主机名与IP映射，应该与Kafka使用主机名有关
192.168.0.161   v161.zlikun.com
192.168.0.162   v162.zlikun.com
192.168.0.163   v163.zlikun.com
```