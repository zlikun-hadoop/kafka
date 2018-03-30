#### 创建Topic
```
$ ./bin/kafka-topics.sh --create --zookeeper localhost:2184 --replication-factor 1 --partitions 1 --topic logs
```

#### 遍历Topic
```
$ ./bin/kafka-topics.sh --list --zookeeper localhost:2184
logs
```

#### 生产者
```
$ ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic logs
>hello
>kafka
```

#### 消费者
```
$ ./bin/kafka-console-consumer.sh --zookeeper localhost:2184 --topic logs
U_00001
U_00002
hello
kafka
```