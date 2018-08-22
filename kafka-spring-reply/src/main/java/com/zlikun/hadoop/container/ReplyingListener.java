package com.zlikun.hadoop.container;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import static com.zlikun.hadoop.conf.KReplyingConfigure.REQUEST_TOPIC;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/22 16:48
 */
@Component
public class ReplyingListener {

    @PostConstruct
    public void init() {
        System.out.println("--init--");
    }

    @KafkaListener(id = "server", topics = REQUEST_TOPIC, containerFactory = "containerFactory")
//    @SendTo // use default replyTo expression
    public String listen(String in) {
        System.out.println("7777777777");
        System.out.println("Server received: " + in);
        return in.toUpperCase();
    }

    @PreDestroy
    public void destroy() {
        System.out.println("--destroy--");
    }

}
