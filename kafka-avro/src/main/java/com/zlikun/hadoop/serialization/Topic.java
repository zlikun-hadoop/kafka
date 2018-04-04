package com.zlikun.hadoop.serialization;

import com.zlikun.hadoop.avro.User;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.EnumSet;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-04 14:30
 */
public enum Topic {

    USER("topic-user-logs", new User());

    public final String topicName;
    public final SpecificRecordBase topicType;

    Topic(String topicName, SpecificRecordBase topicType) {
        this.topicName = topicName;
        this.topicType = topicType;
    }

    public static Topic matchFor(String topicName) {
        return EnumSet.allOf(Topic.class)
                .stream()
                .filter(topic -> topic.topicName.equals(topicName))
                .findFirst()
                .orElse(null);
    }

}
