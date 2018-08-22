package com.zlikun.hadoop.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/22 13:49
 */
class ExternalConfigUtilTest {
    @Test
    void getString() {
        assertEquals("192.168.0.161:9200,192.168.0.162:9200,192.168.0.163:9200",
                ExternalConfigUtil.getString("bootstrap.servers"));
    }

}