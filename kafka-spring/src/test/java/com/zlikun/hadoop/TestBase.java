package com.zlikun.hadoop;

import com.zlikun.hadoop.util.ExternalConfigUtil;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-04-02 15:20
 */
public abstract class TestBase {

    String servers = ExternalConfigUtil.getString("bootstrap.servers");
    String topic = "logs" ;

}
