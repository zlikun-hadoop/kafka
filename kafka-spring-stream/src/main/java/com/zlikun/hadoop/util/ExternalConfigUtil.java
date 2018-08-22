package com.zlikun.hadoop.util;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 外部配置文件读取类（仅用于读取不能或不方便对外暴露的配置信息，如帐号、密码等）
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/22 13:18
 */
public class ExternalConfigUtil {

    private static final String propsFile;
    private static final Properties props;

    static {
        // 实际配置文件路径根据需要自行修改吧
        propsFile = Paths.get("/etc/kafka.properties").toAbsolutePath().toString();
        props = new Properties();
        try {
            props.load(new FileReader(propsFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static final String getString(String key) {
        return props.getProperty(key);
    }

    public static final String getString(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    public static final Integer getInt(String key) {
        return getInt(key, null);
    }

    public static final Integer getInt(String key, Integer defaultValue) {
        String value = getString(key);
        if (value == null) return defaultValue;
        return Integer.valueOf(value);
    }

}
