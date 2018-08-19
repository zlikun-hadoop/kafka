package com.zlikun.hadoop.stock;

import lombok.Data;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/8/19 16:22
 */
@Data
public class Trade {

    // 股票代码
    private String code;
    // 实时价
    private Double currentPrice;
    // 开盘价
    private Double openingPrice;
    // 收盘价
    // private Double closingPrice;

}
