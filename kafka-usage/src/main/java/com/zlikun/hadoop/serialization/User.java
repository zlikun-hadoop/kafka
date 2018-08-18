package com.zlikun.hadoop.serialization;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-30 18:34
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {

    private Long id;
    private String name;
    private LocalDate birthday;

}
