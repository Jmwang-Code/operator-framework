package com.cn.jmw.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 样本结果
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SampleResult {

    /**
     * 抽样方式
     */
    String sample_result;

    /**
     * 抽样数量
     */
    long sample_count;

    /**
     * 抽样时间
     */
    LocalDateTime sample_time;
}
