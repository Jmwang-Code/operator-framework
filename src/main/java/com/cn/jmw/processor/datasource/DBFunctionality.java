package com.cn.jmw.processor.datasource;

import com.cn.jmw.pojo.SQLQueryMontage;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public interface DBFunctionality {

    Pattern RandomSamplingCompile = Pattern.compile("(LIMIT|limit)");

    /**
     * 给SQL字符串增加随机抽样
     *
     * @param sqlQueryMontage
     * @return 增加随机抽样后的SQL
     */
    List<String> addRandomSampling(SQLQueryMontage sqlQueryMontage, int N, int M);

    /**
     * N点抽样法 - 根据N个抽样点获取前后M条数据
     *
     * @param sqlQueryMontage 前提是简单SQL不含嵌套
     * @param N   抽样点N个数
     * @param M   一共M条数据
     * @return 抽样数据
     */
    List<String> N_point_sampling_method(SQLQueryMontage sqlQueryMontage, int N, int M);

}
