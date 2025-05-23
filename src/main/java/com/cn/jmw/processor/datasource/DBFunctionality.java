package com.cn.jmw.processor.datasource;

import com.cn.jmw.pojo.SqlQueryMontage;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Jmwang
 */
public interface DbFunctionality {

    Pattern RANDOM_SAMPLING_COMPILE = Pattern.compile("(LIMIT|limit)");

    /**
     * 添加随机采样
     * <h1>不允许出现 ORDER BY、LIMIT等字眼</h1>
     *
     * @param sqlQueryMontage SQL 查询的封装对象
     * @param n               抽样的数量
     * @param m               限制的数量
     * @return 增加随机抽样后的SQL列表
     */
    List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m);

    /**
     * N点抽样法 - 根据N个抽样点获取前后M条数据
     *
     * @param sqlQueryMontage 前提是简单SQL不含嵌套
     * @param n   抽样点N个数
     * @param m   一共M条数据
     * @return 抽样数据
     */
    List<String> n_point_sampling_method(SqlQueryMontage sqlQueryMontage, int n, int m);

}
