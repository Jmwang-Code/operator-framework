package com.cn.jmw.processor.datasource.jdbc.inter.doris;

import java.util.List;
import java.util.Map;

/**
 * ADBCQueryResultHandler接口定义了处理ADBC查询结果的方法。
 * <p>
 * 该接口是一个函数式接口，提供了处理查询结果的方法。
 * </p>
 *
 * @param <T> 处理结果的类型
 *
 * @autor Jmwang
 */
@FunctionalInterface
public interface AdbcQueryResultHandler<T> {

    /**
     * 处理查询结果。
     *
     * @param batch 查询结果的批次，包含每行数据的映射
     * @return 返回处理后的结果
     * @throws Exception 如果处理过程中发生异常
     */
    T handle(List<Map<String, Object>> batch) throws Exception;
}