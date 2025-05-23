package com.cn.jmw.processor.datasource.jdbc.inter.doris;

import com.cn.jmw.processor.datasource.jdbc.inter.ResultSetHandler;
/**
 * Arrow flight sql 协议
 * 由Apache Arrow社区开源
 * 对列式数据库的可以提供 高速数据传输链路
 *
 * @author Jmwang
 */
public interface Adbc {

    /**
     * 使用Adbc协议 流批执行查询操作
     *
     * @param sql     要执行的SQL查询语句
     * @param handler 处理结果集的处理器
     * @param <T>     返回类型
     * @return 查询结果，由ResultSetHandler处理
     * @throws Exception 如果数据库访问错误或其他错误
     */
    <T> T queryStreamAdbc(String sql, AdbcQueryResultHandler<T> handler) throws Exception;

    /**
     * 使用Arrow flight sql协议 流批执行查询操作
     *
     * @param sql      要执行的SQL查询语句
     * @param handler  处理结果集的处理器
     * @param <T>      返回类型
     * @return 查询结果，由ResultSetHandler处理
     * @throws Exception 如果数据库访问错误或其他错误
     */
    <T> T jdbcConnectWithArrowFlightSql(String sql, ResultSetHandler<T> handler) throws Exception;
}
