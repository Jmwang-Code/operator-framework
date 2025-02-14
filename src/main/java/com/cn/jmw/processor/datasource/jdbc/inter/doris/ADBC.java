package com.cn.jmw.processor.datasource.jdbc.inter.doris;

//import org.apache.commons.dbutils.ResultSetHandler;
import com.cn.jmw.processor.datasource.jdbc.inter.ResultSetHandler;
/**
 * Arrow flight sql 协议
 * 由Apache Arrow社区开源
 * 对列式数据库的可以提供 高速数据传输链路
 */
public interface ADBC {

    /**
     * 使用ADBC协议 流批执行查询操作
     *
     * @param sql     要执行的SQL查询语句
     * @param handler 处理结果集的处理器
     * @param <T>     返回类型
     * @return 查询结果，由ResultSetHandler处理
     * @throws Exception 如果数据库访问错误或其他错误
     */
    <T> T queryStreamADBC(String sql, ADBCQueryResultHandler<T> handler) throws Exception;

    <T> T jdbcConnectWithArrowFlightSQL(String sql, ResultSetHandler<T> handler) throws Exception;
}
