package com.cn.jmw.processor.datasource;
import com.cn.jmw.processor.datasource.jdbc.inter.ResultSetHandler;
//import org.apache.commons.dbutils.ResultSetHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * SQLDatabaseQuery接口用于定义与SQL数据库相关的查询操作。
 * 该接口提供了批量查询和流式查询的功能。
 */
public interface SQLDatabaseQuery extends DatabaseQuery{

    /**
     * 执行DML操作。
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @return 查询结果的列表，每个结果是一个包含字段名和对应值的Map
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    List<Map<String, Object>> executeDMLC(String sql, Object[] params) throws SQLException;

    List<Map<String, Object>> executeDMLC(Connection connection, String sql, Object[] params) throws SQLException;

    /**
     * 执行DML操作但是只想知道SQL是否只想完毕
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @return boolean
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    int executeDMLRUD(String sql, Object[] params) throws SQLException;

    /**
     * 执行DDL操作。
     *
     * @param sql    要执行的DDL SQL语句
     * @param params DDL SQL语句的参数
     * @throws SQLException 如果发生SQL错误
     */
    public void executeDDL(String sql, Object[] params) throws SQLException;

    /**
     * 执行流式查询操作。
     *
     * @param sql    要执行的SQL查询语句
     * @param handler 处理结果集的处理器
     * @param <T>    返回类型
     * @return 查询结果，由ResultSetHandler处理
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    <T> T queryStream(String sql, ResultSetHandler<T> handler, Integer size) throws SQLException;
}