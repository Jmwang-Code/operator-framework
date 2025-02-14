package com.cn.jmw.processor.datasource;

import com.cn.jmw.pojo.SQLQueryMontage;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.nosql.query.NoSQLQuery;
import com.cn.jmw.processor.datasource.pojo.DatabaseEntity;

import java.util.List;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.NOT_IMPLEMENTED_METHOD;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * NoSqlAdapter是一个抽象类，用于定义与NoSQL数据库的适配器。
 * 该类扩展了Database类，并实现了NoSqlDatabaseQuery接口。
 * <p>
 * 该类提供构造函数以初始化与NoSQL数据库的连接参数，如主机名、端口、数据库名、用户名和密码。
 * </p>
 */
public abstract class NoSqlAdapter extends Database implements NoSqlDatabaseQuery {
    /**
     * 构造函数用于创建NoSqlAdapter实例。
     *
     * @param hostname     数据库主机名
     * @param port         数据库端口
     * @param databaseName 数据库名称
     * @param username     数据库用户名
     * @param password     数据库密码
     */
    public NoSqlAdapter(String hostname, Integer port, String databaseName, String username, String password) {
        super(hostname, port, databaseName, username, password);
    }

    /**
     * 给SQL字符串增加随机抽样
     *
     * @param sql
     * @return 增加随机抽样后的SQL
     */
    public String addRandomSampling(String sql, long limit) {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    @Override
    public boolean testConnection() {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    @Override
    public String getDatabaseVersion() {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    @Override
    public List<String> getIgnoreDatabaseList() {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    @Override
    public List query(NoSQLQuery noSQLQuery) {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    @Override
    public List<String> addRandomSampling(SQLQueryMontage sqlQueryMontage, int N, int M) {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    @Override
    public List<String> N_point_sampling_method(SQLQueryMontage sqlQueryMontage, int N, int M) {
        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }
}