package com.cn.jmw.processor.datasource;

import com.cn.jmw.pojo.SqlQueryMontage;
import com.cn.jmw.processor.datasource.nosql.query.NoSqlQuery;
import com.cn.jmw.processor.datasource.pojo.DatabaseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.NOT_IMPLEMENTED_METHOD;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * NoSqlAdapter是一个抽象类，用于定义与NoSQL数据库的适配器。
 * 该类扩展了Database类，并实现了NoSqlDatabaseQuery接口。
 * <p>
 * 该类提供构造函数以初始化与NoSQL数据库的连接参数，如主机名、端口、数据库名、用户名和密码。
 * </p>
 *
 * @author Jmwang
 */
public abstract class AbstractNoSqlAdapter extends AbstractDatabase implements NoSqlDatabaseQuery {

    private static final Logger logger = LoggerFactory.getLogger(AbstractNoSqlAdapter.class);
    protected final NoSqlConnectionStrategy connectionStrategy;

    /**
     * 构造函数用于创建NoSqlAdapter实例。
     *
     * @param hostname     数据库主机名
     * @param port         数据库端口
     * @param databaseName 数据库名称
     * @param username     数据库用户名
     * @param password     数据库密码
     */
    public AbstractNoSqlAdapter(String hostname, Integer port, String databaseName, String username, String password, NoSqlConnectionStrategy strategy) {
        super(hostname, port, databaseName, username, password);
        this.connectionStrategy = strategy != null ? strategy : new DefaultNoSqlConnectionStrategy();
    }

    public AbstractNoSqlAdapter(String hostname, Integer port, String databaseName, String username, String password) {
        this(hostname, port, databaseName, username, password, null);
    }

    /**
     * 获取 NoSQL 连接 URI
     *
     * @return 连接 URI
     */
    protected String getConnectionUri() {
        return connectionStrategy.getConnectionUri(hostname, port, databaseName);
    }

    @Override
    public String getConnectionString() {
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    /**
     * 给SQL字符串增加随机抽样
     *
     * @param sql  需要增加随机抽样的SQL
     * @return 增加随机抽样后的SQL
     */
    public String addRandomSampling(String sql, long limit) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    @Override
    public boolean testConnection() {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    @Override
    public String getDatabaseVersion() {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    @Override
    public List<String> getIgnoreDatabaseList() {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    @Override
    public List<?> query(NoSqlQuery noSqlQuery) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    @Override
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    @Override
    public List<String> n_point_sampling_method(SqlQueryMontage sqlQueryMontage, int n, int m) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    /**
     * 默认 NoSQL 连接策略（占位）
     */
    private static class DefaultNoSqlConnectionStrategy implements NoSqlConnectionStrategy {
        @Override
        public String getConnectionUri(String hostname, Integer port, String databaseName) {
            throw exception(NOT_IMPLEMENTED_METHOD);
        }
    }
}