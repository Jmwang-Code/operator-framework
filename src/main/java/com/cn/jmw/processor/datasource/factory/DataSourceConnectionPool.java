package com.cn.jmw.processor.datasource.factory;

import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据源连接池管理类
 * <p>
 * 该类实现了一个基于 HikariCP 的数据库连接池，采用单例模式管理全局数据源。
 * 提供数据源的创建、获取和连接管理功能，支持动态配置和连接重试机制。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class DataSourceConnectionPool {

    /**
     * 连接池单例实例
     */
    private static volatile DataSourceConnectionPool instance;
    /**
     * 数据源缓存池
     */
    private final Map<String, HikariDataSource> dataSourceMap = new ConcurrentHashMap<>();

    /**
     * 私有构造函数，用于初始化数据源和连接缓存池。
     * <p>
     * 使用LinkedHashMap来管理连接，最近访问的连接将被移动到队列的尾部。
     * </p>
     * <p>
     * 热点连接（最近访问的连接）会被移动到队列的尾部，而冷点连接（最近最少访问的连接）会被移动到队列的头部。
     * 当队列满了，最老的元素（也就是队列头部的元素）会被移除，并且对应的数据库连接会被关闭。
     */
    private DataSourceConnectionPool() {
    }

    /**
     * 获取数据源连接池的唯一实例。
     *
     * @return 单例的DataSourceConnectionPool实例
     */
    public static synchronized DataSourceConnectionPool getInstance() {
        if (instance == null) {
            synchronized (DataSourceConnectionPool.class) {
                if (instance == null) {
                    instance = new DataSourceConnectionPool();
                }
            }
        }
        return instance;
    }

    /**
     * 获取指定 ID 的数据源。
     *
     * @param id 数据源 ID
     * @return HikariDataSource 实例，如果不存在则返回 null
     */
    public HikariDataSource getDataSource(String id) {
        return dataSourceMap.get(id);
    }

    /**
     * 添加数据源到连接池中。
     *
     * @param id         数据源ID
     * @param dataSource 待添加的数据源
     */
    public void addDataSource(String id, HikariDataSource dataSource) {
        dataSourceMap.put(id, dataSource);
    }

    /**
     * 创建并添加数据源到连接池中。
     *
     * @param id              数据源ID
     * @param connectionString 数据库连接字符串
     * @param username        数据库用户名
     * @param password        数据库密码
     * @param config          数据源配置
     * @return 返回创建的数据源
     */
    public synchronized DataSource createDataSource(String id, String connectionString, String username, String password, JdbcAdapterDataSourceConfig config) {
        HikariDataSource dataSource = dataSourceMap.get(id);

        if (dataSource == null) {
            if (config == null) {
                config = new JdbcAdapterDataSourceConfig();
            }
            switch (config.getDataSourcePool()) {
//                case HIKARICP -> dataSource = createHikariDataSource(id, connectionString, username, password, config);
                case DBCP -> throw new UnsupportedOperationException("暂不支持DBCP配置");
                case DRUID -> throw new UnsupportedOperationException("暂不支持Druid配置");
                case C3P0 -> throw new UnsupportedOperationException("暂不支持C3P0配置");
                case TOMCAT_JDBC_POOL -> throw new UnsupportedOperationException("暂不支持TOMCAT_JDBC_POOL配置");
                default -> dataSource = createHikariDataSource(id, connectionString, username, password, config);
            }
        }
        return dataSource;
    }

    /**
     * 创建 HikariCP 数据源。
     *
     * @param id              数据源 ID
     * @param connectionString 数据库连接字符串
     * @param username        数据库用户名
     * @param password        数据库密码
     * @param config          数据源配置
     * @return 创建的 HikariDataSource 实例
     */
    private HikariDataSource createHikariDataSource(String id, String connectionString, String username, String password, JdbcAdapterDataSourceConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        //基础配置
        hikariConfig.setJdbcUrl(connectionString);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);

        //常用配置
        int maxPoolSize = Math.max(config.getMaximumPoolSize(), 10);
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setMinimumIdle(config.getMinimumIdle() > 0 ? config.getMinimumIdle() : maxPoolSize / 4);
        hikariConfig.setAutoCommit(config.isAutoCommit());
        hikariConfig.setConnectionTimeout(Math.max(config.getConnectionTimeout(), 250));
        if (config.getIdleTimeout()>0){
            hikariConfig.setIdleTimeout(config.getIdleTimeout());
        }
        if (config.getKeepaliveTime()>0){
            hikariConfig.setKeepaliveTime(config.getKeepaliveTime());
        }
        if (config.getMaxLifetime()>0){
            hikariConfig.setMaxLifetime(config.getMaxLifetime());
        }
        if (StringUtils.isNotBlank(config.getConnectionTestQuery())){
            hikariConfig.setConnectionTestQuery(config.getConnectionTestQuery());
        }
        if (StringUtils.isNotBlank(config.getPoolName())){
            hikariConfig.setPoolName(config.getPoolName());
        }

        //不常用配置
        hikariConfig.setInitializationFailTimeout(config.getInitializationFailTimeout());
        hikariConfig.setIsolateInternalQueries(config.isIsolateInternalQueries());
        hikariConfig.setAllowPoolSuspension(config.isAllowPoolSuspension());
        hikariConfig.setReadOnly(config.isReadOnly());
        hikariConfig.setRegisterMbeans(config.isRegisterMbeans());
        if (StringUtils.isNotBlank(config.getConnectionInitSql())){
            hikariConfig.setConnectionInitSql(config.getConnectionInitSql());
        }
        hikariConfig.setValidationTimeout(config.getValidationTimeout()>=250?config.getValidationTimeout():5000);
        hikariConfig.setLeakDetectionThreshold(config.getLeakDetectionThreshold()>=2000?config.getLeakDetectionThreshold():0);


        HikariDataSource dataSource = null;
        try {
            dataSource = new HikariDataSource(hikariConfig);
            dataSourceMap.put(id, dataSource);
        }catch (Exception e){
            e.printStackTrace();
            log.error("HikariDataSource创建失败: {}", e.getMessage());
        }

        return dataSource;
    }


    /**
     * 获取指定ID的数据源连接。
     *
     * @param id 数据源ID
     * @return 对应ID的数据库连接
     * @throws SQLException 如果获取连接失败
     */
    public Connection getConnection(String id) throws SQLException {
        return getConnection(id, null, null, null, null, null, null);
    }

    /**
     * 获取指定ID的数据源连接。
     * <p>
     * 首先尝试从连接缓存池中获取连接。如果未找到或连接已关闭，则从数据源中创建新的连接。
     * </p>
     * <p>
     * getConnection方法首先尝试从connectionMap中获取连接。
     * 如果获取的连接为null或已经被关闭，那么它会从数据源中获取一个新的连接，并将其添加到connectionMap中。
     * 这样，每次访问一个连接（无论是获取还是添加），该连接都会被移动到队列的尾部。
     *
     * @param id             数据源 ID
     * @param config         数据源配置
     * @param connectionUser 连接用户
     * @return 数据库连接
     * @throws SQLException 如果获取连接失败
     */
    public Connection getConnection(String id, JdbcAdapterDataSourceConfig config, String connectionUser) throws SQLException {
        return getConnection(id, null, null, null, null, config, connectionUser);
    }

    /**
     * 获取指定ID的数据源连接。
     * <p>
     * 首先尝试从连接缓存池中获取连接。如果未找到或连接已关闭，则从数据源中创建新的连接。
     * </p>
     * <p>
     * getConnection方法首先尝试从connectionMap中获取连接。
     * 如果获取的连接为null或已经被关闭，那么它会从数据源中获取一个新的连接，并将其添加到connectionMap中。
     * 这样，每次访问一个连接（无论是获取还是添加），该连接都会被移动到队列的尾部。
     *
     * @param id              数据源 ID
     * @param connectionString 数据库连接字符串
     * @param username        数据库用户名
     * @param password        数据库密码
     * @param dbName          数据库名称
     * @param config          数据源配置
     * @param connectionUser  连接用户
     * @return 数据库连接
     * @throws SQLException 如果获取连接失败
     */
    public Connection getConnection(String id, String connectionString, String username, String password, String dbName, JdbcAdapterDataSourceConfig config, String connectionUser) throws SQLException {
        // 数据源连接池实例
        DataSourceConnectionPool pool = DataSourceConnectionPool.getInstance();
        DataSource dataSource = pool.getDataSource(id);

        if (dataSource == null) {
            synchronized (this) {
                // 确保线程安全的单例初始化
                dataSource = pool.createDataSource(id, connectionString, username, password, config);
            }
        }

        if (dataSource instanceof HikariDataSource hikariDataSource) {
            HikariPoolMXBean poolMxBean = hikariDataSource.getHikariPoolMXBean();
            log.info("| 连接使用者: {} | 总连接数: {} | 活动连接: {} | 空闲连接: {} | 等待线程数: {} |",
                    connectionUser, poolMxBean.getTotalConnections(), poolMxBean.getActiveConnections(),
                    poolMxBean.getIdleConnections(), poolMxBean.getThreadsAwaitingConnection());
        }

        return getValidConnection(dataSource, dbName, 3);
    }

    /**
     * 获取有效的数据库连接，支持重试机制。
     *
     * @param dataSource 数据源
     * @param dbName     数据库名称
     * @param retryCount 重试次数
     * @return 有效的数据库连接
     * @throws SQLException 如果重试后仍无法获取连接
     */
    private Connection getValidConnection(DataSource dataSource, String dbName, int retryCount) throws SQLException {
        int attempts = 0;
        while (attempts < retryCount) {
            try {
                Connection connection = dataSource.getConnection();
                if (connection != null && !connection.isClosed()) {
                    if (StringUtils.isNotBlank(dbName)) {
                        connection.setCatalog(dbName);
                    }
                    return connection;
                }
            } catch (SQLException e) {
                log.warn("第 {} 次尝试获取连接失败: {}", attempts + 1, e.getMessage());
            }
            attempts++;
        }
        throw new SQLException("无法获取有效的数据库连接，重试次数: " + retryCount);
    }

}