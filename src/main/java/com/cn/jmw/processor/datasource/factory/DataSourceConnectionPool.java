package com.cn.jmw.processor.datasource.factory;

import com.cn.jmw.processor.datasource.JDBCAdapter;
import com.cn.jmw.processor.datasource.pojo.JDBCAdapterDataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 数据源连接池
 * <p>
 * 该类实现了一个符合JDBC规范的连接池，管理数据库连接的创建、获取和关闭。
 * 采用单例模式，以确保全局范围内只有一个连接池实例。
 * </p>
 */
@Slf4j
public class DataSourceConnectionPool {
    //连接池
    private static volatile  DataSourceConnectionPool instance;
    //德鲁伊数据源缓存池
    private final Map<String, HikariDataSource> dataSourceMap = new ConcurrentHashMap<>();;

    /**
     * 私有构造函数，用于初始化数据源和连接缓存池。
     * <p>
     * 使用LinkedHashMap来管理连接，最近访问的连接将被移动到队列的尾部。
     * </p>
     *
     * 热点连接（最近访问的连接）会被移动到队列的尾部，而冷点连接（最近最少访问的连接）会被移动到队列的头部。
     * 当队列满了，最老的元素（也就是队列头部的元素）会被移除，并且对应的数据库连接会被关闭。
     */
    private DataSourceConnectionPool() {
//        this.connectionMap = new LinkedHashMap<>(DEFAULT_POOL_SIZE, 1.0f, true);
//        startIdleConnectionCleaner(); // 启动定时任务
    }

    /**
     * 私有构造函数，用于初始化数据源和连接缓存池。
     * <p>
     * 使用LinkedHashMap来管理连接，最近访问的连接将被移动到队列的尾部。
     * </p>
     *
     * 热点连接（最近访问的连接）会被移动到队列的尾部，而冷点连接（最近最少访问的连接）会被移动到队列的头部。
     * 当队列满了，最老的元素（也就是队列头部的元素）会被移除，并且对应的数据库连接会被关闭。
     * @param poolSize 连接池大小
     */
    private DataSourceConnectionPool(int poolSize) {
//        this.connectionMap = new LinkedHashMap<>(DEFAULT_POOL_SIZE, 1.0f, true);
//        startIdleConnectionCleaner(); // 启动定时任务
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
     * 获取数据源连接池的唯一实例。
     *
     * @param poolSize 连接池大小
     * @return 单例的DataSourceConnectionPool实例
     */
    public static synchronized DataSourceConnectionPool getInstance(int poolSize) {
        if (instance == null) {
            synchronized (DataSourceConnectionPool.class) {
                if (instance == null) {
                    instance = new DataSourceConnectionPool(poolSize);
                }
            }
        }
        return instance;
    }

    /**
     * 添加数据源到连接池中。
     *
     * @param id          数据源ID
     * @param dataSource  待添加的数据源
     */
    public void addDataSource(String id, HikariDataSource dataSource) {
        dataSourceMap.put(id, dataSource);
    }

    /**
     * 添加数据源到连接池中。
     *
     * @param id          数据源ID
     */
    public HikariDataSource getDataSource(String id) {
        return dataSourceMap.get(id);
    }

    public synchronized DataSource createDataSource(String id, String connectionString, String username, String password, JDBCAdapterDataSourceConfig config) {
        HikariDataSource dataSource = dataSourceMap.get(id);

        if (dataSource == null) {
            if (config == null) config = new JDBCAdapterDataSourceConfig();
            switch (config.getDataSourcePool()) {
                case HIKARICP -> {
                    dataSource = createHikariDataSource(id, connectionString, username, password,config);
                    break;
                }
                case DBCP -> throw new UnsupportedOperationException("暂不支持DBCP配置");
                case DRUID -> throw new UnsupportedOperationException("暂不支持Druid配置");
                case C3P0 -> throw new UnsupportedOperationException("暂不支持C3P0配置");
                case TOMCAT_JDBC_POOL -> throw new UnsupportedOperationException("暂不支持TOMCAT_JDBC_POOL配置");
                default -> dataSource = createHikariDataSource(id, connectionString, username, password,config);
            }
//            // 空闲连接数
//            config.setMinimumIdle(5);
//            // 最大连接数
//            config.setMaximumPoolSize(maxActive);
//            // 获取连接超时时间
//            config.setConnectionTimeout(30000);
//            // 空闲连接的超时时间
//            config.setIdleTimeout(30000);
//            // 连接的最大存活时间
//            config.setMaxLifetime(600000);
//            // 验证连接有效性的超时时间
//            config.setValidationTimeout(5000);
//            config.setConnectionTestQuery("SELECT 1");
//
//            // 每 30 秒检查一次连接可用性
//            config.setKeepaliveTime(30000);
//            // 每次获取连接前验证有效性
//            config.setValidationTimeout(5000);

//            检测1小时未归还的
//            config.setLeakDetectionThreshold(3600000);
        }
        return dataSource;
    }

    private HikariDataSource createHikariDataSource(String id, String connectionString, String username, String password,JDBCAdapterDataSourceConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(connectionString);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(config.getMaxActive());
        hikariConfig.setMinimumIdle(config.getMinIdle());
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        dataSourceMap.put(id, dataSource);
        return dataSource;
    }

    /**
     * 获取指定ID的数据源连接。
     * <p>
     * 首先尝试从连接缓存池中获取连接。如果未找到或连接已关闭，则从数据源中创建新的连接。
     * </p>
     *
     * getConnection方法首先尝试从connectionMap中获取连接。
     * 如果获取的连接为null或已经被关闭，那么它会从数据源中获取一个新的连接，并将其添加到connectionMap中。
     * 这样，每次访问一个连接（无论是获取还是添加），该连接都会被移动到队列的尾部。
     * @param id 数据源ID
     * @param dbName 需要访问的数据库名
     * @return 对应ID的数据库连接
     * @throws SQLException 如果获取连接失败
     */
    public Connection getConnection(String id, String connectionString, String username, String password, String dbName, JDBCAdapterDataSourceConfig config,String connectionUser) throws SQLException {
        DataSourceConnectionPool pool = DataSourceConnectionPool.getInstance(); // 数据源连接池实例
        DataSource dataSource = pool.getDataSource(id);

        if (dataSource == null) {
            synchronized (this) {
                // 确保线程安全的单例初始化
                dataSource = pool.createDataSource(id, connectionString, username, password, config);
            }
        }

        if (dataSource instanceof HikariDataSource){
            HikariDataSource hikariDataSource = (HikariDataSource)dataSource;
            HikariPoolMXBean poolMXBean = hikariDataSource.getHikariPoolMXBean();
            log.info("| 连接使用者(当前使用模块): "+connectionUser+" | 总连接数: " + poolMXBean.getTotalConnections() + " | 活动连接: " + poolMXBean.getActiveConnections() + " | 空闲连接: " + poolMXBean.getIdleConnections() +" |等待连接的线程数量: " + poolMXBean.getThreadsAwaitingConnection() +" |");
        }

        try {
            Connection connection = getValidConnection(dataSource, dbName, 3); // 设置重试次数为 3
            // 使用连接
            return connection;
        } catch (SQLException e) {
            log.error("获取数据库连接失败: ", e);
            throw e;
        }

    }

    public Connection getValidConnection(DataSource dataSource, String dbName, int retryCount) throws SQLException {
        int attempts = 0;
        Connection connection = null;

        while (attempts < retryCount) {
            try {
                connection = dataSource.getConnection(); // 从池中获取连接
                if (connection != null && !connection.isClosed()) {
                    // 设置数据库名（可选）
                    if (StringUtils.isNotBlank(dbName)) {
                        connection.setCatalog(dbName);
                    }
                    return connection; // 返回有效连接
                }
            } catch (SQLException e) {
                log.warn("第 {} 次尝试获取连接失败: {}", attempts + 1, e.getMessage());
            } finally {
                attempts++;
            }
        }

        // 如果重试多次后仍未获取到有效连接，则抛出异常
        throw new SQLException("无法获取有效的数据库连接，重试次数: " + retryCount);
    }



    /**
     * 获取指定ID的数据源连接。
     * <p>
     * 首先尝试从连接缓存池中获取连接。如果未找到或连接已关闭，则从数据源中创建新的连接。
     * </p>
     *
     * getConnection方法首先尝试从connectionMap中获取连接。
     * 如果获取的连接为null或已经被关闭，那么它会从数据源中获取一个新的连接，并将其添加到connectionMap中。
     * 这样，每次访问一个连接（无论是获取还是添加），该连接都会被移动到队列的尾部。
     * @param id 数据源ID
     * @return 对应ID的数据库连接
     * @throws SQLException 如果获取连接失败
     */
    public Connection getConnection(String id) throws SQLException {
       return getConnection(id,null,null,null,null,null,null);
    }

    public Connection getConnection(String id,JDBCAdapterDataSourceConfig config,String connectionUser) throws SQLException {
        return getConnection(id,null,null,null,null,config,connectionUser);
    }

}