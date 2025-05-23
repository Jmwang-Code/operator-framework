package com.cn.jmw.processor.datasource.factory;

import com.cn.jmw.processor.datasource.*;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.jdbc.adapter.*;
import com.cn.jmw.processor.datasource.nosql.adapter.*;
import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * 数据库适配器工厂类
 * <p>
 * 该类根据数据库类型动态创建 JDBC 或 NoSQL 适配器实例，支持多种数据库的连接和操作。
 * 通过静态映射表注册支持的适配器，并提供方法获取对应的适配器实例。
 * 为 JDBC 适配器注入 ConnectionStringStrategy，以支持传入的 URL 或生成的连接字符串。
 * </p>
 *
 * @author Jmwang
 */
public class DatabaseAdapterFactory {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseAdapterFactory.class);

    /**
     * JDBC 适配器字典
     */
    private static final Map<DatabaseEnum, Class<? extends AbstractJdbcAdapter>> JDBC_ADAPTER_MAP = new HashMap<>();

    /**
     * NoSQL 适配器字典
     */
    private static final Map<DatabaseEnum, Class<? extends AbstractNoSqlAdapter>> NOSQL_ADAPTER_MAP = new HashMap<>();

    static {
        registerJdbcAdapters();
        registerNoSqlAdapters();
    }

    /**
     * 注册支持的 JDBC 适配器
     */
    private static void registerJdbcAdapters() {
        JDBC_ADAPTER_MAP.put(DatabaseEnum.ALIYUN_RDS, AliyunRdsJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.CLICKHOUSE, ClickHouseJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.DB2, Db2JdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.DERBY, DerbyJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.DM, DmJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.DORIS, DorisJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.GAUSSDB, GaussDbJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.GBASE8A, Gbase8JdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.GBASE, GbaseJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.GOLDENDB, GoldenDbJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.GREENPLUM, GreenplumJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.KINGBASE8, KingBase8JdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.KUNDB, KunDbJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.MYSQL, MySqlJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.OCEANBASE, OceanBaseJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.OPENGAUSS, OpenGaussJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.ORACLE, OracleJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.OSCAR, OscarJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.POSTGRESQL, PostgreSqlJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.SQLITE3, SQLite3JDBCAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.SQLSERVER, SqlServerJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.STARROCKS, StarRocksJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.SYBASE, SybaseJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.TDSQL, TdSqlJdbcAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.TIDB, TIDBJDBCAdapter.class);
        JDBC_ADAPTER_MAP.put(DatabaseEnum.POLAR, PolarJdbcAdapter.class);
    }

    /**
     * 注册支持的 NoSQL 适配器
     */
    private static void registerNoSqlAdapters() {
        NOSQL_ADAPTER_MAP.put(DatabaseEnum.MONGODB, MongoDbJdbcAdapter.class);
        NOSQL_ADAPTER_MAP.put(DatabaseEnum.ALIYUN_ODPS, AliyunOdpsJdbcAdapter.class);
        NOSQL_ADAPTER_MAP.put(DatabaseEnum.SELECTDB, SelectDbJdbcAdapter.class);
        NOSQL_ADAPTER_MAP.put(DatabaseEnum.HIVE, HiveJdbcAdapter.class);
    }

    /**
     * 通过适配器类和连接实体获取数据库实例
     *
     * @param jdbcConnectionEntity 数据库连接实体
     * @param adapterClass         适配器类
     * @return 数据库适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    public static <T> AbstractDatabase getDatabase(JdbcConnectionEntity jdbcConnectionEntity, Class<T> adapterClass) throws IllegalAccessException, InstantiationException {
        return (AbstractDatabase) getAdapter(jdbcConnectionEntity, adapterClass);
    }

    /**
     * 获取 JDBC 适配器实例
     *
     * @param jdbcConnectionEntity 数据库连接实体
     * @return JDBC 适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    public static AbstractJdbcAdapter getSqlAdapter(JdbcConnectionEntity jdbcConnectionEntity) throws IllegalAccessException, InstantiationException {
        if (jdbcConnectionEntity == null) {
            logger.error("连接实体为空");
            throw exception(INPUT_IS_NULL);
        }
        // 假设 JdbcConnectionEntity 有 getUrl() 方法
        String url = jdbcConnectionEntity.getUrl() != null ? jdbcConnectionEntity.getUrl() : null;
        // 如果 JdbcConnectionEntity 有 getUrl()，取消注释以下行
        // String url = jdbcConnectionEntity.getUrl();
        return getSqlAdapter(
                jdbcConnectionEntity.getDbType(),
                jdbcConnectionEntity.getAssetIp(),
                jdbcConnectionEntity.getPort(),
                jdbcConnectionEntity.getDbName(),
                jdbcConnectionEntity.getUsername(),
                jdbcConnectionEntity.getPassword(),
                jdbcConnectionEntity.getConfig(),
                jdbcConnectionEntity.getConnectionUser(),
                jdbcConnectionEntity.isTest(),
                url
        );
    }

    /**
     * 获取 NoSQL 适配器实例
     *
     * @param jdbcConnectionEntity 数据库连接实体
     * @return NoSQL 适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    public static AbstractNoSqlAdapter getNoSqlAdapter(JdbcConnectionEntity jdbcConnectionEntity) throws IllegalAccessException, InstantiationException {
        if (jdbcConnectionEntity == null) {
            logger.error("连接实体为空");
            throw exception(INPUT_IS_NULL);
        }
        // 假设 JdbcConnectionEntity 有 getUrl() 方法
        String url = jdbcConnectionEntity.getUrl() != null ? jdbcConnectionEntity.getUrl() : null;
        return getNoSqlAdapter(
                jdbcConnectionEntity.getDbType(),
                jdbcConnectionEntity.getAssetIp(),
                jdbcConnectionEntity.getPort(),
                jdbcConnectionEntity.getDbName(),
                jdbcConnectionEntity.getUsername(),
                jdbcConnectionEntity.getPassword(),
                jdbcConnectionEntity.getConfig(),
                jdbcConnectionEntity.getConnectionUser(),
                jdbcConnectionEntity.isTest(),
                url
        );
    }

    /**
     * 通过适配器类和连接实体获取具体适配器
     *
     * @param jdbcConnectionEntity 数据库连接实体
     * @param adapterClass         适配器类
     * @return 适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    public static <T> T getAdapter(JdbcConnectionEntity jdbcConnectionEntity, Class<T> adapterClass) throws IllegalAccessException, InstantiationException {
        if (jdbcConnectionEntity == null) {
            logger.error("连接实体为空");
            throw exception(INPUT_IS_NULL);
        }
        if (AbstractJdbcAdapter.class.isAssignableFrom(adapterClass)) {
            String url = jdbcConnectionEntity.getUrl() != null ? jdbcConnectionEntity.getUrl() : null;
            // 如果有 getUrl()，取消注释以下行
            // String url = jdbcConnectionEntity.getUrl();
            ConnectionStringStrategy strategy = url != null && !url.isBlank() ? new UrlConnectionStringStrategy(url) : new GeneratedConnectionStringStrategy(jdbcConnectionEntity.getDbType());
            AbstractJdbcAdapter adapter = createJdbcAdapter(
                    adapterClass.asSubclass(AbstractJdbcAdapter.class),
                    jdbcConnectionEntity.getDbType(),
                    jdbcConnectionEntity.getAssetIp(),
                    jdbcConnectionEntity.getPort(),
                    jdbcConnectionEntity.getDbName(),
                    jdbcConnectionEntity.getUsername(),
                    jdbcConnectionEntity.getPassword(),
                    jdbcConnectionEntity.getConfig(),
                    jdbcConnectionEntity.getConnectionUser(),
                    jdbcConnectionEntity.isTest(),
                    strategy
            );
            return (T) adapter;
        } else if (AbstractNoSqlAdapter.class.isAssignableFrom(adapterClass)) {
            return (T) createNoSqlAdapter(
                    adapterClass.asSubclass(AbstractNoSqlAdapter.class),
                    jdbcConnectionEntity.getDbType(),
                    jdbcConnectionEntity.getAssetIp(),
                    jdbcConnectionEntity.getPort(),
                    jdbcConnectionEntity.getDbName(),
                    jdbcConnectionEntity.getUsername(),
                    jdbcConnectionEntity.getPassword(),
                    jdbcConnectionEntity.getConfig(),
                    jdbcConnectionEntity.getConnectionUser(),
                    jdbcConnectionEntity.isTest()
            );
        } else {
            logger.error("无效的适配器类: {}", adapterClass.getSimpleName());
            throw exception(UNSUPPORTED_DATABASE_TYPE);
        }
    }

    /**
     * 获取 JDBC 适配器
     *
     * @param dbType         数据库类型
     * @param hostname       主机名
     * @param port           端口号
     * @param databaseName   数据库名称
     * @param username       用户名
     * @param password       密码
     * @param config         数据源配置
     * @param connectionUser 连接用户
     * @param test           是否为测试模式
     * @param url            可选的连接 URL
     * @return JDBC 适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    private static AbstractJdbcAdapter getSqlAdapter(DatabaseEnum dbType, String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, boolean test, String url) throws IllegalAccessException, InstantiationException {
        if (dbType == null) {
            logger.error("数据库类型为空");
            throw exception(DATABASE_INPUT_TYPE_ERROR);
        }
        Class<? extends AbstractJdbcAdapter> adapterClass = JDBC_ADAPTER_MAP.get(dbType);
        if (adapterClass == null) {
            logger.error("不支持的数据库类型: {}", dbType);
            throw exception(UNSUPPORTED_DATABASE_TYPE);
        }
        // 验证连接参数
        if (url == null && (hostname == null || hostname.isBlank() || port == null)) {
            logger.error("缺少连接参数：需要 url 或 hostname 和 port，dbType: {}", dbType);
            throw exception(DATABASE_INPUT_TYPE_ERROR, "缺少连接参数：需要 url 或 hostname 和 port");
        }
        ConnectionStringStrategy strategy = url != null && !url.isBlank() ? new UrlConnectionStringStrategy(url) : new GeneratedConnectionStringStrategy(dbType);
        return createJdbcAdapter(adapterClass, dbType, hostname, port, databaseName, username, password, config, connectionUser, test, strategy);
    }

    /**
     * 获取 NoSQL 适配器
     *
     * @param dbType         数据库类型
     * @param hostname       主机名
     * @param port           端口号
     * @param databaseName   数据库名称
     * @param username       用户名
     * @param password       密码
     * @param config         数据源配置
     * @param connectionUser 连接用户
     * @param test           是否为测试模式
     * @return NoSQL 适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    private static AbstractNoSqlAdapter getNoSqlAdapter(DatabaseEnum dbType, String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, boolean test, String url) throws IllegalAccessException, InstantiationException {
        if (dbType == null) {
            logger.error("数据库类型为空");
            throw exception(DATABASE_INPUT_TYPE_ERROR);
        }
        Class<? extends AbstractNoSqlAdapter> adapterClass = NOSQL_ADAPTER_MAP.get(dbType);
        if (adapterClass == null) {
            logger.error("不支持的数据库类型: {}", dbType);
            throw exception(UNSUPPORTED_DATABASE_TYPE);
        }
        // 验证连接参数
        if (url == null && (hostname == null || hostname.isBlank() || port == null)) {
            logger.error("缺少连接参数：需要 url 或 hostname 和 port，dbType: {}", dbType);
            throw exception(DATABASE_INPUT_TYPE_ERROR, "缺少连接参数：需要 url 或 hostname 和 port");
        }
        //TODO 非结构化数据，未开发
        return createNoSqlAdapter(adapterClass, dbType, hostname, port, databaseName, username, password, config, connectionUser, test);
    }

    /**
     * 创建 JDBC 适配器实例
     *
     * @param adapterClass   适配器类
     * @param dbType         数据库类型
     * @param hostname       主机名
     * @param port           端口号
     * @param databaseName   数据库名称
     * @param username       用户名
     * @param password       密码
     * @param config         数据源配置
     * @param connectionUser 连接用户
     * @param test           是否为测试模式
     * @param strategy       连接字符串策略
     * @return JDBC 适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    private static AbstractJdbcAdapter createJdbcAdapter(Class<? extends AbstractJdbcAdapter> adapterClass, DatabaseEnum dbType, String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, boolean test, ConnectionStringStrategy strategy) throws IllegalAccessException, InstantiationException {
        try {
            AbstractJdbcAdapter adapter;
            if (connectionUser == null) {
                adapter = adapterClass.getConstructor(String.class, Integer.class, String.class, String.class, String.class, JdbcAdapterDataSourceConfig.class, Boolean.class, ConnectionStringStrategy.class)
                        .newInstance(hostname, port, databaseName, username, password, config, test, strategy);
            } else {
                adapter = adapterClass.getConstructor(String.class, Integer.class, String.class, String.class, String.class, JdbcAdapterDataSourceConfig.class, String.class, Boolean.class, ConnectionStringStrategy.class)
                        .newInstance(hostname, port, databaseName, username, password, config, connectionUser, test, strategy);
            }
            if (adapter==null){
                logger.error("创建 JDBC 适配器失败: {}，类型: {}，错误: {}", adapterClass.getSimpleName(), dbType);
            }
//            logger.info("创建 JDBC 适配器成功: {}，类型: {}", adapterClass.getSimpleName(), dbType);
            return adapter;
        } catch (InvocationTargetException | NoSuchMethodException e) {
            logger.error("创建 JDBC 适配器失败: {}，类型: {}，错误: {}", adapterClass.getSimpleName(), dbType, e.getMessage());
            throw new RuntimeException("无法创建 JDBC 适配器", e);
        }
    }

    /**
     * 创建 NoSQL 适配器实例
     *
     * @param adapterClass   适配器类
     * @param dbType         数据库类型
     * @param hostname       主机名
     * @param port           端口号
     * @param databaseName   数据库名称
     * @param username       用户名
     * @param password       密码
     * @param config         数据源配置
     * @param connectionUser 连接用户
     * @param test           是否为测试模式
     * @return NoSQL 适配器实例
     * @throws IllegalAccessException 如果无法访问适配器类
     * @throws InstantiationException 如果实例化失败
     */
    private static AbstractNoSqlAdapter createNoSqlAdapter(Class<? extends AbstractNoSqlAdapter> adapterClass, DatabaseEnum dbType, String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, boolean test) throws IllegalAccessException, InstantiationException {
        try {
            AbstractNoSqlAdapter adapter;
            if (connectionUser == null) {
                adapter = adapterClass.getConstructor(String.class, Integer.class, String.class, String.class, String.class, JdbcAdapterDataSourceConfig.class, Boolean.class)
                        .newInstance(hostname, port, databaseName, username, password, config, test);
            } else {
                adapter = adapterClass.getConstructor(String.class, Integer.class, String.class, String.class, String.class, JdbcAdapterDataSourceConfig.class, String.class, Boolean.class)
                        .newInstance(hostname, port, databaseName, username, password, config, connectionUser, test);
            }
//            logger.info("创建 NoSQL 适配器成功: {}，类型: {}", adapterClass.getSimpleName(), dbType);
            return adapter;
        } catch (InvocationTargetException | NoSuchMethodException e) {
            logger.error("创建 NoSQL 适配器失败: {}，类型: {}，错误: {}", adapterClass.getSimpleName(), dbType, e.getMessage());
            throw new RuntimeException("无法创建 NoSQL 适配器", e);
        }
    }

}