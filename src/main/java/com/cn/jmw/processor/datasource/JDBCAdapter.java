package com.cn.jmw.processor.datasource;

import com.cn.jmw.pojo.SqlQueryMontage;
import com.cn.jmw.processor.datasource.factory.DataSourceConnectionPool;
import com.cn.jmw.processor.datasource.pojo.*;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.dbutils.QueryRunner;
import com.cn.jmw.processor.datasource.jdbc.inter.ResultSetHandler;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.*;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;


/**
 * JDBCAdapter是一个抽象类，用于定义与关系型数据库的适配器。
 * 该类扩展了Database类，并实现了SQLDatabaseQuery接口。
 * <p>
 * 该类提供构造函数以初始化与数据库的连接参数，如主机名、端口、数据库名、用户名和密码。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public abstract class AbstractJdbcAdapter extends AbstractJdbcDataSourceConfig implements
        // DQL
        SqlDatabaseQuery,
        // DDL
        SqlCommand,
        // 公共系统命令语言 SCL
        SystemCommandLanguage {

    /**
     * 用于执行SQL查询的QueryRunner实例
     */
    public final QueryRunner runner;
    /**
     * 数据源连接池实例
     */
    public static DataSourceConnectionPool pool = DataSourceConnectionPool.getInstance();
    /**
     * 连接字符出
     */
    public final ConnectionStringStrategy connectionStringStrategy;

    /**
     * 构造函数用于创建JDBCAdapter实例。
     *
     * @param hostname     数据库主机名
     * @param port         数据库端口
     * @param databaseName 数据库名称
     * @param username     数据库用户名
     * @param password     数据库密码
     */
    public AbstractJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, StringUtils.isBlank(databaseName) ? "" : databaseName, username, password);
        super.connectionUser = connectionUser;
        super.config = config;
        this.connectionStringStrategy = strategy;
        String dataSourceId = hostname + port + (StringUtils.isBlank(databaseName) ? "" : databaseName) + username + password;
        DataSource dataSource = test ? null : initializeDataSource(dataSourceId);
//        DataSource dataSource = null;
//        if (!test) {
//            DataSourceConnectionPool pool = DataSourceConnectionPool.getInstance();
//            dataSource = pool.getDataSource(dataSourceId);
//            if (dataSource == null) {
//                synchronized (this) {
//                    dataSource = pool.createDataSource(dataSourceId, getConnectionString(), username, password, config);
//                }
//            }
//        }

        // 初始化 QueryRunner
        this.runner = new QueryRunner(dataSource);
    }

    public AbstractJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, new JdbcAdapterDataSourceConfig(), null, false, strategy);
    }

    private DataSource initializeDataSource(String dataSourceId) {
        DataSource dataSource = pool.getDataSource(dataSourceId);
        if (dataSource == null) {
            synchronized (this) {
                dataSource = pool.getDataSource(dataSourceId);
                if (dataSource == null) {
                    String connectionString = connectionStringStrategy.getConnectionString(hostname, port, databaseName, config);
                    dataSource = pool.createDataSource(dataSourceId, connectionString, username, password, config);
                }
            }
        }
        return dataSource;
    }

    @Override
    public String getConnectionString() {
        return connectionStringStrategy.getConnectionString(hostname, port, databaseName, config);
    }

    protected abstract String generateConnectionString();

    @Override
    public String getValidationQuery() {
        return "SELECT 1";
    }

    /**
     * 测试与数据库的连接是否正常。
     *
     * @return 如果连接有效，返回true，否则返回false
     */
    @Override
    public boolean testConnection() {
        // Step 1: 检查 IP 是否可达
        if (!isIpReachable(hostname)) {
            log.info("IP {} 不可达，无需尝试数据库连接", hostname);
            return false;
        }

        // Step 2: 检查端口是否开放
        if (!isPortOpen(hostname, port)) {
            log.info("IP {} 的端口 {} 未开放，无需尝试数据库连接", hostname, port);
            return false;
        }

        //临时连接测试
        try (Connection connection = DriverManager.getConnection(getConnectionString(), username, password)) {
            // 检查连接是否有效
            return connection != null && !connection.isClosed();
        } catch (SQLException e) {
            // 抛出无法连接数据库的异常
            throw new RuntimeException(e);
        }
    }

    /**
     * 检查 IP 是否可达（类似 ping）
     */
    private boolean isIpReachable(String ip) {
        try {
            InetAddress address = InetAddress.getByName(ip);
            // 设置超时为 2 秒
            return address.isReachable(2000); // 单位：毫秒
        } catch (Exception e) {
            log.warn("检查 IP {} 可达性失败: {}", ip, e.getMessage());
            return false;
        }
    }

    /**
     * 检查指定 IP 和端口是否开放
     */
    private boolean isPortOpen(String ip, int port) {
        try (Socket socket = new Socket()) {
            // 设置连接超时为 2 秒
            socket.connect(new java.net.InetSocketAddress(ip, port), 2000);
            return true;
        } catch (SocketTimeoutException e) {
            log.warn("连接 IP {} 端口 {} 超时", ip, port);
            return false;
        } catch (Exception e) {
            log.warn("IP {} 端口 {} 未开放: {}", ip, port, e.getMessage());
            return false;
        }
    }

    @Override
    public String getUsername() {
        // 返回数据库用户名
        return this.username;
    }

    @Override
    public String getPassword() {
        // 返回数据库密码
        return this.password;
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    /**
     * 获取数据库元数据信息。
     *
     * @return 数据库实体列表，包含数据库中的表信息
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        // 获取需要忽略的数据库列表
        List<String> ignoreDatabases = getIgnoreDatabaseList();
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet catalogs = metaData.getCatalogs()) {
                while (catalogs.next()) {
                    String databaseName = catalogs.getString("TABLE_CAT");
                    // 如果数据库在忽略列表中，则跳过
                    if (ignoreDatabases.contains(databaseName)) {
                        continue;
                    }
                    if (StringUtils.isNotBlank(dbName) && !dbName.equals(databaseName)) {
                        continue;
                    }
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(databaseName);
                    // 存入DatabaseType
                    databaseEntity.setDatabaseEnum(getDatabaseType());
                    Map<String, TableEntity> tableEntities = new HashMap<>(16);
                    try (ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            // 获取表注释
                            tableEntity.setTableComment(tables.getString("REMARKS"));
                            Map<String, ColumnEntity> columnEntities = new HashMap<>(16);
                            try (ResultSet cols = metaData.getColumns(databaseName, null, tableName, "%")) {
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    // 获取列名
                                    columnEntity.setColumnName(cols.getString("COLUMN_NAME"));
                                    // 获取列类型
                                    columnEntity.setColumnType(cols.getString("TYPE_NAME"));
                                    // 获取列大小
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE"));
                                    // 获取可否为NULL
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
                                    // 获取默认值
                                    columnEntity.setDefaultValue(cols.getString("COLUMN_DEF"));
                                    // 获取自增状态
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0);
                                    // 获取列注释
                                    columnEntity.setColumnComment(cols.getString("REMARKS"));

                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                            } catch (SQLException e) {
                                log.info("获取列信息失败: {}", e.getMessage());
                                // 抛出无法检索数据库元数据的异常
//                        throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
                                continue;
                            }
                            // 设置表的列信息
                            tableEntity.setColumns(columnEntities);
                            tableEntities.put(tableName, tableEntity);
                        }
                    } catch (SQLException e) {
                        log.info("获取列信息失败: {}", e.getMessage());
                        // 抛出无法检索数据库元数据的异常
//                        throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
                        continue;
                    }
                    // 设置数据库中的表信息
                    databaseEntity.setTables(tableEntities);
                    databaseEntities.add(databaseEntity);
                }
            } catch (SQLException e) {
                log.info("获取目录失败: {}", e.getMessage());
                // 抛出无法检索数据库元数据的异常
                throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
            }
        } catch (SQLException e) {
            log.info("建立或者获取连接失败: {}", e.getMessage());
            // 抛出无法检索数据库元数据的异常
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
    }

    /**
     * 获取数据库版本信息。
     *
     * @return 数据库版本字符串
     */
    @Override
    public String getDatabaseVersion() {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();
            // 返回数据库版本
            return metaData.getDatabaseProductVersion();
        } catch (Exception e) {
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_VERSION);
        }
    }

    /**
     * 执行批量查询操作，尚未实现。
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public List<Map<String, Object>> executeDMLC(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
            return this.runner.query(connection, sql, new MapListHandler(), params);
        } catch (SQLException e) {
            throw new SQLException("Failed to execute query: " + e.getMessage(), e);
        }
    }

    /**
     * 当存在大批量executeDMLC函数要调用，最好在外部定义一个Connection 公用它
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public List<Map<String, Object>> executeDMLC(Connection connection, String sql, Object[] params) throws SQLException {
        return this.runner.query(connection, sql, new MapListHandler(), params);
//        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    /**
     * 执行批量查询操作，尚未实现。
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @return boolean
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public int executeDMLRUD(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
            return this.runner.update(connection, sql, params);
        } catch (SQLException e) {
            throw new SQLException("Failed to execute update: " + e.getMessage(), e);
        }
    }

    /**
     * 执行批量查询操作，尚未实现。
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @return boolean
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public boolean executeDDL(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            // Set parameters if any
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            return statement.execute();
        } catch (SQLException e) {
            throw new SQLException("Error executing DDL: " + e.getMessage(), e);
        }
//        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
    }

    /**
     * 获取索引
     *
     * @param dbName    数据库
     * @param tableName 数据表
     * @return 索引
     */
    @Override
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection, String dbName, String tableName) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    /**
     * 获取主键
     *
     * @param dbName    数据库
     * @param tableName 数据表
     * @return 主键
     */
    @Override
    public List<Map<String, Object>> getPrimaryKeyInfo(String dbName, String tableName) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    /**
     * 使用流执行查询操作。
     *
     * @param sql     要执行的SQL查询语句
     * @param handler 处理结果集的处理器
     * @param <T>     返回类型
     * @return 查询结果，由ResultSetHandler处理
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public <T> T queryStream(String sql, ResultSetHandler<T> handler, Integer size) throws SQLException {
        T results = null;
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            // 这是MySQL流式查询的重要设置
            statement.setFetchSize(size);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    // 处理结果集
                    // 更新结果
                    results = handler.handle(resultSet);
                }
            }
        }
        return results;
    }

    /**
     * 获取需要忽略的数据库列表。
     *
     * @return 需要忽略的数据库名称列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return List.of();
    }

    /**
     * 获取数据库类型，尚未实现。
     *
     * @return 数据库类型枚举
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    /**
     * 获取创建表的DDL语句，尚未实现。
     */
    @Override
    public ShowCreateTable getCreateTableDDL(String dbName, String table) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }


    /**
     * 添加随机采样
     * <h1>不允许出现 ORDER BY、LIMIT等字眼</h1>
     *
     * @param sqlQueryMontage SQL 查询的封装对象
     * @param n               抽样的数量
     * @param m               限制的数量
     * @return 增加随机抽样后的SQL列表
     */
    @Override
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
        // 抛出未实现方法的异常
        throw exception(NOT_IMPLEMENTED_METHOD);
    }

    /**
     * 获取表的结构信息
     *
     * @param dbName 表名
     * @return 显示表格状态结果
     */
    @Override
    public Map<String, ShowTableStatusResult> showTableStatus(String dbName) {
        Map<String, ShowTableStatusResult> showTableStatusResults = new HashMap<>(16);
        StringBuilder sql = new StringBuilder("SELECT\n" +
                "\tTABLE_NAME,\n" +
                "\tSUM( DATA_LENGTH ) AS data_length \n" +
                "FROM\n" +
                "\tinformation_schema.PARTITIONS\n");

        if (StringUtils.isNotBlank(dbName)) {
            sql.append("WHERE\n" +
                    "  TABLE_SCHEMA = ? \n");
        }

        sql.append("GROUP BY\n" +
                "\tTABLE_NAME");

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            if (StringUtils.isNotBlank(dbName)) {
                statement.setString(1, dbName);
            }
            statement.setFetchSize(Integer.MIN_VALUE);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ShowTableStatusResult result = ShowTableStatusResult.builder()
                            .name(resultSet.getString("TABLE_NAME"))
                            .dataLength(resultSet.getLong("data_length"))
                            .build();

                    showTableStatusResults.put(result.getName(), result);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return showTableStatusResults;
    }

    @Override
    public Map<String, ShowTableStatusResult> showWideTableStatus(String tableName, String indexName) {
        Map<String, ShowTableStatusResult> showTableStatusResults = new HashMap<>(16);
        StringBuilder sql = new StringBuilder(
                "SELECT\n" +
                        "    REGEXP_EXTRACT(PARTITION_DESCRIPTION, '\"([^\"]*)\"', 1) AS partition_value,\n" +
                        "\t\tSUM(DATA_LENGTH) AS data_length\n" +
                        "FROM\n" +
                        "    information_schema.partitions\n" +
                        "WHERE\n" +
                        "    table_name = '" + tableName + "'\n" +
                        "GROUP BY\n" +
                        "     partition_value\n"
        );

        if (StringUtils.isNotBlank(indexName)) {
            sql.append("HAVING\n" +
                    "\t\tpartition_value = '" + indexName + "'");
        }

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            statement.setFetchSize(Integer.MIN_VALUE);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ShowTableStatusResult result = ShowTableStatusResult.builder()
                            .name(resultSet.getString("partition_value"))
                            .dataLength(resultSet.getLong("data_length"))
                            .build();

                    showTableStatusResults.put(result.getName(), result);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return showTableStatusResults;
    }

    @Override
    public boolean dataTableThresholdDeterminationCleaning(List<String> tableNames, List<Double> limitSizes, List<String> sortTimeFields, double threshold) {
        //showTableStatus,获取到所有表的信息关于，curSizes当前大小 rowsCounts行数
        Map<String, ShowTableStatusResult> stringShowTableStatusResultMap = showTableStatus(null);

        for (int i = 0; i < tableNames.size(); i++) {
            //表名
            String tableName = tableNames.get(i);
            //限制GB
            Double limitSize = limitSizes.get(i);
            //阈值之下
            double allowSize = limitSize * threshold;
            //排序时间字段
            String sortTimeField = sortTimeFields.get(i);

            //curSizes当前大小 rowsCounts行数
            ShowTableStatusResult showTableStatusResult = stringShowTableStatusResultMap.get(tableName);
            if (showTableStatusResult == null) {
                log.info("数据表阈值清理————表名: {}, 失效表", tableName);
                continue;
            }
            //curSizes当前大小
            double curSizes = showTableStatusResult.getDataLength();
            //curSizes当前大小要做计算从b换算成GB
            curSizes = curSizes / 1024 / 1024 / 1024;
            //avgRowSize平均数据大小
            Long avgRowSize = showTableStatusResult.getAvgRowLength();

            //阈值之下就通过
            if (curSizes < allowSize) {
                //日志打印
                log.info("数据表阈值清理————表名: {}, 当前大小: {}GB, 允许大小: {}GB", tableName, curSizes, allowSize);
                continue;
            }

            int earliestAndLatestDays = getEarliestAndLatestDays(sortTimeField, tableName);

            /*
              SELECT *
              FROM bds_asset_info
              WHERE DATE(create_time) = DATE_ADD((SELECT DATE(MIN(create_time)) FROM bds_asset_info), INTERVAL 1 DAY)
              ORDER BY create_time ASC;
             */
            String sqlQuery = "DELETE FROM %s \n" +
                    "WHERE DATE(%s) BETWEEN DATE_ADD((SELECT DATE(MIN(%s)) FROM %s), INTERVAL 0 DAY) \n" +
                    "AND DATE_ADD((SELECT DATE(MIN(%s)) FROM %s), INTERVAL %d DAY);";

            String sqlCount = "SELECT COUNT(*) AS COUNT \n" +
                    "FROM %s \n" +
                    "WHERE DATE(%s) = DATE_ADD((SELECT DATE(MIN(%s)) FROM %s), INTERVAL %d DAY);";

            int n = 0;
            double newCurSizes = curSizes;
            while (newCurSizes > allowSize && n <= earliestAndLatestDays) {
                sqlCount = String.format(sqlCount, tableName, sortTimeField, sortTimeField, tableName, n);

                try {
                    List<Map<String, Object>> countResult = executeDMLC(sqlCount, null);
                    double count = countResult.isEmpty() ? 0 : (Long) countResult.getFirst().get("COUNT");
                    newCurSizes = newCurSizes - ((count * avgRowSize) / 1024 / 1024 / 1024);

                    if (newCurSizes < allowSize) {
                        log.info("数据表阈值清理————正在处理表: {}, 当前天数: {}, 计数: {}条, 新大小: {}GB", tableName, n, count, newCurSizes);
                        //跳出去之前需要delete
                        sqlQuery = String.format(sqlQuery, tableName, sortTimeField, sortTimeField, tableName, sortTimeField, tableName, n);
                        int numberOfSuccessfulDMLExecutions = executeDMLRUD(sqlQuery, null);
                        log.info("数据表阈值清理————表名: {}, 删除操作已执行, 受影响行数: {}行", tableName, numberOfSuccessfulDMLExecutions);
                        break;
                    }
                } catch (SQLException e) {
                    log.error("数据表阈值清理————处理表: {}, 当前天数: {}, 异常信息: {}", tableName, n, e.getMessage());
                    log.info("————————————————————————————————————————————————————————————————————————————————————————————————————————");
                    throw new RuntimeException(e);
                }
                n++;
            }
        }
        return true;
    }

    /**
     * 计算当前数据库中最早和最迟进入库中的数据天数差
     *
     * @param tableName     表名称
     * @param sortTimeField 排序字段
     * @return 最早和最迟进入库中的数据天数差
     */
    @Override
    public int getEarliestAndLatestDays(String tableName, String sortTimeField) {
        String dayDiffQuery = "SELECT DATEDIFF(MAX(" + sortTimeField + "), MIN(" + sortTimeField + ")) AS dayDiff FROM " + tableName;

        int maxDays = 0;
        try {
            List<Map<String, Object>> result = executeDMLC(dayDiffQuery, null);
            if (!result.isEmpty()) {
                maxDays = (Integer) result.getFirst().get("dayDiff");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return maxDays;
    }

    /**
     * 根据N个抽样点获取前后M条数据
     *
     * @param sqlQueryBuilder 前提是简单SQL不含嵌套
     * @param n               抽样点个数
     * @param m               每个抽样点前后各获取的记录数
     * @return 抽样数据
     */
    @Override
    public List<String> n_point_sampling_method(SqlQueryMontage sqlQueryBuilder, int n, int m) {
        //sql替换FROM之前的变成SELECT COUNT(1)
        String sql = sqlQueryBuilder.getSqlQueryBuilders().getFirst().buildSQL();
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");
        List<String> sampledData = new ArrayList<>();
        try {
            // 获取表的总记录数
            List<Map<String, Object>> countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.getFirst().get("count(1)");

            // 验证抽样点和前后记录分布一定要小于表的总记录数
            if (n <= 0 || totalCount == 0 || m <= 0) {
                // 无效参数直接返回
                return sampledData;
            }

            if (totalCount < (long) n * 2 * m) {
                throw exception(RANDOM_SAMPLING_EXCEED_RECORDS);
            }

            // 计算均匀分布的抽样点
            // 确保抽样点分布均匀
            long interval = totalCount / (n);

            // 确定每个抽样点的前后范围，进行查询并去重

            for (int i = 1; i <= n; i++) {
                // 计算当前的抽样点
                long samplePoint = i * interval;

                // 确保抽样点的前后范围不会超过表的边界
                Long start = Math.max(samplePoint - m, 0);
                Long end = Math.min(samplePoint + m, totalCount - 1);

                // 计算返回记录的数量
                // 计算 LIMIT 的数量
                long limitCount = end >= start ? (end - start + 1) : 0;

                // 查询指定范围内的数据
                String limitSql = sql + " LIMIT " + start + ", " + limitCount;
                sampledData.add(limitSql);

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return sampledData;
    }
}