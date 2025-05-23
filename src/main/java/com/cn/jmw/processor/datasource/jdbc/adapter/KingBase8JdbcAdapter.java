package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.pojo.SqlQueryMontage;
import com.cn.jmw.pojo.SampleResult;
import com.cn.jmw.processor.datasource.AbstractJdbcAdapter;
import com.cn.jmw.processor.datasource.ConnectionStringStrategy;
import com.cn.jmw.processor.datasource.GeneratedConnectionStringStrategy;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.SqlQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.ColumnEntity;
import com.cn.jmw.processor.datasource.pojo.DatabaseEntity;
import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;
import com.cn.jmw.processor.datasource.pojo.TableEntity;
import com.cn.jmw.processor.datasource.jdbc.inter.ResultSetHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * KingBase8JDBCAdapter类用于适配KingBase8数据库连接。
 * <p>
 * 该类继承自 AbstractJdbcAdapter，提供与 KingBase8 数据库的连接和操作功能，包括伪随机抽样。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class KingBase8JdbcAdapter extends AbstractJdbcAdapter {

    private static final String SCHEMA_SEPARATOR = ".public.";

    /**
     * 构造函数，初始化适配器实例。
     *
     * @param hostname       主机名
     * @param port           端口号
     * @param databaseName   数据库名称
     * @param username       用户名
     * @param password       密码
     * @param config         数据源配置
     * @param connectionUser 连接用户
     * @param test           是否为测试模式
     */
    public KingBase8JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password, config, connectionUser, test,strategy);
    }

    public KingBase8JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    /**
//     * 构造函数，使用默认连接用户。
//     */
//    public KingBase8JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null, test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    /**
//     * 构造函数，使用默认配置。
//     */
//    public KingBase8JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test) {
//        this(hostname, port, databaseName, username, password, new JdbcAdapterDataSourceConfig(), null, test, new GeneratedConnectionStringStrategy(null));
//    }

    /**
     * 获取Aliyun RDS的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return connectionStringStrategy.getConnectionString(hostname, port, databaseName, config);
    }

    /**
     * 获取KingBase8的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return String.format("jdbc:kingbase8://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true",
                hostname, port, databaseName);
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.KINGBASE8;
    }

    /**
     * 获取需要忽略的数据库列表。
     *
     * @return 返回一个包含被忽略的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("kingbase", "security", "system", "sys_hm", "sys_catalog", "sys", "sysaudit", "sysmac", "xlog_record_read");
    }


    /**
     * 添加随机采样
     * <p>
     * KingBase8 支持 LIMIT 实现伪随机抽样。本方法通过计算总数并结合 LIMIT 返回前 m 行数据，不保证随机性。
     * </p>
     *
     * @param sqlQueryMontage SQL 查询的封装对象
     * @param n               抽样的数量
     * @param m               限制的数量
     * @return 增加随机抽样后的 SQL 列表
     */
    @Override
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
        Optional<SampleResult> optionalSampleResult = sqlQueryMontage.getSampleResult();
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleMethod("伪随机"));

        // 初始化返回结果
        List<String> resultList = new ArrayList<>();

        // 获取 SQL 构建器
        SqlQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().getFirst();
        if (sqlQueryBuilder == null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 构建原始 SQL
        String sql = sqlQueryBuilder.buildSQL();

        // 检查 SQL 是否为空
        if (StringUtils.isBlank(sql)) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 构建查询总数的 SQL
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");

        // 如果 sampleResult 存在，则设置抽样时间
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleTime(LocalDateTime.now()));

        try {
            // 执行查询获取总数
            List<Map<String, Object>> countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.getFirst().get("count(1)");

            // 检查参数有效性
            if (n <= 0 || totalCount == 0 || m <= 0) {
                optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(0));
                return resultList;
            }

            // 如果总数小于抽样数量 m，则返回完整 SQL
            if (totalCount < m) {
                resultList.add(sql);
                optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(totalCount));
                return resultList;
            }

            // 设置抽样数量
            optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(m));
            resultList.add(sql + " LIMIT " + m);
        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            throw new RuntimeException("获取总记录数失败", e);
        }

        return resultList;
    }

    /**
     * 执行批量查询操作。
     *
     * @param sql    要执行的 SQL 查询语句
     * @param params 查询参数数组，可为 null
     * @return 查询结果列表，每个元素为列名到值的映射
     * @throws SQLException 如果数据库访问出错
     */
    @Override
    public List<Map<String, Object>> executeDMLC(String sql, Object[] params) throws SQLException {
        String normalizedSql = normalizeSql(sql);
        try (Connection connection = pool.getConnection(generateConnectionKey(), config, connectionUser)) {
            return runner.query(connection, normalizedSql, new MapListHandler(), params);
        } catch (SQLException e) {
            log.error("执行查询失败，SQL: {}，错误: {}", normalizedSql, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 执行批量更新、插入或删除操作。
     *
     * @param sql    要执行的 SQL 语句
     * @param params 查询参数数组，可为 null
     * @return 受影响的行数
     * @throws SQLException 如果数据库访问出错
     */
    @Override
    public int executeDMLRUD(String sql, Object[] params) throws SQLException {
        String normalizedSql = normalizeSql(sql);
        try (Connection connection = pool.getConnection(generateConnectionKey(), config, connectionUser)) {
            return runner.update(connection, normalizedSql, params);
        } catch (SQLException e) {
            log.error("执行更新失败，SQL: {}，错误: {}", normalizedSql, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 执行 DDL 操作（如创建表、修改表结构）。
     *
     * @param sql    要执行的 SQL 语句
     * @param params 查询参数数组，可为 null
     * @return 操作是否成功
     * @throws SQLException 如果数据库访问出错
     */
    @Override
    public boolean executeDDL(String sql, Object[] params) throws SQLException {
        String normalizedSql = normalizeSql(sql);
        try (Connection connection = pool.getConnection(generateConnectionKey(), config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(normalizedSql)) {
            // Set parameters if any
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            return statement.execute();
        } catch (SQLException e) {
            log.error("执行 DDL 失败，SQL: {}，错误: {}", normalizedSql, e.getMessage(), e);
            throw new SQLException("执行 DDL 操作失败: " + e.getMessage(), e);
        }
    }

    /**
     * 使用流式查询执行大结果集操作。
     *
     * @param sql     要执行的 SQL 查询语句
     * @param handler 处理结果集的处理器
     * @param size    每批处理的行数（未使用，保留参数兼容性）
     * @param <T>     返回类型，由 handler 定义
     * @return 查询结果，由 ResultSetHandler 处理
     * @throws SQLException 如果数据库访问出错
     */
    @Override
    public <T> T queryStream(String sql, ResultSetHandler<T> handler, Integer size) throws SQLException {
        String normalizedSql = normalizeSql(sql);
        try (Connection connection = pool.getConnection(generateConnectionKey(), config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(normalizedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            statement.setFetchSize(Integer.MIN_VALUE); // 启用流式查询
            try (ResultSet resultSet = statement.executeQuery()) {
                return handler.handle(resultSet);
            }
        } catch (SQLException e) {
            log.error("流式查询失败，SQL: {}，错误: {}", normalizedSql, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    /**
     * 获取指定数据库的元数据信息。
     * <p>
     * 该方法通过 JDBC DatabaseMetaData 获取 schema、表和列信息，并优化内存使用。
     * </p>
     *
     * @param dbName 可选的数据库名称，若为空则返回所有非忽略 schema
     * @return 数据库实体列表，包含表和列信息
     * @throws RuntimeException 如果元数据获取失败
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        // 获取需要忽略的数据库列表
        Set<String> ignoreDatabases = new HashSet<>(getIgnoreDatabaseList()); // 转换为 Set，提升查找效率

        try (Connection connection = pool.getConnection(generateConnectionKey(), config, connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();

            // PostgreSQL 使用 getSchemas() 来获取 Schema 列表
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");

                    if (ignoreDatabases.contains(schemaName) || (dbName != null && !dbName.equals(schemaName))) {
                        continue;
                    }

                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(schemaName);
                    databaseEntity.setDatabaseEnum(getDatabaseType());

                    Map<String, TableEntity> tableEntities = new HashMap<>(16);

                    // 通过 Schema 获取表信息
                    try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");

                            //TODO 查索引
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection, schemaName, tableName);

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            // 获取表注释
                            tableEntity.setTableComment(tables.getString("REMARKS"));

                            Map<String, ColumnEntity> columnEntities = new HashMap<>(16);
                            // 获取表的列信息
                            try (ResultSet cols = metaData.getColumns(null, schemaName, tableName, "%")) {
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    String TYPE_NAME = cols.getString("TYPE_NAME");
                                    // 列名
                                    columnEntity.setColumnName(cols.getString("COLUMN_NAME"));
                                    // 列类型
                                    columnEntity.setColumnType(TYPE_NAME);
                                    // 列大小
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE"));
                                    // 是否可为空
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
                                    // 默认值
                                    columnEntity.setDefaultValue(cols.getString("COLUMN_DEF"));
                                    // 是否自增
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0);

                                    // 判断是什么类型的TYPE_NAME
                                    columnEntity.setType(getColumnType(TYPE_NAME));

                                    // 获取字段注释
                                    columnEntity.setColumnComment(cols.getString("REMARKS"));

                                    //TODO 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                                        if ("t".equals(indexDetails.get("IS_PRIMARY"))) {
                                            // 设置为主键
                                            columnEntity.setIsPrimaryKey(1);
                                        }
                                    }

                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                            }
                            tableEntity.setColumns(columnEntities);
                            tableEntities.put(tableName, tableEntity);
                        }
                    }
                    databaseEntity.setTables(tableEntities);
                    databaseEntities.add(databaseEntity);
                }
            }
        } catch (SQLException e) {
            log.error("获取数据库元数据失败: {}", e.getMessage());
            throw new RuntimeException("Unable to retrieve database metadata", e);
        }

        return databaseEntities;
    }

    /**
     * 判断列的数据类型。
     *
     * @param columnType 列类型名称
     * @return 类型代码：1-字符串，2-数字，0-时间，3-其他
     */
    public int getColumnType(String columnType) {
        return switch (columnType.toLowerCase()) {
            case "char", "varchar", "text" -> 1; // 字符串类型
            case "int8", "int4" -> 2; // 数字类型
            case "timetz", "time", "timestamptz", "timestamp", "date" -> 0; // 时间类型
            default -> 3; // 其他类型
        };
    }

    /**
     * 获取表的索引信息。
     *
     * @param connection 数据库连接
     * @param dbName     数据库名称（schema）
     * @param tableName  表名
     * @return 索引信息映射，键为列名，值为索引详情
     * @throws RuntimeException 如果索引查询失败
     */
    @Override
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection, String dbName, String tableName) {
        Map<String, Map<String, Object>> indexMap = new HashMap<>(16);
        String sql = """
                SELECT DISTINCT
                    nsp.nspname AS schema_name,
                    t.relname AS table_name,
                    i.relname AS index_name,
                    a.attname AS column_name,
                    ix.indisunique AS is_unique,
                    ix.indisprimary AS is_primary
                FROM
                    pg_class t
                    JOIN pg_index ix ON t.oid = ix.indrelid
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                    JOIN pg_namespace nsp ON nsp.oid = t.relnamespace
                WHERE
                    nsp.nspname = ?
                    AND t.relkind = 'r'
                    AND t.relname = ?""";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, dbName);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("column_name");
                    Map<String, Object> map = new HashMap<>(6); // 预估字段数
                    map.put("TABLE_SCHEMA", resultSet.getString("schema_name"));
                    map.put("TABLE_NAME", resultSet.getString("table_name"));
                    map.put("INDEX_NAME", resultSet.getString("index_name"));
                    map.put("IN_UNIQUE", resultSet.getString("is_unique"));
                    map.put("IS_PRIMARY", resultSet.getString("is_primary"));
                    map.put("COLUMN_NAME", columnName);
                    indexMap.put(columnName, map);
                }
            }
        } catch (SQLException e) {
            log.error("获取索引信息失败，表: {}.{}，错误: {}", dbName, tableName, e.getMessage(), e);
            throw new RuntimeException("无法获取索引信息", e);
        }
        return indexMap;
    }

    /**
     * 规范化 SQL，移除反引号并调整 schema 分隔符。
     *
     * @param sql 原始 SQL
     * @return 规范化后的 SQL
     */
    private String normalizeSql(String sql) {
        return sql.replace("`", "").replace(".", SCHEMA_SEPARATOR);
    }

    /**
     * 生成连接池的唯一键。
     *
     * @return 连接标识符
     */
    private String generateConnectionKey() {
        return hostname + port + databaseName + username + password;
    }

}