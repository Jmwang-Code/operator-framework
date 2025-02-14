package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.pojo.SQLQueryMontage;
import com.cn.jmw.pojo.SampleResult;
import com.cn.jmw.processor.datasource.JDBCAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.ColumnEntity;
import com.cn.jmw.processor.datasource.pojo.DatabaseEntity;
import com.cn.jmw.processor.datasource.pojo.JDBCAdapterDataSourceConfig;
import com.cn.jmw.processor.datasource.pojo.TableEntity;
import com.cn.jmw.processor.datasource.jdbc.inter.ResultSetHandler;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * KingBase8JDBCAdapter类用于适配KingBase8数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与KingBase8相关的数据库操作。
 * </p>
 */
public class KingBase8JDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建KingBase8JDBCAdapter实例。
     *
     * @param hostname     KingBase8服务器的主机名
     * @param port         KingBase8服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public KingBase8JDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public KingBase8JDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public KingBase8JDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取KingBase8的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:kingbase8://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true";
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
        return Arrays.asList("kingbase", "security","system","sys_hm","sys_catalog","sys","sysaudit","sysmac","xlog_record_read");
    }


    /**
     * 添加随机采样
     * <h1>不允许出现 ORDER BY、 LIMIT等字眼</h1>
     *
     * @param sqlQueryMontage
     * @return 增加随机抽样后的SQL
     */
    @Override
    public List<String> addRandomSampling(SQLQueryMontage sqlQueryMontage, int N, int M){
        //抽样样本结果
        SampleResult sampleResult = sqlQueryMontage.getSampleResult();
        sampleResult.setSample_result("增量");
        List<String> list = new ArrayList<>();
        SQLQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().get(0);
        if (sqlQueryBuilder==null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        String sql = sqlQueryBuilder.buildSQL().replaceAll("`","").replaceAll("[()]","");
        sql = simplifySql(sql);
        //TODO删除第一个FROM后面的 .之前的单词
        if (StringUtils.isBlank(sql)){
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        Matcher matcher = RandomSamplingCompile.matcher(sql);
        if (matcher.find()){
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }
        //首先获取总量
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");
        sampleResult.setSample_time(LocalDateTime.now());
        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.get(0).get("count");
            // 验证抽样点和前后记录分布一定要小于表的总记录数
            if (N <= 0 || totalCount == 0 || M <= 0) {
                sampleResult.setSample_count(0);
                return list; // 无效参数直接返回
            }

            if (totalCount < M) {
                list.add(sql);
                sampleResult.setSample_count(totalCount);
                return list;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        sampleResult.setSample_count(M);
        String randomSampling = "LIMIT " + M;
        sql = sql + randomSampling;
        list.add(sql);
        return list;
    }

    public String simplifySql(String sql) {
        // 使用正则表达式去掉表名
        return sql.replaceAll("(?<=\\s|^)\\w+\\.([\\w\\d]+)", "$1");
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
        sql = sql.replaceAll("`", "").replace(".", ".public.");
        try (Connection connection = pool.getConnection(hostname + port + databaseName,config,connectionUser)){
            return this.runner.query(connection, sql, new MapListHandler(), params);
        }
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
        sql = sql.replaceAll("`", "").replace("`.`", ".public.");
        try (Connection connection = pool.getConnection(hostname + port + databaseName,config,connectionUser)){
            return this.runner.update(connection, sql, params);
        }
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
    public void executeDDL(String sql, Object[] params) throws SQLException {
        sql = sql.replaceAll("`", "").replace("`.`", ".public.");
        try (Connection connection = pool.getConnection(hostname + port + databaseName,config,connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            // Set parameters if any
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            statement.execute();
        } catch (SQLException e) {
            throw new SQLException("Error executing DDL: " + e.getMessage(), e);
        }
//        throw exception(NOT_IMPLEMENTED_METHOD); // 抛出未实现方法的异常
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
        sql = sql.replaceAll("`", "").replace("`.`", ".public.");
        T results = null;
        try (Connection connection = pool.getConnection(hostname + port + databaseName,config,connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            statement.setFetchSize(Integer.MIN_VALUE); // 这是MySQL流式查询的重要设置
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    T result = handler.handle(resultSet); // 处理结果集
                    results = result; // 更新结果
                }
            }
        }
        return results; // 返回最终结果
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    /**
     * 获取数据库元数据信息。
     *
     * @return 数据库实体列表，包含数据库中的表信息
     * @throws SQLException 如果无法检索数据库元数据，将抛出异常
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        List<String> ignoreDatabases = getIgnoreDatabaseList(); // 获取需要忽略的数据库列表

        try (Connection connection = pool.getConnection(hostname + port + databaseName,config,connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();

            // PostgreSQL 使用 getSchemas() 来获取 Schema 列表
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");

                    // 如果 Schema 在忽略列表中，则跳过
                    if (ignoreDatabases.contains(schemaName)) {
                        continue;
                    }
                    if (StringUtils.isNotBlank(dbName) && !dbName.equals(schemaName)) {
                        continue;
                    }

                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(schemaName);
                    databaseEntity.setDatabaseEnum(getDatabaseType());

                    Map<String, TableEntity> tableEntities = new HashMap<>();

                    // 通过 Schema 获取表信息
                    try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");

                            //TODO 查索引
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection,schemaName, tableName);

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            tableEntity.setTableComment(tables.getString("REMARKS")); // 获取表注释

                            Map<String, ColumnEntity> columnEntities = new HashMap<>();

                            // 获取表的列信息
                            try (ResultSet cols = metaData.getColumns(null, schemaName, tableName, "%")) {
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    String TYPE_NAME = cols.getString("TYPE_NAME");
                                    columnEntity.setColumnName(cols.getString("COLUMN_NAME")); // 列名
                                    columnEntity.setColumnType(TYPE_NAME);   // 列类型
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE"));    // 列大小
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable); // 是否可为空
                                    columnEntity.setDefaultValue(cols.getString("COLUMN_DEF")); // 默认值
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0); // 是否自增

                                    // 判断是什么类型的TYPE_NAME
                                    columnEntity.setType(getColumnType(TYPE_NAME));

                                    // 获取字段注释
                                    columnEntity.setColumnComment(cols.getString("REMARKS"));

                                    //TODO 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                                        if ("t".equals(indexDetails.get("IS_PRIMARY"))) {
                                            columnEntity.setIsPrimaryKey(1); // 设置为主键
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
            e.printStackTrace();
            throw new RuntimeException("Unable to retrieve database metadata", e);
        }

        return databaseEntities;
    }

    /**
     * 1字符串 2数字 0时间
     *
     * 判断输入的字符串名称是什么类型的数据字段
     */
    public int getColumnType(String columnType) {
        if ("char".equals(columnType) || "varchar".equals(columnType) ||
                "text".equals(columnType)) {
            return 1;
        } else if ("int8".equals(columnType)
                || "int4".equals(columnType)) {
            return 2;
        } else if ("timetz".equals(columnType) || "time".equals(columnType) ||
                "timestamptz".equals(columnType) || "timestamp".equals(columnType)
                || "date".equals(columnType)) {
            return 0;
        }

        return 3;
    }

    /**
     * 获取索引
     *
     * @param dbName    数据库
     * @param tableName 数据表
     * @return 索引
     */
    @Override
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection,String dbName, String tableName) {
        Map<String, Map<String, Object>> hashMap = new HashMap<>();
        String sql = "SELECT DISTINCT\n" +
                "    nsp.nspname AS schema_name,  \n" +
                "    t.relname AS table_name,  \n" +
                "    i.relname AS index_name,  \n" +
                "    a.attname AS column_name,  \n" +
                "    ix.indisunique AS is_unique,  \n" +
                "    ix.indisprimary AS is_primary  \n" +
                "FROM  \n" +
                "    pg_class t  \n" +
                "JOIN pg_index ix ON t.oid = ix.indrelid  \n" +
                "JOIN pg_class i ON i.oid = ix.indexrelid  \n" +
                "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)  \n" +
                "JOIN pg_namespace nsp ON nsp.oid = t.relnamespace  \n" +
                "WHERE  \n" +
                "    nsp.nspname = ?  \n" +
                "    AND t.relkind = 'r'  \n" +
                "    AND t.relname = ?;  \n";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, dbName);
            statement.setString(2, tableName);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                String columnName = resultSet.getString("column_name");
                map.put("TABLE_SCHEMA", resultSet.getString("schema_name"));
                map.put("TABLE_NAME", resultSet.getString("table_name"));
                map.put("INDEX_NAME", resultSet.getString("index_name"));
                map.put("IN_UNIQUE", resultSet.getString("is_unique"));
                map.put("IS_PRIMARY", resultSet.getString("is_primary"));
                map.put("COLUMN_NAME", columnName);
                hashMap.put(columnName, map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to retrieve index info", e);
        }
        return hashMap;
    }

}