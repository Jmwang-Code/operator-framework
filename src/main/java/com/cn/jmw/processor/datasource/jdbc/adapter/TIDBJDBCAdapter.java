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
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * TIDBJDBCAdapter类用于适配TiDB数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与TiDB相关的数据库操作。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class TIDBJDBCAdapter extends AbstractJdbcAdapter {

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
    public TIDBJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password,config,connectionUser,test,strategy);
    }

    public TIDBJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    public TIDBJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null,test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    public TIDBJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,Boolean test)  {
//        this(hostname, port, databaseName, username, password, new JdbcAdapterDataSourceConfig(), null,test, new GeneratedConnectionStringStrategy(null));
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
     * 获取TiDB的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return "jdbc:mysql://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true";
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.TIDB;
    }

    /**
     * 获取需忽略的数据库列表。
     *
     * @return 返回一个包含需忽略的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("INFORMATION_SCHEMA", "METRICS_SCHEMA", "mysql", "PERFORMANCE_SCHEMA");
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
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m){
        // 获取 Optional<SampleResult>
        Optional<SampleResult> optionalSampleResult = sqlQueryMontage.getSampleResult();

        // 如果 sampleResult 存在，则设置抽样方式为 "增量"
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleMethod("增量"));

        // 初始化返回结果
        List<String> list = new ArrayList<>();

        // 获取 SQL 构建器
        SqlQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().getFirst();
        if (sqlQueryBuilder == null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 构建原始 SQL
        String sql = sqlQueryBuilder.buildSQL();

        //首先获取总量
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");

        // 如果 sampleResult 存在，则设置抽样时间
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleTime(LocalDateTime.now()));

        try {
            List<Map<String, Object>> countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.getFirst().get("count(1)");

            // 检查参数有效性
            if (n <= 0 || totalCount == 0 || m <= 0) {
                optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(0));
                return list;
            }

            // 如果总数小于抽样数量 m，则返回完整 SQL
            if (totalCount < m) {
                list.add(sql);
                optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(totalCount));
                return list;
            }

            // 设置抽样数量
            optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(m));

            // 构建随机抽样 SQL
            String randomSampling = "LIMIT " + m;
            sql = sql + randomSampling;
            list.add(sql);
        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            throw new RuntimeException("获取总记录数失败", e);
        }

        return list;
    }

    /**
     * 获取数据库元数据信息。
     *
     * @return 数据库实体列表，包含数据库中的表信息
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        // 获取需要忽略的数据库列表
        List<String> ignoreDatabases = getIgnoreDatabaseList();
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password,config,connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet catalogs = metaData.getCatalogs()) {
                while (catalogs.next()) {
                    String databaseName = catalogs.getString("TABLE_CAT");
                    // 如果数据库在忽略列表中，则跳过
                    if (ignoreDatabases.contains(databaseName)) {
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

                            //TODO 查索引
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection,databaseName, tableName);

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            // 获取表注释
                            tableEntity.setTableComment(tables.getString("REMARKS"));
                            Map<String, ColumnEntity> columnEntities = new HashMap<>(16);
                            try (ResultSet cols = metaData.getColumns(databaseName, null, tableName, "%")) {
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    String TYPE_NAME = cols.getString("TYPE_NAME");
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
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT"))?1:0);
                                    // 获取字段注释
                                    columnEntity.setColumnComment(cols.getString("REMARKS"));

                                    // 判断是什么类型的TYPE_NAME
                                    columnEntity.setType(getColumnType(TYPE_NAME));

                                    //TODO 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                                        if ("PRIMARY".equals(indexDetails.get("INDEX_NAME"))) {
                                            // 设置为主键
                                            columnEntity.setIsPrimaryKey(1);
                                        }
                                    }

                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                            }
                            // 设置表的列信息
                            tableEntity.setColumns(columnEntities);
                            tableEntities.put(tableName, tableEntity);
                        }
                    }
                    // 设置数据库中的表信息
                    databaseEntity.setTables(tableEntities);
                    databaseEntities.add(databaseEntity);
                }
            }
        } catch (SQLException e) {
            log.error("获取数据库元数据失败", e);
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
    }

    /**
     * 1字符串 2数字 0时间
     * 判断输入的字符串名称是什么类型的数据字段
     */
    public int getColumnType(String columnType) {
        if ("CHAR".equals(columnType) || "VARCHAR".equals(columnType) ||
                "TEXT".equals(columnType)) {
            return 1;
        } else if ("MEDIUMINT".equals(columnType) || "INT".equals(columnType)
                || "BIGINT".equals(columnType)) {
            return 2;
        } else if ("DATE".equals(columnType) || "TIME".equals(columnType) ||
                "DATETIME".equals(columnType) || "TIMESTAMP".equals(columnType)
                || "YEAR".equals(columnType)) {
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
        Map<String, Map<String, Object>> hashMap = new HashMap<>(16);
        String sql = "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '" + dbName + "' AND TABLE_NAME = '" + tableName + "'";
        try (PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>(16);
                String COLUMN_NAME = resultSet.getString("COLUMN_NAME");
                map.put("TABLE_SCHEMA", resultSet.getString("TABLE_SCHEMA"));
                map.put("TABLE_NAME", resultSet.getString("TABLE_NAME"));
                map.put("INDEX_NAME", resultSet.getString("INDEX_NAME"));
                map.put("SEQ_IN_INDEX", resultSet.getString("SEQ_IN_INDEX"));
                map.put("COLUMN_NAME", COLUMN_NAME);
                map.put("COLLATION", resultSet.getString("COLLATION"));
                map.put("CARDINALITY", resultSet.getString("CARDINALITY"));
                map.put("SUB_PART", resultSet.getString("SUB_PART"));
                map.put("PACKED", resultSet.getString("PACKED"));
                map.put("NULLABLE", resultSet.getString("NULLABLE"));
                map.put("INDEX_TYPE", resultSet.getString("INDEX_TYPE"));
                map.put("COMMENT", resultSet.getString("COMMENT"));
                map.put("INDEX_COMMENT", resultSet.getString("INDEX_COMMENT"));
                hashMap.put(COLUMN_NAME, map);
            }
        } catch (SQLException e) {
            log.error("获取索引信息失败", e);
            throw exception(INDEX_INFO_GET_ERROR);
        }
        return hashMap;
    }
}