package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.pojo.SQLQueryMontage;
import com.cn.jmw.pojo.SampleResult;
import com.cn.jmw.processor.datasource.JDBCAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.*;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * MySQLJDBCAdapter类用于适配MySQL数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与MySQL相关的数据库操作。
 * </p>
 */
public class MySQLJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建MySQLJDBCAdapter实例。
     *
     * @param hostname     MySQL服务器的主机名
     * @param port         MySQL服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public MySQLJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public MySQLJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public MySQLJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password) {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取MySQL的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:mysql://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?allowMultiQueries=true&useUnicode=true&useSSL=false&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true";
    }

    /**
     * 使用批量查询数据。
     *
     * @param sql    要执行的SQL查询
     * @param params 查询参数
     * @return 返回查询结果列表，包含每行数据的映射
     * @throws SQLException 如果SQL执行失败
     */
    public List<Map<String, Object>> queryBatch(String sql, Object[] params) throws SQLException {
        return super.runner.query(pool.getConnection(hostname + port + databaseName,config,connectionUser), sql, new MapListHandler(), params);
    }

    /**
     * 获取需要忽略的数据库列表。
     *
     * @return 返回一个包含被忽略的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("information_schema", "mysql", "performance_schema", "sys");
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.MYSQL;
    }

    /**
     * 添加随机采样
     * <h1>不允许出现 ORDER BY、 LIMIT等字眼</h1>
     *
     * @param sqlQueryMontage
     * @return 增加随机抽样后的SQL
     */
    @Override
    public List<String> addRandomSampling(SQLQueryMontage sqlQueryMontage, int N, int M) {
        //抽样样本结果
        SampleResult sampleResult = sqlQueryMontage.getSampleResult();
        sampleResult.setSample_result("增量");
        List<String> list = new ArrayList<>();
        SQLQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().get(0);
        if (sqlQueryBuilder==null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        String sql = sqlQueryBuilder.buildSQL();
        //首先获取总量
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM\\s", "SELECT COUNT(1) FROM ");

        sampleResult.setSample_time(LocalDateTime.now());
        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.get(0).get("count(1)");
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
        String sql = "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '" + dbName + "' AND TABLE_NAME = '" + tableName + "'";
        try (PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
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
                if (map.size() == 0) {
                    continue;
                }
                hashMap.put(COLUMN_NAME, map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw exception(INDEX_INFO_GET_ERROR);
        }
        return hashMap;
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
            try (ResultSet catalogs = metaData.getCatalogs()) {
                while (catalogs.next()) {
                    String databaseName = catalogs.getString("TABLE_CAT");
                    // 如果数据库在忽略列表中，则跳过
                    if (ignoreDatabases.contains(databaseName)) {
                        continue;
                    }
                    if (StringUtils.isNotBlank(dbName) && !databaseName.equals(dbName)) {
                        continue;
                    }
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(databaseName);
                    // 存入DatabaseType
                    databaseEntity.setDatabaseEnum(getDatabaseType());
                    Map<String, TableEntity> tableEntities = new HashMap<>();
                    try (ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");

                            //TODO 查索引
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection,databaseName, tableName);

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            tableEntity.setTableComment(tables.getString("REMARKS")); // 获取表注释
                            Map<String, ColumnEntity> columnEntities = new HashMap<>();
                            try (ResultSet cols = metaData.getColumns(databaseName, null, tableName, "%")) {
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    String TYPE_NAME = cols.getString("TYPE_NAME");
                                    columnEntity.setColumnName(cols.getString("COLUMN_NAME")); // 获取列名
                                    columnEntity.setColumnType(cols.getString("TYPE_NAME")); // 获取列类型
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE")); // 获取列大小
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable); // 获取可否为NULL
                                    columnEntity.setDefaultValue(cols.getString("COLUMN_DEF")); // 获取默认值
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT"))?1:0); // 获取自增状态
                                    columnEntity.setColumnComment(cols.getString("REMARKS")); // 获取列注释

                                    // 判断是什么类型的TYPE_NAME
                                    columnEntity.setType(getColumnType(TYPE_NAME));

                                    //TODO 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                                        if ("PRIMARY".equals(indexDetails.get("INDEX_NAME"))) {
                                            columnEntity.setIsPrimaryKey(1); // 设置为主键
                                        }
                                    }

                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                            }
                            tableEntity.setColumns(columnEntities); // 设置表的列信息
                            tableEntities.put(tableName, tableEntity);
                        }
                    }
                    databaseEntity.setTables(tableEntities); // 设置数据库中的表信息
                    databaseEntities.add(databaseEntity);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA); // 抛出无法检索数据库元数据的异常
        }
        return databaseEntities; // 返回数据库实体列表
    }

    /**
     * 1字符串 2数字 0时间
     *
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
                "DATETIME".equals(columnType)
                || "YEAR".equals(columnType)) {
           return 0;
        }

        return 3;
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        JDBCConnectionEntity
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DM, "127.0.0.1", 5236, "", "SYSDBA", "SYSDBA");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.POSTGRESQL, "192.168.10.103", 5432, "", "postgres", "123456");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.OCEANBASE, "192.168.10.103", 2881, "", "root", "123456");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.TIDB, "192.168.10.103", 4000, "", "root", "123456");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.CLICKHOUSE, "192.168.10.103", 8123, "", "default", "123456");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.KINGBASE8, "192.168.10.103", 54321, "test", "system", "123456");
//                jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.MONGODB, "152.136.154.249", 27017, "Mongo", "Mongo", "123456");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.MYSQL, "127.0.0.1", 3306, "", "root", "123456");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202",9030, "", "root", "123456aA!@");
                jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.MYSQL, "192.168.10.222", 3306, "", "root", "jt@2023!");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.SQLSERVER, "192.168.10.101", 1433, "", "sa", "jt@2023!");
//        jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.POLAR, "192.168.10.240", 4886, "", "user", "passwd");

        MySQLJDBCAdapter adapter = DatabaseAdapterFactory.getAdapter(jdbcConnectionEntity, MySQLJDBCAdapter.class);
        List<DatabaseEntity> databaseMetadata = adapter.getDatabaseMetadata();
        System.out.println(databaseMetadata);
//        Map<String, Map<String, Object>> indexInfo = adapter.getIndexInfo("jt_bds", "system_blacklist_type");
//        System.out.println(indexInfo);
    }
}