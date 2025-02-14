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
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * OceanBaseJDBCAdapter类用于适配OceanBase数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与OceanBase相关的数据库操作。
 * </p>
 */
public class OceanBaseJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建OceanBaseJDBCAdapter实例。
     *
     * @param hostname     OceanBase服务器的主机名
     * @param port         OceanBase服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public OceanBaseJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public OceanBaseJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public OceanBaseJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取OceanBase的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:oceanbase://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?rewriteBatchedStatements=TRUE&allowMultiQueries=TRUE&useLocalSessionState=TRUE&useUnicode=TRUE&characterEncoding=utf-8&socketTimeout=10000&connectTimeout=30000";
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.OCEANBASE;
    }

    /**
     * 获取需要忽略的数据库列表。
     *
     * @return 返回一个包含被忽略的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("information_schema", "mysql", "SYS", "LBACSYS", "ORAAUDITOR", "oceanbase","ocs");
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
        String sql = sqlQueryBuilder.buildSQL();
        if (StringUtils.isBlank(sql)){
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        Matcher matcher = RandomSamplingCompile.matcher(sql);
        if (matcher.find()){
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }

        //获取COUNT总量
        //sql替换FROM之前的变成SELECT COUNT(1)
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");
        //执行COUNT
        sampleResult.setSample_time(LocalDateTime.now());
        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
        //totalCount和(N*M*2)的百分比
        sampleResult.setSample_count(M);
        String randomSampling = " LIMIT "+M;
        sql = sql + randomSampling;
        list.add(sql);
        return list;
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
                    if (StringUtils.isNotBlank(dbName) && !dbName.equals(databaseName)) {
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
                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            // 获取表注释
                            tableEntity.setTableComment(tables.getString("REMARKS")); // 获取表注释

                            Map<String, ColumnEntity> columnEntities = new HashMap<>();
                            try (ResultSet cols = metaData.getColumns(databaseName, null, tableName, "%")) {
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    columnEntity.setColumnName(cols.getString("COLUMN_NAME")); // 获取列名
                                    columnEntity.setColumnType(cols.getString("TYPE_NAME")); // 获取列类型
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE")); // 获取列大小
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable); // 获取可否为NULL
                                    columnEntity.setDefaultValue(cols.getString("COLUMN_DEF")); // 获取默认值
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0); // 获取自增状态
                                    // 获取字段注释
                                    columnEntity.setColumnComment(cols.getString("REMARKS")); // 获取字段注释

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
}