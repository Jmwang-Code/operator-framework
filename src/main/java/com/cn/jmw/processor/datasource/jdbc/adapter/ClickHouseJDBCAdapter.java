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
import com.clickhouse.data.value.UnsignedLong;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * ClickHouseJDBCAdapter类用于适配ClickHouse数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与ClickHouse相关的数据库操作，包括获取数据库元数据。
 * </p>
 */
public class ClickHouseJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建ClickHouseJDBCAdapter实例。
     *
     * @param hostname     ClickHouse服务器的主机名
     * @param port         ClickHouse服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public ClickHouseJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public ClickHouseJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public ClickHouseJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }

    /**
     * 获取ClickHouse的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:clickhouse://" + super.hostname + ":" + super.port + "/" + super.databaseName + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true&compress=0";
    }

    /**
     * 获取数据库类型。
     *
     * @return 返回数据库类型枚举，表示当前数据库为ClickHouse
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.CLICKHOUSE;
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

        //获取COUNT总量
        //sql替换FROM之前的变成SELECT COUNT(1)
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");
        sampleResult.setSample_time(LocalDateTime.now());
        //执行COUNT
        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long totalCount = countResult.isEmpty() ? 0 : Long.parseLong(((UnsignedLong)countResult.get(0).get("COUNT(1)")).toString());
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
        double percent = totalCount<(N * M * 2)?1.0:(N * M * 2) * 1.0 / totalCount;

        String randomSampling = " SAMPLE "+percent;
        sql = sql + randomSampling;
        list.add(sql);
        return list;
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    /**
     * 获取数据库元数据，包括所有数据库、表及其列的信息。
     *
     * @return 返回数据库实体列表，包含数据库及其表的详细信息
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        List<String> ignoreDatabases = getIgnoreDatabaseList();
        try (Connection connection = DriverManager.getConnection(getConnectionString(), username, password)) {
            // 获取所有数据库
            try (PreparedStatement ps = connection.prepareStatement("SELECT name FROM system.databases"); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String databaseName = rs.getString("name");
                    // 跳过在忽略列表中的数据库
                    if (ignoreDatabases.contains(databaseName)) {
                        continue;
                    }
                    if (StringUtils.isNotBlank(dbName) && !databaseName.equals(dbName)) {
                        continue;
                    }
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(databaseName);
                    databaseEntity.setDatabaseEnum(getDatabaseType());
                    Map<String,TableEntity> tableEntities = new HashMap<>();
                    // 获取指定数据库的所有表
                    try (PreparedStatement psTables = connection.prepareStatement("SELECT name,comment FROM system.tables WHERE database = ?")) {
                        psTables.setString(1, databaseName);
                        ResultSet rsTables = psTables.executeQuery();
                        while (rsTables.next()) {
                            String tableName = rsTables.getString("name");
                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            tableEntity.setTableComment(rsTables.getString("comment")); // 获取表注释
                            Map<String,ColumnEntity> columnEntities = new HashMap<>();
                            // 获取指定表的所有列
                            try (PreparedStatement psColumns = connection.prepareStatement("SELECT name, type,comment FROM system.columns WHERE database = ? AND table = ?")) {
                                psColumns.setString(1, databaseName);
                                psColumns.setString(2, tableName);
                                ResultSet rsColumns = psColumns.executeQuery();
                                while (rsColumns.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    columnEntity.setColumnName(rsColumns.getString("name"));
                                    columnEntity.setColumnType(rsColumns.getString("type"));
                                    columnEntity.setColumnComment(rsColumns.getString("comment")); // 获取字段注释
                                    columnEntities.put(columnEntity.getColumnName(),columnEntity);
                                }
                                rsColumns.close();
                            }
                            tableEntity.setColumns(columnEntities);
                            tableEntities.put(tableName,tableEntity);
                        }
                        rsTables.close();
                    }
                    databaseEntity.setTables(tableEntities);
                    databaseEntities.add(databaseEntity);
                }
            }
        } catch (SQLException e) {
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
    }

    /**
     * 获取要忽略的数据库列表。
     *
     * @return 返回一个包含不需要检索的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("INFORMATION_SCHEMA", "information_schema", "system", "mydatabase");
    }
}