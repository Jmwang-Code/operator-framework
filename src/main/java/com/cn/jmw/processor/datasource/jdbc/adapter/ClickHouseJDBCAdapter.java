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
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * ClickHouseJDBCAdapter类用于适配ClickHouse数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与ClickHouse相关的数据库操作，包括获取数据库元数据。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class ClickHouseJdbcAdapter extends AbstractJdbcAdapter {

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
     * @param strategy       连接字符串生成策略
     */
    public ClickHouseJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password,config,connectionUser,test, strategy);
    }

    public ClickHouseJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    public ClickHouseJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null,test,new GeneratedConnectionStringStrategy(null));
//    }
//
//    public ClickHouseJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test)  {
//        this(hostname, port, databaseName, username, password, new JdbcAdapterDataSourceConfig(), null,test,new GeneratedConnectionStringStrategy(null));
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
     * 获取ClickHouse的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return String.format("jdbc:clickhouse://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true&compress=0",
                hostname, port, databaseName);
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
     * @param sqlQueryMontage SQL查询组合对象
     * @param n               抽样点
     * @param m               抽样数量
     * @return 增加随机抽样后的SQL
     */
    @Override
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
        Optional<SampleResult> optionalSampleResult = sqlQueryMontage.getSampleResult();
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleMethod("增量"));

        // 初始化返回结果
        List<String> resultList = new ArrayList<>();

        // 获取 SQL 构建器
        SqlQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().getFirst();
        if (sqlQueryBuilder == null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 构建原始 SQL
        String sql = sqlQueryBuilder.buildSQL();

        // 构建查询总数的 SQL
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");

        // 如果 sampleResult 存在，则设置抽样时间
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleTime(LocalDateTime.now()));

        try {
            // 执行查询获取总数
            List<Map<String, Object>> countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : Long.parseLong(countResult.getFirst().get("COUNT(1)").toString());

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

            // 添加 LIMIT 实现随机抽样
            double percent = totalCount < ((long) n * m * 2) ? 1.0 : (n * m * 2) * 1.0 / totalCount;
            String randomSampling = " SAMPLE " + percent;
            resultList.add(sql + randomSampling);

        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            return resultList;
        }

        return resultList;
    }

    /**
     * 获取数据库元数据，包括所有数据库、表及其列的信息。
     *
     * @return 返回数据库实体列表，包含数据库及其表的详细信息
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    /**
     * 获取指定数据库的元数据。
     *
     * @param dbName 数据库名称
     * @return 返回数据库实体列表，包含数据库及其表的详细信息
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        List<String> ignoreDatabases = getIgnoreDatabaseList();

        try (Connection connection = DriverManager.getConnection(getConnectionString(), username, password);
             PreparedStatement ps = connection.prepareStatement("SELECT name FROM system.databases");
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String databaseName = rs.getString("name");
                if (ignoreDatabases.contains(databaseName) || (StringUtils.isNotBlank(dbName) && !databaseName.equals(dbName))) {
                    continue;
                }

                DatabaseEntity databaseEntity = new DatabaseEntity();
                databaseEntity.setDatabaseName(databaseName);
                databaseEntity.setDatabaseEnum(getDatabaseType());
                Map<String,TableEntity> tableEntities = new HashMap<>(16);

                // 获取指定数据库的所有表
                try (PreparedStatement psTables = connection.prepareStatement("SELECT name,comment FROM system.tables WHERE database = ?")) {
                    psTables.setString(1, databaseName);
                    try (ResultSet rsTables = psTables.executeQuery()) {
                        while (rsTables.next()) {
                            String tableName = rsTables.getString("name");
                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            // 获取表注释
                            tableEntity.setTableComment(rsTables.getString("comment"));
                            Map<String, ColumnEntity> columnEntities = new HashMap<>(16);

                            // 获取指定表的所有列
                            try (PreparedStatement psColumns = connection.prepareStatement("SELECT name, type,comment FROM system.columns WHERE database = ? AND table = ?")) {
                                psColumns.setString(1, databaseName);
                                psColumns.setString(2, tableName);
                                ResultSet rsColumns = psColumns.executeQuery();
                                while (rsColumns.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    columnEntity.setColumnName(rsColumns.getString("name"));
                                    columnEntity.setColumnType(rsColumns.getString("type"));
                                    // 获取字段注释
                                    columnEntity.setColumnComment(rsColumns.getString("comment"));
                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                                rsColumns.close();
                            }
                            tableEntity.setColumns(columnEntities);
                            tableEntities.put(tableName, tableEntity);
                        }
                    }
                }
                databaseEntity.setTables(tableEntities);
                databaseEntities.add(databaseEntity);
            }
        } catch (SQLException e) {
            log.error("获取数据库元数据失败: ", e);
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
    }

    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("INFORMATION_SCHEMA", "information_schema", "system", "mydatabase");
    }
}