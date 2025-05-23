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
 * DMJDBCAdapter类用于适配DM数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与DM相关的数据库操作，包括获取数据库元数据。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class DmJdbcAdapter extends AbstractJdbcAdapter {

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
    public DmJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password, config, connectionUser, test,strategy);
    }

    public DmJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    /**
//     * 构造函数，省略 connectionUser 参数。
//     */
//    public DmJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null, test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    /**
//     * 构造函数，使用默认配置。
//     */
//    public DmJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test) {
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
     * 获取DM数据库的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return String.format("jdbc:dm://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true",
                hostname, port, databaseName);
    }

    /**
     * 获取需要忽略的数据库列表。
     *
     * @return 需要忽略的数据库名称列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("SYSDBA","SYSSSO","SYS","RESOURCES");
    }

    /**
     * 获取数据库类型。
     *
     * @return 返回数据库类型枚举，表示当前数据库为DM
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.DM;
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

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();

            try (ResultSet catalogs = metaData.getSchemas()) {

                while (catalogs.next()) {
                    String schemaName = catalogs.getString("TABLE_SCHEM");
                    // 跳过在忽略列表中的数据库
                    if (ignoreDatabases.contains(schemaName) || (StringUtils.isNotBlank(dbName) && !schemaName.equals(dbName))) {
                        continue;
                    }

                    // 创建一个新的DatabaseEntity对象，并添加到databaseEntities列表中
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(schemaName);
                    databaseEntity.setDatabaseEnum(getDatabaseType());
                    Map<String, TableEntity> tableEntities = new HashMap<>(16);

                    // 获取所有表的信息
                    ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"});
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");

                        //TODO 查索引
                        Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection, schemaName, tableName);

                        // 创建一个新的TableEntity对象
                        TableEntity tableEntity = new TableEntity();
                        tableEntity.setTableName(tableName);
                        // 获取表注释
                        tableEntity.setTableComment(tables.getString("REMARKS"));
                        Map<String, ColumnEntity> columnEntities = new HashMap<>(16);

                        // 获取列的信息
                        ResultSet columns = metaData.getColumns(null, schemaName, tableName, "%");
                        while (columns.next()) {
                            ColumnEntity columnEntity = new ColumnEntity();
                            String columnName = columns.getString("COLUMN_NAME");
                            String columnType = columns.getString("TYPE_NAME");
                            columnEntity.setColumnName(columnName);
                            columnEntity.setColumnType(columnType);
                            // 获取列注释
                            columnEntity.setColumnComment(columns.getString("REMARKS"));

                            // 判断是什么类型的TYPE_NAME
                            columnEntity.setType(getColumnType(columnType));

                            // 是否是索引
                            if (indexInfo.containsKey(columnName)) {
                                columnEntity.setIsIndex(1);
                                Map<String, Object> indexDetails = indexInfo.get(columnName);
                                columnEntity.setIsPrimaryKey("YES".equals(indexDetails.get("IS_PRIMARY_KEY")) ? 1 : 0);
                            } else {
                                columnEntity.setIsPrimaryKey(0);
                            }

                            columnEntities.put(columnName, columnEntity);
                        }
                        tableEntity.setColumns(columnEntities);
                        tableEntities.put(tableName, tableEntity);
                    }
                    databaseEntity.setTables(tableEntities);
                    databaseEntities.add(databaseEntity);
                }
            }
        } catch (SQLException e) {
            log.error("获取数据库元数据失败: ", e);
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
    }

    /**
     * 获取列类型。
     *
     * @param columnType 数据库中的列类型名称
     * @return 返回类型代码：1 表示字符串，2 表示数字，0 表示时间，3 表示其他
     */
    public int getColumnType(String columnType) {
        if ("CHAR".equals(columnType) || "VARCHAR".equals(columnType) || "CHARACTER".equals(columnType) || "VARCHAR2".equals(columnType)) {
            return 1;
        } else if ("BIGINT".equals(columnType) || "INT".equals(columnType) || "NUMERIC".equals(columnType) || "DECIMAL".equals(columnType) || "NUMBER".equals(columnType) || "INTEGER".equals(columnType)) {
            return 2;
        } else if ("DATE".equals(columnType) || "DATETIME".equals(columnType) || "TIMESTAMP".equals(columnType)) {
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
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection, String dbName, String tableName) {
        Map<String, Map<String, Object>> indexInfo = new HashMap<>(16);
        String sql = "SELECT ui.INDEX_NAME, ui.TABLE_OWNER, ui.TABLE_NAME, ui.UNIQUENESS, uic.COLUMN_NAME, " +
                "ucc.CONSTRAINT_NAME AS PRIMARY_KEY_CONSTRAINT, " +
                "CASE WHEN ucc.CONSTRAINT_TYPE = 'P' THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY " +
                "FROM USER_INDEXES ui " +
                "JOIN USER_IND_COLUMNS uic ON ui.INDEX_NAME = uic.INDEX_NAME " +
                "LEFT JOIN USER_CONSTRAINTS ucc ON ui.INDEX_NAME = ucc.INDEX_NAME " +
                "WHERE ui.TABLE_OWNER = ? AND ui.TABLE_NAME = ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, dbName);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> details = new HashMap<>(16);
                    String columnName = resultSet.getString("COLUMN_NAME");
                    details.put("INDEX_NAME", resultSet.getString("INDEX_NAME"));
                    details.put("TABLE_OWNER", resultSet.getString("TABLE_OWNER"));
                    details.put("TABLE_NAME", resultSet.getString("TABLE_NAME"));
                    details.put("UNIQUENESS", resultSet.getString("UNIQUENESS"));
                    details.put("IS_PRIMARY_KEY", resultSet.getString("IS_PRIMARY_KEY"));
                    details.put("PRIMARY_KEY_CONSTRAINT", resultSet.getString("PRIMARY_KEY_CONSTRAINT"));
                    indexInfo.put(columnName, details);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to retrieve index info: {}", e.getMessage(), e);
            throw exception(INDEX_INFO_GET_ERROR);
        }
        return indexInfo;
    }

    /**
     * 添加伪随机抽样逻辑。
     * <p>
     * 使用 FETCH FIRST ... ROWS ONLY 实现伪随机抽样，仅返回前 m 行，不保证真正的随机性。
     * </p>
     *
     * @param sqlQueryMontage SQL 查询组合对象
     * @param n               抽样点（未使用）
     * @param m               抽样数量
     * @return 包含伪随机抽样逻辑的 SQL 列表
     */
    @Override
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
        Optional<SampleResult> optionalSampleResult = sqlQueryMontage.getSampleResult();
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleMethod("伪随机"));

        // 初始化返回结果
        List<String> list = new ArrayList<>();

        // 获取 SQL 构建器
        SqlQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().getFirst();
        if (sqlQueryBuilder == null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 构建原始 SQL
        String sql = sqlQueryBuilder.buildSQL().replaceAll("`", "");

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
            String randomSampling = "LIMIT " + m;
            list.add(sql + randomSampling);

        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            return list;
        }
        return list;
    }
}