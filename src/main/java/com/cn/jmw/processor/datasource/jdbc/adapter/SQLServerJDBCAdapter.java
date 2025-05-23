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
import java.util.*;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * SQLServerJDBCAdapter类用于适配SQL Server数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与SQL Server相关的数据库操作。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class SqlServerJdbcAdapter extends AbstractJdbcAdapter {

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
    public SqlServerJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password,config,connectionUser,test,strategy);
    }

    public SqlServerJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    public SqlServerJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null,test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    public SqlServerJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test)  {
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
     * 获取SQL Server的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return "jdbc:sqlserver://" + super.hostname + ":" + super.port + ";" +
                "database=" + super.databaseName + ";" +
                "Encrypt=true;trustServerCertificate=true;loginTimeout=30;";
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.SQLSERVER;
    }

    /**
     * 获取要忽略的数据库列表。
     *
     * @return 返回一个包含不需要检索的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("msdb","compute_node","DWConfiguration","DWDiagnostics","DWQueue","tempdb","model");
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
        String sql = sqlQueryBuilder.buildSQL().replaceAll("`","").replaceAll("[()]","");
        sql = simplifySql(sql);
        if (StringUtils.isBlank(sql)){
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        Matcher matcher = RANDOM_SAMPLING_COMPILE.matcher(sql);
        if (matcher.find()){
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }

        sql = sql.replaceAll("select","select TOP 10000");

        list.add(sql);
        return list;
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
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
                    if (StringUtils.isNotBlank(dbName) || !dbName.equals(databaseName)) {
                        continue;
                    }
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(databaseName);
                    databaseEntity.setDatabaseEnum(getDatabaseType());
                    Map<String, TableEntity> tableEntities = new HashMap<>(16);
                    try (ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);

                            /*
                              表注释
                             */
                            String tableCommentSQL = "SELECT t.name AS 'tableName', CAST(p.value AS nvarchar(MAX)) AS 'tableComment'\n" +
                                    "FROM "+databaseName+".sys.tables t\n" +
                                    "         LEFT JOIN "+databaseName+".sys.extended_properties p\n" +
                                    "                   ON p.major_id = t.object_id AND p.name = 'MS_Description' AND p.minor_id = 0\n" +
                                    "WHERE t.name = '"+tableName+"'\n";

                            List<Map<String, Object>> tableCommentResult = executeDMLC(tableCommentSQL, null);
                            if (tableCommentResult!=null && !tableCommentResult.isEmpty()) {
                                String tableComment = (String) tableCommentResult.getFirst().get("tableComment");
                                // 获取表注释
                                tableEntity.setTableComment(tableComment);
                            }

                            /*
                              字段注释
                             */
                            String columnCommentSQL = "SELECT c.name AS 'columnName',ex.value AS 'columnComment'\n" +
                                    "FROM "+databaseName+".sys.sysobjects t\n" +
                                    "    INNER JOIN "+databaseName+".sys.syscolumns c ON t.id = c.id\n" +
                                    "    LEFT JOIN "+databaseName+".sys.extended_properties ex ON t.id = ex.major_id AND c.colid = ex.minor_id AND ex.name = 'MS_Description'\n" +
                                    "WHERE t.name = '"+tableName+"'\n";

                            List<Map<String, Object>> columnCommentResult = executeDMLC(columnCommentSQL, null);
                            Map<String, String> columnCommentsMap = new HashMap<>(16);
                            for (Map<String, Object> result : columnCommentResult) {
                                String columnComment = (String) result.get("columnComment");
                                String columnName =  (String) result.get("columnName");
                                columnCommentsMap.put(columnName, columnComment);
                            }

                            /*
                              TODO 查索引
                             */
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection,databaseName, tableName);

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

                                    if (!columnCommentResult.isEmpty()) {
                                        // 获取字段注释
                                        columnEntity.setColumnComment(columnCommentsMap.get(cols.getString("COLUMN_NAME")));
                                    }

                                    //TODO 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                                        if ("1".equals(indexDetails.get("IS_PRIMARY_KEY"))) {
                                            // 设置为主键
                                            columnEntity.setIsPrimaryKey(1);
                                        }

                                        if ("Y".equals(indexDetails.get("IS_AUTOINCREMENT"))){
                                            columnEntity.setAutoIncrement(1);
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
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
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
        String indexSQL = "SELECT\n" +
                "    i.name AS 'INDEX_NAME',\n" +
                "    c.name AS 'COLUMN_NAME',\n" +
                "    i.is_primary_key AS 'IS_PRIMARY_KEY',\n" +
                "    CASE WHEN c.is_identity = 1 THEN 'Y' ELSE 'N' END AS 'IS_AUTOINCREMENT'"+
                "FROM\n" +
                "    sys.indexes i\n" +
                "        INNER JOIN\n" +
                "    sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id\n" +
                "        INNER JOIN\n" +
                "    sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id\n" +
                "        INNER JOIN\n" +
                "    sys.tables t ON i.object_id = t.object_id\n" +
                "WHERE\n" +
                "    DB_NAME() = '"+dbName+"'\n" +
                "    AND t.name = '"+tableName+"'";
        try ( PreparedStatement statement = connection.prepareStatement(indexSQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>(16);
                String COLUMN_NAME = resultSet.getString("COLUMN_NAME");
                map.put("INDEX_NAME", resultSet.getString("INDEX_NAME"));
                map.put("IS_PRIMARY_KEY", resultSet.getString("IS_PRIMARY_KEY"));
                map.put("COLUMN_NAME", COLUMN_NAME);
                map.put("IS_AUTOINCREMENT", resultSet.getString("IS_AUTOINCREMENT"));
                hashMap.put(COLUMN_NAME, map);
            }
        } catch (SQLException e) {
            log.error("获取索引信息失败", e);
            throw exception(INDEX_INFO_GET_ERROR);
        }
        return hashMap;
    }

    public String simplifySql(String sql) {
        // 使用正则表达式去掉表名
        return sql.replaceAll("(?<=\\s|^)\\w+\\.([\\w\\d]+)", "$1");
    }

}