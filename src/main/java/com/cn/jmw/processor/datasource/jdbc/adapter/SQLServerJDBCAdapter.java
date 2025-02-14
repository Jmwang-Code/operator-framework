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
 */
public class SQLServerJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建SQLServerJDBCAdapter实例。
     *
     * @param hostname     SQL Server服务器的主机名
     * @param port         SQL Server服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public SQLServerJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public SQLServerJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public SQLServerJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取SQL Server的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
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
        if (StringUtils.isBlank(sql)){
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        Matcher matcher = RandomSamplingCompile.matcher(sql);
        if (matcher.find()){
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }

        //获取COUNT总量
        //sql替换FROM之前的变成SELECT COUNT(1)
        String countSql = sql
                .replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM")
                .replaceAll("(?i)ORDER\\s+BY\\s+.*$", "");
        //执行COUNT
        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        long totalCount = countResult.isEmpty() ? 0 : (Integer) countResult.get(0).get("1");
        //totalCount和(N*M*2)的百分比
        double percent = totalCount<(N * M * 2)?1.0:(N * M * 2) * 1.0 / totalCount;

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
                    if (StringUtils.isNotBlank(dbName) || !dbName.equals(databaseName)) {
                        continue;
                    }
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(databaseName);
                    databaseEntity.setDatabaseEnum(getDatabaseType());
                    Map<String, TableEntity> tableEntities = new HashMap<>();
                    try (ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);

                            /**
                             * 表注释
                             */
                            String tableCommentSQL = "SELECT t.name AS 'tableName', CAST(p.value AS nvarchar(MAX)) AS 'tableComment'\n" +
                                    "FROM "+databaseName+".sys.tables t\n" +
                                    "         LEFT JOIN "+databaseName+".sys.extended_properties p\n" +
                                    "                   ON p.major_id = t.object_id AND p.name = 'MS_Description' AND p.minor_id = 0\n" +
                                    "WHERE t.name = '"+tableName+"'\n";

                            List<Map<String, Object>> tableCommentResult = executeDMLC(tableCommentSQL, null);
                            if (tableCommentResult!=null && !tableCommentResult.isEmpty()) {
                                String tableComment = (String) tableCommentResult.get(0).get("tableComment");
                                // 获取表注释
                                tableEntity.setTableComment(tableComment); // 获取表注释
                            }

                            /**
                             * 字段注释
                             */
                            String columnCommentSQL = "SELECT c.name AS 'columnName',ex.value AS 'columnComment'\n" +
                                    "FROM "+databaseName+".sys.sysobjects t\n" +
                                    "    INNER JOIN "+databaseName+".sys.syscolumns c ON t.id = c.id\n" +
                                    "    LEFT JOIN "+databaseName+".sys.extended_properties ex ON t.id = ex.major_id AND c.colid = ex.minor_id AND ex.name = 'MS_Description'\n" +
                                    "WHERE t.name = '"+tableName+"'\n";

                            List<Map<String, Object>> columnCommentResult = executeDMLC(columnCommentSQL, null);
                            Map<String, String> columnCommentsMap = new HashMap<>();
                            for (Map<String, Object> result : columnCommentResult) {
                                String columnComment = (String) result.get("columnComment");
                                String columnName =  (String) result.get("columnName");
                                columnCommentsMap.put(columnName, columnComment);
                            }

                            /**
                             * TODO 查索引
                             */
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection,databaseName, tableName);

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

                                    if (columnCommentResult!=null && !columnCommentResult.isEmpty()) {
                                        // 获取字段注释
                                        columnEntity.setColumnComment(columnCommentsMap.get(cols.getString("COLUMN_NAME"))); // 获取字段注释
                                    }

                                    //TODO 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                                        if ("1".equals(indexDetails.get("IS_PRIMARY_KEY"))) {
                                            columnEntity.setIsPrimaryKey(1); // 设置为主键
                                        }

                                        if ("Y".equals(indexDetails.get("IS_AUTOINCREMENT"))){
                                            columnEntity.setAutoIncrement(1);
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
     * 获取索引
     *
     * @param dbName    数据库
     * @param tableName 数据表
     * @return 索引
     */
    @Override
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection,String dbName, String tableName) {
        Map<String, Map<String, Object>> hashMap = new HashMap<>();
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
                Map<String, Object> map = new HashMap<>();
                String COLUMN_NAME = resultSet.getString("COLUMN_NAME");
                map.put("INDEX_NAME", resultSet.getString("INDEX_NAME"));
                map.put("IS_PRIMARY_KEY", resultSet.getString("IS_PRIMARY_KEY"));
                map.put("COLUMN_NAME", COLUMN_NAME);
                map.put("IS_AUTOINCREMENT", resultSet.getString("IS_AUTOINCREMENT"));
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

    public String simplifySql(String sql) {
        // 使用正则表达式去掉表名
        return sql.replaceAll("(?<=\\s|^)\\w+\\.([\\w\\d]+)", "$1");
    }

}