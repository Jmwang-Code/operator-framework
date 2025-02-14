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

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * OracleJDBCAdapter类用于适配Oracle数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与Oracle相关的数据库操作。
 * </p>
 */
public class OracleJDBCAdapter extends JDBCAdapter {

    /**
     * 构造函数用于创建OracleJDBCAdapter实例。
     *
     * @param hostname     Oracle服务器的主机名
     * @param port         Oracle服务器的端口号
     * @param sid          Oracle数据库的SID
     * @param username     用户名
     * @param password     密码
     */
    public OracleJDBCAdapter(String hostname, Integer port, String sid, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, sid, username, password,config,connectionUser);
    }

    public OracleJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public OracleJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }

    /**
     * 获取Oracle的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:oracle:thin:@" + super.hostname + ":" + super.port + ":ORCL"// + super.databaseName
                +"?useUnicode=true&characterEncoding=UTF-8";
    }

    @Override
    public String getValidationQuery(){
        return "SELECT 1 FROM DUAL";
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.ORACLE;
    }

    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("ORDDCM_MAPPING_DOCS","ANONYMOUS","APEX_030200","APEX_PUBLIC_USER","APPQOSSYS","BI","CTXSYS"
            ,"DBSNMP","DIP","EXFSYS","FLOWS_FILES","HR","IX","MDDATA","MDSYS","MGMT_VIEW","OE","OLAPSYS","ORACLE_OCM","ORDDATA"
            ,"ORDPLUGINS","ORDSYS","OUTLN","OWBSYS","OWBSYS_AUDIT","PM","SCOTT","SH","SI_INFORMTN_SCHEMA","SPATIAL_CSW_ADMIN_USR"
        ,"SPATIAL_WFS_ADMIN_USR","SYS","SYSMAN","SYSTEM","WMSYS","XDB","XS$NULL");
    }

//    @Override
//    public List<DatabaseEntity> getDatabaseMetadata() {
//        List<DatabaseEntity> databaseEntities = new ArrayList<>();
//        List<String> ignoreDatabases = getIgnoreDatabaseList(); // 获取需要忽略的数据库列表
//        try (Connection connection = pool.getConnection(hostname + port + databaseName)) {
//            DatabaseMetaData metaData = connection.getMetaData();
//
//            // 在Oracle中，使用getSchemas()而非getCatalogs()
//            try (ResultSet schemas = metaData.getSchemas()) {
//                while (schemas.next()) {
//                    String schemaName = schemas.getString("TABLE_SCHEM");
//
//                    // 如果Schema在忽略列表中，则跳过
//                    if (ignoreDatabases.contains(schemaName)) {
//                        continue;
//                    }
//
//                    DatabaseEntity databaseEntity = new DatabaseEntity();
//                    databaseEntity.setDatabaseName(schemaName); // 设置为Schema名称
//                    databaseEntity.setDatabaseEnum(getDatabaseType());
//
//                    Map<String, TableEntity> tableEntities = new HashMap<>();
//                    // 注意：将catalog设置为null，并使用schema进行过滤
//                    try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
//                        while (tables.next()) {
//                            String tableName = tables.getString("TABLE_NAME");
//
//                            //TODO 查索引
//                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(schemaName, tableName);
//
//                            TableEntity tableEntity = new TableEntity();
//                            tableEntity.setTableName(tableName);
//
//                            Map<String, ColumnEntity> columnEntities = new HashMap<>();
//                            try (ResultSet cols = metaData.getColumns(null, schemaName, tableName, "%")) {
//                                while (cols.next()) {
//                                    String columnName = cols.getString("COLUMN_NAME");
//                                    String columnType = cols.getString("TYPE_NAME");
//
//                                    ColumnEntity columnEntity = new ColumnEntity();
//                                    columnEntity.setColumnName(columnName);
//                                    columnEntity.setColumnType(columnType);
//                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE"));
//                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
//                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0);
//
//                                    // 判断是什么类型的TYPE_NAME
//                                    columnEntity.setType(getColumnType(columnType));
//
//                                    //TODO 是否是索引
//                                    if (indexInfo.containsKey(columnName)) {
//                                        //是不是索引
//                                        columnEntity.setIsIndex(1);
//                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
//                                        //设置主键
//                                        if ("YES".equals(indexDetails.get("IS_PRIMARY_KEY"))) {
//                                            columnEntity.setIsPrimaryKey(1); // 设置为主键
//                                        } else {
//                                            columnEntity.setIsPrimaryKey(0); // 设置为主键
//                                        }
//                                    }
//
//                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
//                                }
//                            }catch (Exception e){
//                                e.printStackTrace();
//                                continue;
//                            }
//                            tableEntity.setColumns(columnEntities);
//                            tableEntities.put(tableName, tableEntity);
//                        }
//                    }
//                    databaseEntity.setTables(tableEntities);
//                    databaseEntities.add(databaseEntity);
//                }
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
//        }
//        return databaseEntities;
//    }

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

            // 在Oracle中，使用getSchemas()而非getCatalogs()
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");

                    // 如果Schema在忽略列表中，则跳过
                    if (ignoreDatabases.contains(schemaName)) {
                        continue;
                    }
                    if (StringUtils.isNotBlank(dbName) && !schemaName.equals(dbName)) {
                        continue;
                    }

                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(schemaName); // 设置为Schema名称
                    databaseEntity.setDatabaseEnum(getDatabaseType());

                    Map<String, TableEntity> tableEntities = new HashMap<>();
                    // 注意：将catalog设置为null，并使用schema进行过滤
                    try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");

                            // 获取表注释
                            String tableComment = null;
                            try (Connection connection1 = pool.getConnection(hostname + port + databaseName,config,connectionUser);
                                    PreparedStatement stmt = connection1.prepareStatement("SELECT COMMENTS FROM ALL_TAB_COMMENTS WHERE TABLE_NAME = ? AND OWNER = ?")) {
                                stmt.setString(1, tableName);
                                stmt.setString(2, schemaName);
                                try (ResultSet comments = stmt.executeQuery()) {
                                    if (comments.next()) {
                                        tableComment = comments.getString("COMMENTS");
                                    }
                                }
                            }

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            tableEntity.setTableComment(tableComment); // 设置表注释

                            // 获取索引信息
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection,schemaName, tableName);

                            Map<String, ColumnEntity> columnEntities = new HashMap<>();
                            try (ResultSet cols = metaData.getColumns(null, schemaName, tableName, "%")) {
                                while (cols.next()) {
                                    String columnName = cols.getString("COLUMN_NAME");
                                    String columnType = cols.getString("TYPE_NAME");

                                    ColumnEntity columnEntity = new ColumnEntity();
                                    columnEntity.setColumnName(columnName);
                                    columnEntity.setColumnType(columnType);
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE"));
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0);

                                    // 判断是什么类型的TYPE_NAME
                                    columnEntity.setType(getColumnType(columnType));

                                    // 判断是否是索引
                                    if (indexInfo.containsKey(columnName)) {
                                        columnEntity.setIsIndex(1);
                                        Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                                        // 设置主键
                                        columnEntity.setIsPrimaryKey("YES".equals(indexDetails.get("IS_PRIMARY_KEY")) ? 1 : 0);
                                    }

                                    // 获取字段注释
                                    String columnComment = null;
                                    try (Connection connection2 = pool.getConnection(hostname + port + databaseName,config,connectionUser);
                                            PreparedStatement commentStmt = connection2.prepareStatement("SELECT COMMENTS FROM ALL_COL_COMMENTS WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND OWNER = ?")) {
                                        commentStmt.setString(1, tableName);
                                        commentStmt.setString(2, columnName);
                                        commentStmt.setString(3, schemaName);
                                        try (ResultSet columnComments = commentStmt.executeQuery()) {
                                            if (columnComments.next()) {
                                                columnComment = columnComments.getString("COMMENTS");
                                            }
                                        }
                                    }
                                    columnEntity.setColumnComment(columnComment); // 设置字段注释

                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                continue;
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
            throw new RuntimeException("无法检索数据库元数据", e);
        }
        return databaseEntities;
    }


    /**
     * TODO
     * SELECT INDEX_NAME,INDEX_TYPE,TABLE_OWNER,TABLE_NAME,UNIQUENESS
     * FROM dba_indexes
     * WHERE table_owner = 'JMW' AND table_name = 'TEST';
     */
    @Override
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection,String dbName, String tableName) {
        Map<String, Map<String, Object>> hashMap = new HashMap<>();
        String sql = "SELECT i.index_name, i.index_type, i.table_owner, i.table_name, i.uniqueness, c.column_name,\n" +
                "       CASE WHEN pk.index_name IS NOT NULL THEN 'Yes' ELSE 'No' END AS is_primary_key\n" +
                "FROM dba_indexes i\n" +
                "JOIN dba_ind_columns c ON i.index_name = c.index_name\n" +
                "LEFT JOIN dba_constraints pk ON i.table_owner = pk.owner AND i.table_name = pk.table_name AND i.index_name = pk.index_name AND pk.constraint_type = 'P'" +
                "WHERE i.table_owner = '"+dbName+"'\n" +
                "  AND i.table_name = '"+tableName+"'\n" +
                "ORDER BY i.index_name, c.column_position";
        try (PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                String COLUMN_NAME = resultSet.getString("COLUMN_NAME");
                map.put("INDEX_NAME", resultSet.getString("INDEX_NAME"));
                map.put("TABLE_OWNER", resultSet.getString("TABLE_OWNER"));
                map.put("TABLE_NAME", resultSet.getString("TABLE_NAME"));
                map.put("UNIQUENESS", resultSet.getString("UNIQUENESS"));
                map.put("IS_PRIMARY_KEY", resultSet.getString("IS_PRIMARY_KEY"));
                if (map.size() == 0) {
                    continue;
                }
                hashMap.put(COLUMN_NAME, map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return hashMap;
//            throw exception(INDEX_INFO_GET_ERROR);
        }
        return hashMap;
    }

    /**
     * 1字符串 2数字 0时间
     * <p>
     * 判断输入的字符串名称是什么类型的数据字段
     */
    public int getColumnType(String columnType) {
        if ("CHAR".equals(columnType) || "VARCHAR".equals(columnType) ||
                "CHARACTER".equals(columnType) || "VARCHAR2".equals(columnType)) {
            return 1;
        } else if ("BIGINT".equals(columnType) || "INT".equals(columnType)
                || "LARGEINT".equals(columnType)
                || "NUMERIC".equals(columnType)
                || "DECIMAL".equals(columnType)
                || "NUMBER".equals(columnType)
                || "INTEGER".equals(columnType)) {
            return 2;
        } else if ("DATE".equals(columnType) ||
                "INTERVAL".equals(columnType) ||
                "TIMESTAMP".equals(columnType)) {
            return 0;
        }

        return 3;
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
        String sql = sqlQueryBuilder.buildSQL().replace("`","");
        //首先获取总量
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");

        sampleResult.setSample_time(LocalDateTime.now());
        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : Long.parseLong(((BigDecimal) countResult.get(0).get("count(1)")).toString());
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
            e.printStackTrace();
//            throw new RuntimeException(e);
        }

        sampleResult.setSample_count(M);
        String randomSampling = "LIMIT " + M;
        sql = sql + randomSampling;
        list.add(sql);
        return list;
    }
}