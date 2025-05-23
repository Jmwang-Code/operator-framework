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
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * OracleJDBCAdapter类用于适配Oracle数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与Oracle相关的数据库操作。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class OracleJdbcAdapter extends AbstractJdbcAdapter {

    /**
     * 元数据表注释
     */
    final String ALL_TAB_COMMENTS = "SELECT TABLE_NAME, COMMENTS FROM ALL_TAB_COMMENTS";
    /**
     * 元数据列注释
     */
    final String ALL_COL_COMMENTS = "SELECT COLUMN_NAME, COMMENTS FROM ALL_COL_COMMENTS WHERE TABLE_NAME = ?";

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
    public OracleJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password, config, connectionUser, test,strategy);
    }

    public OracleJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    public OracleJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null, test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    public OracleJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test) {
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
     * 获取Oracle的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return String.format("jdbc:oracle:thin:@%s:%d:%s?useUnicode=true&characterEncoding=UTF-8", hostname, port, databaseName);
    }

    /**
     * 获取验证查询语句。
     *
     * @return 返回用于验证连接的SQL查询
     */
    @Override
    public String getValidationQuery() {
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

    /**
     * 获取需要忽略的数据库列表。
     *
     * @return 返回一个包含被忽略的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList(
//        11g版本的系统库
                "ORDDCM_MAPPING_DOCS", "ANONYMOUS", "APEX_030200", "APEX_PUBLIC_USER", "APPQOSSYS", "BI", "CTXSYS"
                , "DBSNMP", "DIP", "EXFSYS", "FLOWS_FILES", "HR", "IX", "MDDATA", "MDSYS", "MGMT_VIEW", "OE", "OLAPSYS", "ORACLE_OCM", "ORDDATA"
                , "ORDPLUGINS", "ORDSYS", "OUTLN", "OWBSYS", "OWBSYS_AUDIT", "PM", "SCOTT", "SH", "SI_INFORMTN_SCHEMA", "SPATIAL_CSW_ADMIN_USR"
                , "SPATIAL_WFS_ADMIN_USR", "SYS", "SYSMAN", "SYSTEM", "WMSYS", "XDB", "XS$NULL"
//        19c版本的系统库
                , "AUDSYS", "DBSFWUSER", "DVF", "DVSYS", "GGSYS", "GSMADMIN_INTERNAL", "GSMCATUSER", "GSMROOTUSER", "GSMUSER", "LBACSYS", "MDDATA"
                , "OJVMSYS", "REMOTE_SCHEDULER_AGENT", "SYS$UMF", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC");

//        return Arrays.asList(
//        "ORDDCM_MAPPING_DOCS","ANONYMOUS","APEX_030200","APEX_PUBLIC_USER","APPQOSSYS","BI","CTXSYS"
//            ,"DBSNMP","DIP","EXFSYS","FLOWS_FILES","HR","IX","MDDATA","MDSYS","MGMT_VIEW","OE","OLAPSYS","ORACLE_OCM","ORDDATA"
//            ,"ORDPLUGINS","ORDSYS","OUTLN","OWBSYS","OWBSYS_AUDIT","PM","SCOTT","SH","SI_INFORMTN_SCHEMA","SPATIAL_CSW_ADMIN_USR"
//        ,"SPATIAL_WFS_ADMIN_USR","SYS","SYSMAN","SYSTEM","WMSYS","XDB","XS$NULL"
//        ,"AUDSYS","DBSFWUSER","DVF","DVSYS","GGSYS","GSMADMIN_INTERNAL","GSMCATUSER","GSMROOTUSER","GSMUSER","MDDATA"
//        ,"OJVMSYS","REMOTE_SCHEDULER_AGENT","SYS$UMF","SYSBACKUP","SYSDG","SYSKM","SYSRAC");

//        return Arrays.asList("SYS","MDSYS","SYSTEM","ORDDATA","CTXSYS","DVSYS","GSMADMIN_INTERNAL","WMSYS","XDB","LBACSYS","DBSNMP");
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    /**
     * 获取数据库元数据信息。
     *
     * @param dbName 数据库名称（可选）
     * @return 数据库实体列表，包含数据库中的表信息
     */
    /**
     * 获取数据库元数据信息。
     *
     * @param dbName 数据库名称（可选）
     * @return 数据库实体列表，包含数据库中的表信息
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        List<String> ignoreDatabases = getIgnoreDatabaseList();
        List<String> errors = new ArrayList<>();

        log.info("开始获取数据库元数据，dbName: {}", dbName);
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
            connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), 60_000);
            DatabaseMetaData metaData = connection.getMetaData();
            log.info("成功获取 DatabaseMetaData");

// 一次性获取所有表注释
            Map<String, Map<String, String>> globalTableComments = new HashMap<>();
            StringBuilder allTabCommentsSql = new StringBuilder("SELECT OWNER, TABLE_NAME, COMMENTS FROM ALL_TAB_COMMENTS");
            if (!ignoreDatabases.isEmpty()) {
                allTabCommentsSql.append(" WHERE OWNER NOT IN (")
                        .append(ignoreDatabases.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")))
                        .append(")");
            }
            try (PreparedStatement stmt = connection.prepareStatement(allTabCommentsSql.toString())) {
                stmt.setQueryTimeout(60); // 增加超时以处理大数据量
                log.info("执行全局表注释查询: {}", allTabCommentsSql);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String schemaName = rs.getString("OWNER");
                        String tableName = rs.getString("TABLE_NAME");
                        String comment = rs.getString("COMMENTS");
                        if (StringUtils.isNotBlank(comment)) {
                            globalTableComments.computeIfAbsent(schemaName, k -> new HashMap<>()).put(tableName, comment);
                        }
                    }
                    log.info("成功获取全局表注释，schema 数量: {}", globalTableComments.size());
                }
            } catch (SQLException e) {
                errors.add("获取全局表注释失败: " + e.getMessage());
                log.error("获取全局表注释失败: {}", e.getMessage(), e);
            }

// 一次性获取所有列注释
            Map<String, Map<String, String>> globalColumnComments = new HashMap<>();
            StringBuilder allColCommentsSql = new StringBuilder("SELECT OWNER, TABLE_NAME, COLUMN_NAME, COMMENTS FROM ALL_COL_COMMENTS");
            if (!ignoreDatabases.isEmpty()) {
                allColCommentsSql.append(" WHERE OWNER NOT IN (")
                        .append(ignoreDatabases.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")))
                        .append(")");
            }
            try (PreparedStatement stmt = connection.prepareStatement(allColCommentsSql.toString())) {
                stmt.setQueryTimeout(60);
                log.info("执行全局列注释查询: {}", allColCommentsSql);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String schemaName = rs.getString("OWNER");
                        String tableName = rs.getString("TABLE_NAME");
                        String columnName = rs.getString("COLUMN_NAME");
                        String comment = rs.getString("COMMENTS");
                        if (StringUtils.isNotBlank(comment)) {
                            globalColumnComments.computeIfAbsent(schemaName + "." + tableName, k -> new HashMap<>()).put(columnName, comment);
                        }
                    }
                    log.info("成功获取全局列注释，表数量: {}", globalColumnComments.size());
                }
            } catch (SQLException e) {
                errors.add("获取全局列注释失败: " + e.getMessage());
                log.error("获取全局列注释失败: {}", e.getMessage(), e);
            }

// 一次性获取所有索引信息
            Map<String, Map<String, Map<String, Object>>> globalIndexInfo = new HashMap<>();
            StringBuilder allIndexesSql = new StringBuilder(
                    "SELECT i.TABLE_OWNER, i.TABLE_NAME, i.INDEX_NAME, i.UNIQUENESS, c.COLUMN_NAME, " +
                            "CASE WHEN pk.INDEX_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY " +
                            "FROM ALL_INDEXES i " +
                            "JOIN ALL_IND_COLUMNS c ON i.INDEX_NAME = c.INDEX_NAME AND i.TABLE_OWNER = c.TABLE_OWNER " +
                            "LEFT JOIN ALL_CONSTRAINTS pk ON i.TABLE_OWNER = pk.OWNER AND i.TABLE_NAME = pk.TABLE_NAME " +
                            "AND i.INDEX_NAME = pk.INDEX_NAME AND pk.CONSTRAINT_TYPE = 'P'"
            );
            if (!ignoreDatabases.isEmpty()) {
                allIndexesSql.append(" WHERE i.TABLE_OWNER NOT IN (")
                        .append(ignoreDatabases.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")))
                        .append(")");
            }
            try (PreparedStatement stmt = connection.prepareStatement(allIndexesSql.toString())) {
                stmt.setQueryTimeout(60);
                log.info("执行全局索引查询: {}", allIndexesSql);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String schemaName = rs.getString("TABLE_OWNER");
                        String tableName = rs.getString("TABLE_NAME");
                        String columnName = rs.getString("COLUMN_NAME");
                        Map<String, Object> details = new HashMap<>();
                        details.put("INDEX_NAME", rs.getString("INDEX_NAME"));
                        details.put("UNIQUENESS", rs.getString("UNIQUENESS"));
                        details.put("IS_PRIMARY_KEY", rs.getString("IS_PRIMARY_KEY"));
                        globalIndexInfo.computeIfAbsent(schemaName + "." + tableName, k -> new HashMap<>()).put(columnName, details);
                    }
                    log.info("成功获取全局索引信息，表数量: {}", globalIndexInfo.size());
                }
            } catch (SQLException e) {
                errors.add("获取全局索引信息失败: " + e.getMessage());
                log.error("获取全局索引信息失败: {}", e.getMessage(), e);
            }

            // 遍历 schema
            try (ResultSet schemas = metaData.getSchemas()) {
                log.info("开始遍历 schemas");
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    if (ignoreDatabases.contains(schemaName) || (StringUtils.isNotBlank(dbName) && !schemaName.equals(dbName))) {
                        continue;
                    }
                    log.info("处理 schema: {}", schemaName);

                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(schemaName);
                    databaseEntity.setDatabaseEnum(getDatabaseType());

                    Map<String, TableEntity> tableEntities = new HashMap<>(16);
                    try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
                        log.info("开始获取 {} 的表信息", schemaName);
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            log.debug("处理表: {}", tableName);

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            tableEntity.setTableComment(globalTableComments.getOrDefault(schemaName, new HashMap<>()).get(tableName));

                            Map<String, ColumnEntity> columnEntities = new HashMap<>(16);
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
                                    columnEntity.setType(getColumnType(columnType));
                                    columnEntity.setColumnComment(globalColumnComments.getOrDefault(schemaName + "." + tableName, new HashMap<>()).get(columnName));

                                    Map<String, Object> indexDetails = globalIndexInfo.getOrDefault(schemaName + "." + tableName, new HashMap<>()).get(columnName);
                                    if (indexDetails != null) {
                                        columnEntity.setIsIndex(1);
                                        columnEntity.setIsPrimaryKey("YES".equals(indexDetails.get("IS_PRIMARY_KEY")) ? 1 : 0);
                                    }

                                    columnEntities.put(columnName, columnEntity);
                                }
                                log.debug("成功获取表 {} 的列信息，列数: {}", tableName, columnEntities.size());
                            } catch (SQLException e) {
                                errors.add("获取表 " + tableName + " 的列信息失败: " + e.getMessage());
                                log.error("获取表 {} 的列信息失败: {}", tableName, e.getMessage(), e);
                                continue;
                            }
                            tableEntity.setColumns(columnEntities);
                            tableEntities.put(tableName, tableEntity);
                        }
                        log.info("完成 {} 的表信息获取，表数: {}", schemaName, tableEntities.size());
                    }
                    databaseEntity.setTables(tableEntities);
                    databaseEntities.add(databaseEntity);
                }
                log.info("完成所有 schema 处理，schema 数量: {}", databaseEntities.size());
            }
        } catch (SQLException e) {
            log.error("获取数据库元数据失败: {}", e.getMessage(), e);
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }

        if (!errors.isEmpty()) {
            log.warn("部分元数据获取失败，错误详情: {}", String.join("; ", errors));
        }
        log.info("数据库元数据获取完成，返回结果大小: {}", databaseEntities.size());
        return databaseEntities;
    }


//    @Override
//    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
//        List<DatabaseEntity> databaseEntities = new ArrayList<>();
//        // 获取需要忽略的数据库列表
//        List<String> ignoreDatabases = getIgnoreDatabaseList();
//        List<String> errors = new ArrayList<>();
//
//        log.info("开始获取数据库元数据，dbName: {}", dbName);
//        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
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
//                    if (StringUtils.isNotBlank(dbName) && !schemaName.equals(dbName)) {
//                        continue;
//                    }
//
//                    DatabaseEntity databaseEntity = new DatabaseEntity();
//                    // 设置为Schema名称
//                    databaseEntity.setDatabaseName(schemaName);
//                    databaseEntity.setDatabaseEnum(getDatabaseType());
//
//                    // 获取表注释
//                    Map<String, String> tableCommentsMap = new HashMap<>();
//                    try (PreparedStatement stmt = connection.prepareStatement(ALL_TAB_COMMENTS)) {
//                        try (ResultSet comments = stmt.executeQuery()) {
//                            while (comments.next()) {
//                                String tableName = comments.getString("TABLE_NAME");
//                                String tableComment = comments.getString("COMMENTS");
//                                tableCommentsMap.put(tableName, tableComment);
//                            }
//                        }
//                    } catch (SQLException e) {
//                        log.error("获取表注释失败: {}", e.getMessage());
//                        continue;
//                    }
//
//                    Map<String, TableEntity> tableEntities = new HashMap<>(16);
//                    // 注意：将catalog设置为null，并使用schema进行过滤
//                    try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
//                        while (tables.next()) {
//                            String tableName = tables.getString("TABLE_NAME");
//
//                            // 获取该表的所有列注释，提前查询并缓存
//                            Map<String, String> columnCommentsMap = new HashMap<>();
//                            try (PreparedStatement commentStmt = connection.prepareStatement(
//                                    ALL_COL_COMMENTS)) {
//                                commentStmt.setString(1, tableName);
//                                log.warn(ALL_COL_COMMENTS.replace("?", "'" + tableName + "'"));
//                                try (ResultSet columnComments = commentStmt.executeQuery()) {
//                                    while (columnComments.next()) {
//                                        String columnName = columnComments.getString("COLUMN_NAME");
//                                        String comment = columnComments.getString("COMMENTS");
//                                        columnCommentsMap.put(columnName, comment);
//                                    }
//                                }
//                            } catch (SQLException e) {
//                                log.error("获取表 {} 的列注释失败: {}", tableName, e.getMessage());
//                                continue;
//                            }
//
//                            TableEntity tableEntity = new TableEntity();
//                            tableEntity.setTableName(tableName);
//                            // 设置表注释
//                            tableEntity.setTableComment(tableCommentsMap.get(tableName));
//
//                            // 获取索引信息
//                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection, schemaName, tableName);
//
//                            Map<String, ColumnEntity> columnEntities = new HashMap<>(16);
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
//
//                                    // 设置字段注释
//                                    String columnComment = columnCommentsMap.get(columnName);
//                                    columnEntity.setColumnComment(columnComment);
//
//                                    // 判断是否是索引
//                                    if (indexInfo.containsKey(columnName)) {
//                                        columnEntity.setIsIndex(1);
//                                        Map<String, Object> indexDetails = indexInfo.get(columnName);
//                                        columnEntity.setIsPrimaryKey("YES".equals(indexDetails.get("IS_PRIMARY_KEY")) ? 1 : 0);
//                                    }
//
//                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
//                                }
//                            } catch (Exception e) {
//                                log.error("获取表 {} 的列信息失败: {}", tableName, e.getMessage());
//                                // 抛出无法检索数据库元数据的异常
////                                throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
//                                continue;
//                            }
//
//                            tableEntity.setColumns(columnEntities);
//                            tableEntities.put(tableName, tableEntity);
//                        }
//                    }
//                    databaseEntity.setTables(tableEntities);
//                    databaseEntities.add(databaseEntity);
//                }
//            } catch (SQLException e) {
//                log.error("获取目录信息失败: {}", e.getMessage(), e);
//                throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
//            }
//        } catch (SQLException e) {
//            log.error("建立或获取连接失败: {}", e.getMessage(), e);
//            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
//        }
//        return databaseEntities;
//    }


    /**
     * 获取索引信息。
     * SELECT INDEX_NAME,INDEX_TYPE,TABLE_OWNER,TABLE_NAME,UNIQUENESS
     * FROM dba_indexes
     * WHERE table_owner = 'JMW' AND table_name = 'TEST';
     *
     * @param connection 数据库连接
     * @param dbName     数据库名称
     * @param tableName  表名称
     * @return 索引信息的映射
     */
    @Override
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection, String dbName, String tableName) {
        Map<String, Map<String, Object>> hashMap = new HashMap<>(16);

        // 确保大小写一致
        String schemaName = dbName != null ? dbName.toUpperCase() : dbName;
        String tableNameUpper = tableName != null ? tableName.toUpperCase() : tableName;

        // 使用 ALL_ 视图，降低权限要求
        String sql = "SELECT i.index_name, i.index_type, i.table_owner, i.table_name, i.uniqueness, c.column_name, " +
                "CASE WHEN pk.index_name IS NOT NULL THEN 'Yes' ELSE 'No' END AS is_primary_key " +
                "FROM all_indexes i " +
                "JOIN all_ind_columns c ON i.index_name = c.index_name " +
                "LEFT JOIN all_constraints pk ON i.table_owner = pk.owner " +
                "    AND i.table_name = pk.table_name " +
                "    AND i.index_name = pk.index_name " +
                "    AND pk.constraint_type = 'P' " +
                "WHERE i.table_owner = ? " +
                "  AND i.table_name = ? " +
                "ORDER BY i.index_name, c.column_position";

        log.info("查询索引信息，SQL: {}, dbName: {}, tableName: {}", sql, schemaName, tableNameUpper);

        try (PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            statement.setString(1, schemaName);
            statement.setString(2, tableNameUpper);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>(16);
                    String columnName = resultSet.getString("COLUMN_NAME");
                    map.put("INDEX_NAME", resultSet.getString("INDEX_NAME"));
                    map.put("TABLE_OWNER", resultSet.getString("TABLE_OWNER"));
                    map.put("TABLE_NAME", resultSet.getString("TABLE_NAME"));
                    map.put("UNIQUENESS", resultSet.getString("UNIQUENESS"));
                    map.put("IS_PRIMARY_KEY", resultSet.getString("IS_PRIMARY_KEY"));
                    hashMap.put(columnName, map);
                }
            }
        } catch (SQLException e) {
            log.error("获取索引信息失败，dbName: {}, tableName: {}, 错误: {}", schemaName, tableNameUpper, e.getMessage(), e);
            throw new RuntimeException("获取索引信息失败: " + e.getMessage(), e);
        }

        return hashMap;
    }

    /**
     * 判断输入的字符串名称是什么类型的数据字段。
     *
     * @param columnType 列的数据类型
     * @return 返回字段类型：1-字符串，2-数字，0-时间，3-其他
     */
    public int getColumnType(String columnType) {
        return switch (columnType.toUpperCase()) {
            case "CHAR", "VARCHAR", "CHARACTER", "VARCHAR2" ->
                // 字符串类型
                    1;
            case "BIGINT", "INT", "LARGEINT", "NUMERIC", "DECIMAL", "NUMBER", "INTEGER" ->
                // 数字类型
                    2;
            case "DATE", "INTERVAL", "TIMESTAMP" ->
                // 时间类型
                    0;
            default ->
                // 其他类型
                    3;
        };
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
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
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
        String sql = sqlQueryBuilder.buildSQL().replace("`", "").replaceAll("^\\((.*)\\)$", "$1");

        // 构建查询总数的 SQL
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");

        // 如果 sampleResult 存在，则设置抽样时间
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleTime(LocalDateTime.now()));

        try {
            // 执行查询获取总数
            List<Map<String, Object>> countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : Long.parseLong(countResult.getFirst().get("count(1)").toString());

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

            int orderByIndex = sql.indexOf("order by");
            String randomSampling = " WHERE ROWNUM <= " + m;

            if (orderByIndex != -1) {
                // 在 ORDER BY 之前插入 WHERE ROWNUM <= m
                sql = sql.substring(0, orderByIndex) + randomSampling + " " + sql.substring(orderByIndex);
            } else {
                // 如果没有 ORDER BY，直接在末尾添加 WHERE ROWNUM <= m
                sql = sql + randomSampling;
            }

//            // 使用随机偏移实现近似随机抽样 TODO 需要ORACLE大数据量验证抽样功能
//            Random random = new Random();
//            long offset = random.nextLong() % (totalCount - m + 1); // 随机偏移量
//            if (offset < 0) offset = 0;
//            String randomSql = "SELECT * FROM (SELECT a.*, ROWNUM rn FROM (" + sql + ") a WHERE ROWNUM <= " + (offset + m) + ") WHERE rn > " + offset;

            list.add(sql);
            return list;
        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            throw new RuntimeException("获取总记录数失败", e);
        }
    }
}