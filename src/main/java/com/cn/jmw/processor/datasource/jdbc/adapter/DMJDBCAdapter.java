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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * DMJDBCAdapter类用于适配DM数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与DM相关的数据库操作，包括获取数据库元数据。
 * </p>
 */
public class DMJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建DMJDBCAdapter实例。
     *
     * @param hostname     DM服务器的主机名
     * @param port         DM服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public DMJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public DMJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public DMJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取DM数据库的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:dm://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true";
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
        try (Connection connection = pool.getConnection(hostname + port + databaseName,config,connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet catalogs = metaData.getSchemas();
            while (catalogs.next()) {
                String schemaName = catalogs.getString("TABLE_SCHEM");
                // 跳过在忽略列表中的数据库
                if (ignoreDatabases.contains(schemaName)) {
                    continue;
                }
                if (StringUtils.isNotBlank(dbName) && !schemaName.equals(dbName)) {
                    continue;
                }
                // 创建一个新的DatabaseEntity对象，并添加到databaseEntities列表中
                DatabaseEntity databaseEntity = new DatabaseEntity();
                databaseEntity.setDatabaseName(schemaName);
                databaseEntity.setDatabaseEnum(getDatabaseType());
                Map<String, TableEntity> tableEntities = new HashMap<>();

                // 获取所有表的信息
                ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"});
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");

                    //TODO 查索引
                    Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection,schemaName, tableName);

                    // 创建一个新的TableEntity对象
                    TableEntity tableEntity = new TableEntity();
                    tableEntity.setTableName(tableName);
                    tableEntity.setTableComment(tables.getString("REMARKS")); // 获取表注释
                    Map<String, ColumnEntity> columnEntities = new HashMap<>();

                    // 获取列的信息
                    ResultSet columns = metaData.getColumns(null, schemaName, tableName, "%");
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        String columnType = columns.getString("TYPE_NAME");
                        // 创建一个新的ColumnEntity对象，并添加到columnEntities列表中
                        ColumnEntity columnEntity = new ColumnEntity();
                        columnEntity.setColumnName(columnName);
                        columnEntity.setColumnType(columnType);
                        columnEntity.setColumnComment(columns.getString("REMARKS")); // 获取列注释

                        // 判断是什么类型的TYPE_NAME
                        columnEntity.setType(getColumnType(columnType));

                        //TODO 是否是索引
                        if (indexInfo.containsKey(columnName)) {
                            //是不是索引
                            columnEntity.setIsIndex(1);
                            Map<String, Object> indexDetails = indexInfo.get(columnEntity.getColumnName());
                            //设置主键
                            if ("YES".equals(indexDetails.get("IS_PRIMARY_KEY"))) {
                                columnEntity.setIsPrimaryKey(1); // 设置为主键
                            } else {
                                columnEntity.setIsPrimaryKey(0); // 设置为主键
                            }
                        }

                        columnEntities.put(columnName, columnEntity);
                    }
                    tableEntity.setColumns(columnEntities);
                    tableEntities.put(tableName, tableEntity);
                }
                databaseEntity.setTables(tableEntities);
                databaseEntities.add(databaseEntity);
            }
        } catch (SQLException e) {
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
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
                "DATETIME".equals(columnType) ||
                "TIMESTAMP".equals(columnType)) {
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
        Map<String, Map<String, Object>> hashMap = new HashMap<>();
        String sql = "SELECT\n" +
                "\tui.INDEX_NAME,\n" +
                "\tui.TABLE_OWNER,\n" +
                "\tui.TABLE_NAME,\n" +
                "\tui.UNIQUENESS,\n" +
                "\tuic.COLUMN_NAME,\n" +
                "\tucc.CONSTRAINT_NAME AS PRIMARY_KEY_CONSTRAINT,-- 如果该索引是主键，则显示约束名称；否则可能为NULL\n" +
                "CASE\n" +
                "\t\t\n" +
                "\t\tWHEN ucc.CONSTRAINT_TYPE = 'P' THEN\n" +
                "\t\t'YES' ELSE 'NO' \n" +
                "\tEND AS IS_PRIMARY_KEY -- 指示索引是否为主键\n" +
                "\t\n" +
                "FROM\n" +
                "\tUSER_INDEXES ui\n" +
                "\tJOIN USER_IND_COLUMNS uic ON ui.INDEX_NAME = uic.INDEX_NAME\n" +
                "\tLEFT JOIN -- 使用LEFT JOIN来确保即使索引不是主键也能返回索引信息\n" +
                "\tUSER_CONSTRAINTS ucc ON ui.INDEX_NAME = ucc.INDEX_NAME \n" +
                "WHERE\n" +
                "\tui.TABLE_OWNER = '" + dbName + "' \n" +
                "\tAND -- 库名\n" +
                "\tui.TABLE_NAME = '" + tableName + "';-- 替换为您的表名";
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
                map.put("PRIMARY_KEY_CONSTRAINT", resultSet.getString("PRIMARY_KEY_CONSTRAINT"));
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
        if (sqlQueryBuilder == null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        String sql = sqlQueryBuilder.buildSQL().replaceAll("`", "");
        //首先获取总量
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");

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
}