package com.cn.jmw.processor.datasource.nosql.adapter;

import com.cn.jmw.processor.datasource.AbstractNoSqlAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.pojo.DatabaseEntity;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_ERROR;
import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_NOT_ALLOW_KEYWORD;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * HiveJDBCAdapter类用于适配Hive数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与Hive相关的数据库操作。
 * </p>
 *
 * @author Jmwang
 */
public class HiveJdbcAdapter extends AbstractNoSqlAdapter {
    /**
     * 构造函数用于创建HiveJDBCAdapter实例。
     *
     * @param hostname     Hive服务器的主机名
     * @param port         Hive服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public HiveJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password) {
        super(hostname, port, databaseName, username, password);
    }

    /**
     * 获取Hive的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:hive2://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true";
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        return List.of();
    }

    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.HIVE;
    }

    /**
     * 添加随机采样
     * <h1>不允许出现 ORDER BY、 LIMIT等字眼</h1>
     *
     * @param sql SQL语句
     * @return 增加随机抽样后的SQL
     */
    @Override
    public String addRandomSampling(String sql,long limit){
        if (StringUtils.isBlank(sql)){
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        Matcher matcher = RANDOM_SAMPLING_COMPILE.matcher(sql);
        if (matcher.find()){
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }

        //SELECT * FROM your_table TABLESAMPLE (2 PERCENT);
        String randomSampling = " ORDER BY RAND() LIMIT "+limit;
        sql = sql + randomSampling;
        return sql;
    }
}