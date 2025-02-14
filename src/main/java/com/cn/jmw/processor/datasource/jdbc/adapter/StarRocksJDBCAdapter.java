package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.pojo.SQLQueryMontage;
import com.cn.jmw.pojo.SampleResult;
import com.cn.jmw.processor.datasource.JDBCAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.JDBCAdapterDataSourceConfig;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_ERROR;
import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_NOT_ALLOW_KEYWORD;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * StarRocksJDBCAdapter类用于适配StarRocks数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与StarRocks相关的数据库操作。
 * </p>
 */
public class StarRocksJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建StarRocksJDBCAdapter实例。
     *
     * @param hostname     StarRocks服务器的主机名
     * @param port         StarRocks服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public StarRocksJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public StarRocksJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public StarRocksJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取StarRocks的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:mysql://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true";
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.STARROCKS;
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