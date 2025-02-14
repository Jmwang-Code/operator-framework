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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_ERROR;
import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_NOT_ALLOW_KEYWORD;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * GreenplumJDBCAdapter类用于适配Greenplum数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与Greenplum相关的数据库操作。
 * </p>
 */
public class GreenplumJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建GreenplumJDBCAdapter实例。
     *
     * @param hostname     Greenplum服务器的主机名
     * @param port         Greenplum服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public GreenplumJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) {
        super(hostname, port, databaseName, username, password,config,connectionUser);
    }

    public GreenplumJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password,JDBCAdapterDataSourceConfig config) {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public GreenplumJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password)  {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取Greenplum的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:pivotal:greenplum://" + super.hostname + ":" + super.port + ";DatabaseName=" + super.databaseName
                + ";UseUnicode=true;CharacterEncoding=UTF-8;AutoReconnect=true";
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.GREENPLUM;
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
        if (StringUtils.isBlank(sql)){
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        Matcher matcher = RandomSamplingCompile.matcher(sql);
        if (matcher.find()){
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }

        //获取COUNT总量
        //sql替换FROM之前的变成SELECT COUNT(1)
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");
        //执行COUNT
        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.get(0).get("count(1)");
        //totalCount和(N*M*2)的百分比
        double percent = totalCount<(N * M * 2)?1.0:(N * M * 2) * 1.0 / totalCount;

        String randomSampling = " TABLESAMPLE SYSTEM "+(percent*100);
        sql = sql + randomSampling;
        list.add(sql);
        return list;
    }
}