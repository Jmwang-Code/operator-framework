package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.pojo.SqlQueryMontage;
import com.cn.jmw.pojo.SampleResult;
import com.cn.jmw.processor.datasource.AbstractJdbcAdapter;
import com.cn.jmw.processor.datasource.ConnectionStringStrategy;
import com.cn.jmw.processor.datasource.GeneratedConnectionStringStrategy;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.SqlQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_ERROR;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * DB2JDBCAdapter类用于适配DB2数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与DB2相关的数据库操作。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class Db2JdbcAdapter extends AbstractJdbcAdapter {

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
    public Db2JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password, config, connectionUser, test,strategy);
    }

    public Db2JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    public Db2JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null, test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    public Db2JdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test) {
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
     * 获取DB2的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return String.format("jdbc:db2://%s:%d/%s:user=%s;password=%s;", hostname, port, databaseName, username, password);
    }

    /**
     * 获取数据库类型。
     *
     * @return 返回数据库类型枚举，表示当前数据库为DB2
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.DB2;
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

        //首先获取总量
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM");

        // 如果 sampleResult 存在，则设置抽样时间
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleTime(LocalDateTime.now()));

        try {
            // 执行查询获取总数
            List<Map<String, Object>> countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.getFirst().get("count(1)");

            // 验证抽样点和前后记录分布一定要小于表的总记录数
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
            String randomSampling = " FETCH FIRST " + m + " ROWS ONLY"; // DB2 使用 FETCH FIRST 替代 LIMIT
            resultList.add(sql + randomSampling);
        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            return resultList;
        }
        return resultList;
    }
}