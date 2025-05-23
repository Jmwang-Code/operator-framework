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
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_ERROR;
import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.RANDOM_SAMPLING_NOT_ALLOW_KEYWORD;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * GreenplumJDBCAdapter类用于适配Greenplum数据库连接。
 * <p>
 * 该类继承自 AbstractJdbcAdapter，提供与 Greenplum 数据库的连接和操作功能，支持高效随机抽样。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class GreenplumJdbcAdapter extends AbstractJdbcAdapter {

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
    public GreenplumJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) {
        super(hostname, port, databaseName, username, password,config,connectionUser,test,strategy);
    }

    public GreenplumJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    /**
//     * 构造函数，使用默认连接用户。
//     */
//    public GreenplumJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) {
//        this(hostname, port, databaseName, username, password, config, null,test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    /**
//     * 构造函数，使用默认配置。
//     */
//    public GreenplumJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test)  {
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
     * 获取Greenplum的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return String.format("jdbc:pivotal:greenplum://%s:%d;DatabaseName=%s;UseUnicode=true;CharacterEncoding=UTF-8;AutoReconnect=true",
                hostname, port, databaseName);
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
     * 添加高效随机抽样逻辑。
     * <p>
     * Greenplum 支持 TABLESAMPLE SYSTEM 实现高效随机抽样。本方法根据数据总数动态计算抽样比例，确保高效性和随机性。
     * </p>
     *
     * @param sqlQueryMontage SQL 查询组合对象，包含查询构建器和抽样结果
     * @param n               抽样点（当前未使用）
     * @param m               抽样数量
     * @return 包含随机抽样逻辑的 SQL 列表
     */
    @Override
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
        Optional<SampleResult> optionalSampleResult = sqlQueryMontage.getSampleResult();
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleMethod("随机"));

        // 初始化返回结果
        List<String> resultList = new ArrayList<>();

        // 获取 SQL 构建器
        SqlQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().getFirst();
        if (sqlQueryBuilder == null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 构建原始 SQL
        String sql = sqlQueryBuilder.buildSQL();

        // 检查 SQL 是否为空
        if (StringUtils.isBlank(sql)) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 检查 SQL 中是否包含不允许的关键字
        Matcher matcher = RANDOM_SAMPLING_COMPILE.matcher(sql);
        if (matcher.find()) {
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }

        // 获取总记录数
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
            double samplePercent = Math.min(1.0, (double) m / totalCount * 100); // 转换为百分比，优化计算
            String randomSampling = String.format(" TABLESAMPLE SYSTEM (%.2f)", samplePercent);
            resultList.add(sql + randomSampling);

        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            throw new RuntimeException("获取总记录数失败", e);
        }

        return resultList;
    }
}