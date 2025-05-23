package com.cn.jmw.processor.base;

import com.cn.jmw.pojo.SqlQueryMontage;
import com.cn.jmw.processor.BaseProcessor;
import com.cn.jmw.processor.datasource.AbstractDatabase;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 算法采样处理器
 * <p>
 * 该处理器用于对输入的 SQL 查询集合执行随机抽样操作，抽取指定数量的记录。
 * 如果数据总量小于抽样数量，则返回全部数据；否则按指定规则进行抽样。
 * </p>
 *
 * @author Jmwang
 */
public class AlgorithmicSamplingProcessor extends BaseProcessor<List<SqlQueryMontage>, List<SqlQueryMontage>> {

    /**
     * 处理输入的 SQL 查询集合，进行随机抽样。
     *
     * @param input 输入的 SQL 查询集合
     * @param data  附加数据，通常包含 JdbcConnectionEntity
     * @return 处理后的 SQL 查询集合
     * @throws Exception 如果数据库操作或配置获取失败
     */
    @Override
    public List<SqlQueryMontage> process(List<SqlQueryMontage> input, Object... data) throws Exception {
        // 检查输入是否为空或空集合
        if (input == null || input.isEmpty()) {
            return new ArrayList<>();
        }

        // 检查附加数据是否有效
        if (data == null || data.length == 0) {
            return Collections.emptyList();
        }

        JdbcConnectionEntity jdbcConnectionEntity = (JdbcConnectionEntity) data[0];
        if (jdbcConnectionEntity == null) {
            return Collections.emptyList();
        }

        JdbcAdapterDataSourceConfig config = jdbcConnectionEntity.getConfig();
        int samplingCount = config.getSamplingCount();

        DatabaseEnum dbType = jdbcConnectionEntity.getDbType();
        AbstractDatabase database = DatabaseAdapterFactory.getDatabase(
                jdbcConnectionEntity,
                dbType != null ? (Class<AbstractDatabase>) dbType.getAdapterClass() : null
        );

        for (SqlQueryMontage sqlQueryMontage : input) {
            List<String> sampledSqlList;
            // 根据附加数据长度选择抽样逻辑（此处逻辑相同，可优化）
            sampledSqlList = database.addRandomSampling(sqlQueryMontage, 10, samplingCount);

            if (sampledSqlList != null && !sampledSqlList.isEmpty()) {
                sqlQueryMontage.setSql(sampledSqlList);
                // 设置抽样方式（假设 SampleResult 存在）
                sqlQueryMontage.getSampleResult().ifPresent(sr -> sr.setSampleMethod("随机抽样"));
            }
        }

        return input;
    }
}