package com.cn.jmw.processor.base;

import com.cn.jmw.processor.BaseProcessor;
import com.cn.jmw.processor.datasource.AbstractNoSqlAdapter;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;

import java.util.List;

/**
 * NoSQL 查询处理器
 * <p>
 * 通过 NoSQL 查询字符串执行查询操作，返回查询结果。
 * </p>
 *
 * @author Jmwang
 */
public class NosqlQueryProcessor extends BaseProcessor<JdbcConnectionEntity, List<?>> {

    /**
     * 执行 NoSQL 查询。
     *
     * @param input 数据库连接实体，包含 NoSQL 查询语句
     * @param data  附加数据（未使用）
     * @return 查询结果列表
     * @throws Exception 如果输入为空或查询失败
     */
    @Override
    public List<?> process(JdbcConnectionEntity input, Object... data) throws Exception {
        if (input == null) {
            throw new IllegalArgumentException("输入参数不能为空");
        }

        AbstractNoSqlAdapter adapter = DatabaseAdapterFactory.getNoSqlAdapter(input);
        return adapter.query(input.getNoSqlQuery());
    }
}