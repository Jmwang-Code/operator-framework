package com.cn.jmw.processor.base;

import com.cn.jmw.processor.BaseProcessor;
import com.cn.jmw.processor.datasource.AbstractJdbcAdapter;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;

import java.util.List;
import java.util.Map;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.INPUT_IS_NULL;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * JDBC 批量查询处理器
 * <p>
 * 通过数据库连接实体执行批量 SQL 查询，返回查询结果。
 * </p>
 *
 * @author Jmwang
 */
public class JdbcQueryBatchProcessor extends BaseProcessor<JdbcConnectionEntity, List<Map<String, Object>>> {

    /**
     * 处理数据库连接实体，执行批量查询。
     *
     * @param input 数据库连接实体，包含 SQL 和参数
     * @param data  附加数据（未使用）
     * @return 查询结果列表，每个元素为字段名到值的映射
     * @throws Exception 如果输入为空或查询失败
     */
    @Override
    public List<Map<String, Object>> process(JdbcConnectionEntity input, Object... data) throws Exception {
        if (input == null) {
            throw exception(INPUT_IS_NULL);
        }

        AbstractJdbcAdapter adapter = DatabaseAdapterFactory.getSqlAdapter(input);
        return adapter.executeDMLC(input.getSql(), input.getParams());
    }
}