package com.cn.jmw.processor.base;

import com.cn.jmw.processor.BaseProcessor;
import com.cn.jmw.processor.datasource.DatabaseAdapter;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.INPUT_IS_NULL;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * JDBC 数据库版本处理器
 * <p>
 * 通过数据库连接字符串获取数据库版本信息。
 * </p>
 *
 * @author Jmwang
 */
public class JdbcDatabaseVersion extends BaseProcessor<JdbcConnectionEntity, String> {

    /**
     * 处理数据库连接实体，获取数据库版本。
     *
     * @param input 数据库连接实体
     * @param data  附加数据（未使用）
     * @return 数据库版本字符串
     * @throws Exception 如果输入为空或获取版本失败
     */
    @Override
    public String process(JdbcConnectionEntity input, Object... data) throws Exception {
        if (input == null) {
            throw exception(INPUT_IS_NULL);
        }

        DatabaseAdapter adapter = DatabaseAdapterFactory.getSqlAdapter(input);
        return adapter.getDatabaseVersion();
    }
}