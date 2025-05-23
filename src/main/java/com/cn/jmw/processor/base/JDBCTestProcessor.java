package com.cn.jmw.processor.base;

import com.cn.jmw.processor.BaseProcessor;
import com.cn.jmw.processor.datasource.DatabaseAdapter;
import com.cn.jmw.processor.datasource.AbstractJdbcAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.enums.DatabaseTypeEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;

import java.sql.Connection;
import java.sql.DriverManager;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.INPUT_GET_DB_TYPE_IS_NULL;
import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.INPUT_IS_NULL;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * JDBC 测试处理器
 * <p>
 *     通过数据库连接字符串测试数据库连接是否正常
 *     连接成功返回 true，否则返回 false
 *     通过 {@link DriverManager} 获取数据库连接
 *     通过 {@link Connection#isClosed()} 判断连接是否关闭
 *     通过 {@link Connection#close()} 关闭连接
 * </p>
 *
 * @author Jmwang
 */
public class JdbcTestProcessor extends BaseProcessor<JdbcConnectionEntity, Boolean> {

    /**
     * 测试数据库连接是否正常。
     *
     * @param input 数据库连接实体
     * @param data  附加数据（未使用）
     * @return true 如果连接成功，false 如果连接失败
     * @throws Exception 如果输入或数据库类型为空
     */
    @Override
    public Boolean process(JdbcConnectionEntity input, Object... data) throws Exception {
        if (input == null) {
            throw exception(INPUT_IS_NULL);
        }
        if (input.getDbType() == null) {
            throw exception(INPUT_GET_DB_TYPE_IS_NULL);
        }

        input.setTest(true);
        DatabaseEnum dbType = input.getDbType();
        if (dbType.getDatabaseCategory().equals(DatabaseTypeEnum.SQL.getDatabaseCategory())) {
            try {
                AbstractJdbcAdapter sqlAdapter = DatabaseAdapterFactory.getSqlAdapter(input);
                return sqlAdapter.testConnection();
            } catch (Exception e) {
                return false;
            }
        } else {
            DatabaseAdapter adapter = DatabaseAdapterFactory.getNoSqlAdapter(input);
            return adapter.testConnection();
        }
    }
}