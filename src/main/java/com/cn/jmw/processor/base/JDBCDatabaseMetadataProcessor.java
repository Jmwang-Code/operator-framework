package com.cn.jmw.processor.base;

import com.cn.jmw.processor.BaseProcessor;
import com.cn.jmw.processor.datasource.AbstractJdbcAdapter;
import com.cn.jmw.processor.datasource.AbstractNoSqlAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.enums.DatabaseTypeEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.pojo.DatabaseEntity;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;

import java.util.List;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.INPUT_GET_DB_TYPE_IS_NULL;
import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.INPUT_IS_NULL;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;


/**
 * 数据库元数据处理器
 * <p>
 * 通过数据库连接字符串获取数据库元数据，包括数据库名、表名和列名。
 * 支持 SQL 和 NoSQL 数据库类型，根据数据库类型动态选择适配器。
 * </p>
 *
 * @author Jmwang
 */
public class JdbcDatabaseMetadataProcessor extends BaseProcessor<JdbcConnectionEntity, List<DatabaseEntity>> {

    /**
     * 处理数据库连接实体，获取元数据。
     *
     * @param input 数据库连接实体
     * @param data  附加数据（未使用）
     * @return 数据库元数据列表
     * @throws Exception 如果输入为空或数据库类型无效
     */
    @Override
    public List<DatabaseEntity> process(JdbcConnectionEntity input, Object... data) throws Exception {
        if (input == null) {
            throw exception(INPUT_IS_NULL);
        }
        if (input.getDbType() == null) {
            throw exception(INPUT_GET_DB_TYPE_IS_NULL);
        }

        DatabaseEnum dbType = input.getDbType();
        if (dbType.getDatabaseCategory().equals(DatabaseTypeEnum.SQL.getDatabaseCategory())) {
            AbstractJdbcAdapter sqlAdapter = DatabaseAdapterFactory.getSqlAdapter(input);
            return sqlAdapter.getDatabaseMetadata();
        } else {
            AbstractNoSqlAdapter noSqlAdapter = DatabaseAdapterFactory.getNoSqlAdapter(input);
            return noSqlAdapter.getDatabaseMetadata();
        }
    }
}