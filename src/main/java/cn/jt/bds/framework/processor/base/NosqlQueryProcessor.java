package cn.jt.bds.framework.processor.base;

import cn.jt.bds.framework.processor.BaseProcessor;
import cn.jt.bds.framework.processor.datasource.NoSqlAdapter;
import cn.jt.bds.framework.processor.datasource.factory.DatabaseAdapterFactory;
import cn.jt.bds.framework.processor.datasource.pojo.JDBCConnectionEntity;

import java.util.List;

/**
 * Nosql查询处理器
 */
public class NosqlQueryProcessor extends BaseProcessor<JDBCConnectionEntity, List> {
    @Override
    public List process(JDBCConnectionEntity input, Object... data) throws Exception {
        NoSqlAdapter adapterNosql = DatabaseAdapterFactory.getNoSQLAdapter(input);
        return adapterNosql.query(input.getNoSQLQuery());
    }
}
