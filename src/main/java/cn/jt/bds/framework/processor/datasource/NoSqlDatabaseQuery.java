package cn.jt.bds.framework.processor.datasource;

import cn.jt.bds.framework.processor.datasource.nosql.query.NoSQLQuery;
import java.util.List;

/**
 * NoSqlDatabaseQuery接口用于定义与NoSQL数据库相关的查询操作。
 * <p>
 * 该接口提供根据NoSQL查询对象执行查询的方法。
 * </p>
 */
public interface NoSqlDatabaseQuery extends DatabaseQuery{
    /**
     * 执行NoSQL查询操作。
     *
     * @param noSQLQuery 要执行的NoSQL查询对象
     * @return 查询结果的列表，具体内容取决于查询的实现
     */
    List query(NoSQLQuery noSQLQuery);
}