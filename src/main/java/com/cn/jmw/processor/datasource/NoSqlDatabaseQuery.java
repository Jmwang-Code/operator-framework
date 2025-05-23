package com.cn.jmw.processor.datasource;

import com.cn.jmw.processor.datasource.nosql.query.NoSqlQuery;
import java.util.List;

/**
 * NoSqlDatabaseQuery接口用于定义与NoSQL数据库相关的查询操作。
 * <p>
 * 该接口提供根据NoSQL查询对象执行查询的方法。
 * </p>
 *
 * @author Jmwang
 */
public interface NoSqlDatabaseQuery extends DatabaseQuery{
    /**
     * 执行NoSQL查询操作。
     *
     * @param noSqlQuery 要执行的NoSQL查询对象
     * @return 查询结果的列表，具体内容取决于查询的实现
     */
    List<?> query(NoSqlQuery noSqlQuery);
}