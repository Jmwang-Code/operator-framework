package com.cn.jmw.processor.datasource.jdbc.inter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSetHandler接口定义了处理ResultSet的方法。
 * <p>
 * 该接口提供了处理SQL查询结果的方法。
 * </p>
 *
 * @param <T> 处理结果的类型
 *
 * @author Jmwang
 */
public interface ResultSetHandler<T> {

     /**
      * 处理SQL查询结果。
      *
      * @param resultSet SQL查询结果集
      * @return 返回处理后的结果
      * @throws SQLException 如果处理过程中发生SQL异常
      */
     T handle(ResultSet resultSet) throws SQLException;
}
