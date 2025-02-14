package com.cn.jmw.processor.datasource.jdbc.inter;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetHandler<T> {
     T handle(ResultSet var1) throws SQLException;
}
