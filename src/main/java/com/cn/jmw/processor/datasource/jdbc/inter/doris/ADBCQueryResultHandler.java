package com.cn.jmw.processor.datasource.jdbc.inter.doris;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface ADBCQueryResultHandler<T>  {

    T handle(List<Map<String, Object>> batch) throws Exception;
}
