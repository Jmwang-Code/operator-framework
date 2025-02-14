package com.cn.jmw.processor.datasource.jdbc.dialect.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class QuerySQLResult {

    private String sql;
//    临时获取到当前层的占位符数据集
//    private Object[] data;
    //当前层以及子层的占位符数据集（全部，包含当前层的占位符数据集）
    private List<Object> dataList;

    @Override
    public String toString() {
        return "QuerySQLResult{" +
                "sql='" + sql + '\'' +
                ", list=" + dataList +
                '}';
    }
}
