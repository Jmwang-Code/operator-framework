package com.cn.jmw.processor.datasource.jdbc.dialect.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author Jmwang
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class QuerySqlResult {

    private String sql;
    /**
     * 当前层以及子层的占位符数据集（全部，包含当前层的占位符数据集）
     */
    private List<Object> dataList;

    @Override
    public String toString() {
        return "QuerySQLResult{" +
                "sql='" + sql + '\'' +
                ", list=" + dataList +
                '}';
    }
}
