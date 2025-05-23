package com.cn.jmw.processor.datasource.jdbc.dialect.pojo;


import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jmwang
 */
public class QueryConditionBuilder {
    private final List<com.cn.jmw.processor.datasource.jdbc.dialect.pojo.QueryCondition> conditions = new ArrayList<>();
    private SQLOperatorEnum operator;

    public QueryConditionBuilder setOperator(SQLOperatorEnum operator) {
        this.operator = operator;
        return this;
    }

    public QueryConditionBuilder addCondition(String field, SQLOperatorEnum operator) {
        conditions.add(new com.cn.jmw.processor.datasource.jdbc.dialect.pojo.QueryCondition(field, operator));
        return this;
    }

    public QueryConditionBuilder addCondition(String field, SQLOperatorEnum operator, Object value) {
        conditions.add(new com.cn.jmw.processor.datasource.jdbc.dialect.pojo.QueryCondition(field, operator, value));
        return this;
    }

    public QueryConditionBuilder addNestedCondition(QueryConditionBuilder nestedBuilder) {
        conditions.add(new com.cn.jmw.processor.datasource.jdbc.dialect.pojo.QueryCondition(conditions, operator));
        return this;
    }

    public com.cn.jmw.processor.datasource.jdbc.dialect.pojo.QueryCondition build() {
        return new com.cn.jmw.processor.datasource.jdbc.dialect.pojo.QueryCondition(conditions, operator);
    }
}