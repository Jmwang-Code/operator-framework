package cn.jt.bds.framework.processor.datasource.jdbc.dialect.pojo;


import cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryConditionBuilder {
    private List<QueryCondition> conditions = new ArrayList<>();
    private SQLOperatorEnum operator;

    public QueryConditionBuilder setOperator(SQLOperatorEnum operator) {
        this.operator = operator;
        return this;
    }

    public QueryConditionBuilder addCondition(String field, SQLOperatorEnum operator, Object value) {
        conditions.add(new QueryCondition(field, operator, value));
        return this;
    }

    public QueryConditionBuilder addNestedCondition(QueryConditionBuilder nestedBuilder) {
        conditions.add(new QueryCondition(conditions, operator));
        return this;
    }

    public QueryCondition build() {
        return new QueryCondition(conditions, operator);
    }
}