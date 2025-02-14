package com.cn.jmw.processor.datasource.jdbc.dialect.pojo;

import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryStringCondition {

    @JsonProperty("condition")
    // 参与查询的表名
    private String condition;

    @JsonProperty("NestedConditions")
    // 参与查询的字段名
    private SQLOperatorEnum operator;
}
