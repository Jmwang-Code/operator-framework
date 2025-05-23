package com.cn.jmw.processor.datasource.jdbc.dialect.pojo;

import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Jmwang
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryStringCondition {

    /**
     * 参与查询的表名
     */
    @JsonProperty("condition")
    private String condition;

    /**
     * 参与查询的字段名
     */
    @JsonProperty("NestedConditions")
    private SQLOperatorEnum operator;
}
