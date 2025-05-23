package cn.jt.bds.framework.processor.datasource.jdbc.dialect.pojo;

import cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLFunctionEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class QueryGroup {

    /**
     * 字段名
     */
    private String field;

    /**
     * 当前字段的表名
     */
    private String tableName;

    /**
     * 构造一个QueryGroup实例，仅使用字段名。
     *
     * @param field 字段名
     */
    public QueryGroup(String field) {
        this.field = field;
    }

    public QueryGroup(String field,String tableName) {
        this.field = field;
        this.tableName = tableName;
    }

}
