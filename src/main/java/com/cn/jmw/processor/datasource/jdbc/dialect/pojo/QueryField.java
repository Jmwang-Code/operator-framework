package com.cn.jmw.processor.datasource.jdbc.dialect.pojo;

import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLFunctionEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * QueryField类用于表示SQL查询中的字段信息，包括字段名、别名和函数类型。
 * <p>
 * 此类支持为字段指定别名和函数类型，用于在查询构建过程中提供更多信息。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryField {
    // 字段名
    private String[] field;
    // 字段别名
    private String fieldAlias;
    // 函数类型，可以是Doris支持的任何函数
    private SQLFunctionEnum function;
    // 当前字段的表名
    private String tableName;

    /**
     * 构造一个QueryField实例，仅使用字段名。
     *
     * @param field 字段名
     */
    public QueryField(String field) {
        this.field = new String[]{field};
    }

    public QueryField(String field,String tableName) {
        this.field = new String[]{field};
        this.tableName = tableName;
    }

    public QueryField(String field, String fieldAlias,String tableName){
        this.field = new String[]{field};
        this.fieldAlias = fieldAlias;
        this.tableName = tableName;
    }

    /**
     * 构造一个QueryField实例，使用字段名和字段别名。
     *
     * @param field      字段名
     * @param fieldAlias 字段别名
     */
    public QueryField(String[] field, String fieldAlias,SQLFunctionEnum function) {
        this.field = field;
        this.fieldAlias = fieldAlias;
        this.function = function;
    }

    /**
     * 构造一个QueryField实例，使用字段名和字段别名。
     *
     * @param field      字段名
     * @param fieldAlias 字段别名
     */
    public QueryField(String field, String fieldAlias,SQLFunctionEnum function) {
        this.field = new String[]{field};
        this.fieldAlias = fieldAlias;
        this.function = function;
    }

    /**
     * 将QueryField转换为jOOQ的Field对象，并根据表别名生成合格字段。
     *
     * @param tableAlias 表别名
     * @return jOOQ的Field对象，表示此字段
     */
    public Field<?> toField(String tableAlias) {
        String qualifiedField;
        if (tableAlias != null && !tableAlias.isEmpty()) {
            qualifiedField = "`" + tableAlias + "`.`" + field[0] + "`"; // 使用表别名构建合格字段
        } else {
            qualifiedField = "`" + field[0] + "`"; // 仅使用字段名
        }
        if (fieldAlias != null && !fieldAlias.isEmpty()) {
            return DSL.field(qualifiedField).as(fieldAlias); // 返回带别名的字段
        } else {
            return DSL.field(qualifiedField); // 返回无别名的字段
        }
    }

    public String getFieldString(){
        //通过,分割拼接所有String[]
        return String.join(",", field);
    }
}