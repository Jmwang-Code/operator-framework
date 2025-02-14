package com.cn.jmw.processor.datasource.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ColumnEntity类用于表示数据库表中的列信息。
 * <p>
 * 该类包含列名和列类型的基本信息。
 * </p>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnEntity {
    /**
     * 列名。
     */
    private String columnName;
    /**
     * 列类型。
     */
    private String columnType;
    /**
     * 字段注释
     */
    private String columnComment;
    /**
     * 列大小
     */
    private int columnSize;
    /**
     * 是否为空
     */
    private boolean nullable;
    /**
     * 默认值
     */
    private String defaultValue;
    /**
     * 是否是索引 1是 0不是
     */
    private int isIndex;
    /**
     * 是否为自增
     */
    private int autoIncrement;
    /**
     * 是否是主键
     */
    private int isPrimaryKey;
    /**
     * 0时间类型  1字符串类型 2数字类型 3未知格式
     */
    private int Type;
}