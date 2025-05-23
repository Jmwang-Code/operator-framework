package com.cn.jmw.processor.datasource.pojo;

import lombok.Data;

import java.util.Map;
/**
 * TableEntity类用于表示数据库表的信息。
 * <p>
 * 该类包含表名和与之相关联的列信息列表。
 * </p>
 *
 * @author Jmwang
 */
@Data
public class TableEntity {
    /**
     * 表ID（可能存在）
     */
    private String tableId;
    /**
     * 表名。
     */
    private String tableName;
    /**
     * 表注释
     */
    private String tableComment;
    /**
     * 表级别
     */
    private Integer tableLevel;

    /**
     * 列信息列表，这些列属于该表。
     */
    private Map<String, com.cn.jmw.processor.datasource.pojo.ColumnEntity> columns;


}