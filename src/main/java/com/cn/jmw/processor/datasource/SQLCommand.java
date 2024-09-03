package com.cn.jmw.processor.datasource;

import com.cn.jmw.processor.datasource.pojo.ShowCreateTable;

/**
 * 基础接口，定义SQL命令
 */
public interface SQLCommand {

    /**
     * 获取建表DDL命令
     */
    ShowCreateTable getCreateTableDDL(String dbName, String table);
}
