package com.cn.jmw.processor.datasource;

import com.cn.jmw.processor.datasource.pojo.ShowCreateTable;

/**
 * 基础接口，定义SQL命令
 *
 * @author Jmwang
 */
public interface SqlCommand {

    /**
     * 获取建表DDL命令
     *
     * @param dbName 数据库
     * @param table 表
     * @return 建表DDL
     */
    ShowCreateTable getCreateTableDDL(String dbName, String table);
}
