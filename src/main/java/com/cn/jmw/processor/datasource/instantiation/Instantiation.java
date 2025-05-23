package com.cn.jmw.processor.datasource.instantiation;

import java.sql.SQLException;

/**
 * 数据库表实例化接口
 * <p>
 * 该接口定义了根据数据库名称和表名称动态实例化对象的方法，适用于从数据库元数据生成实例的场景。
 * </p>
 *
 * @author Jmwang
 */
public interface Instantiation {

    /**
     * 根据数据库名称和表名称实例化对象。
     *
     * @param databaseName 数据库名称，不能为空
     * @param tableName    表名称，不能为空
     * @return 实例化的对象
     * @throws SQLException 如果数据库操作失败或元数据无效
     * @throws IllegalArgumentException 如果 databaseName 或 tableName 为空
     */
    Object instantiate(String databaseName, String tableName) throws SQLException;
}