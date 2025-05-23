package com.cn.jmw.processor.datasource;

import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;

/**
 * 连接字符串生成策略接口
 */
public interface ConnectionStringStrategy {
    /**
     * 生成数据库连接字符串
     *
     * @param hostname     主机名
     * @param port         端口号
     * @param databaseName 数据库名称
     * @param config       数据源配置
     * @return 连接字符串
     */
    String getConnectionString(String hostname, Integer port, String databaseName, JdbcAdapterDataSourceConfig config);
}