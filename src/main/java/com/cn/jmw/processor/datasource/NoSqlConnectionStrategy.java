package com.cn.jmw.processor.datasource;

/**
 * NoSQL 连接 URI 生成策略接口
 */
public interface NoSqlConnectionStrategy {

    /**
     * 生成 NoSQL 数据库的连接 URI
     *
     * @param hostname     主机名
     * @param port         端口号
     * @param databaseName 数据库名称
     * @return 连接 URI
     */
    String getConnectionUri(String hostname, Integer port, String databaseName);
}
