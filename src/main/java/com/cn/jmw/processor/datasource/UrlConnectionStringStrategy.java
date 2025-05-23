package com.cn.jmw.processor.datasource;

import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;

/**
 * 使用传入 URL 的连接字符串策略
 */
public class UrlConnectionStringStrategy implements ConnectionStringStrategy {
    private final String url;

    public UrlConnectionStringStrategy(String url) {
        this.url = url;
    }

    @Override
    public String getConnectionString(String hostname, Integer port, String databaseName, JdbcAdapterDataSourceConfig config) {
        return url;
    }
}