package com.cn.jmw.processor.datasource;

import com.cn.jmw.processor.datasource.pojo.JDBCAdapterDataSourceConfig;
import com.zaxxer.hikari.HikariConfig;

public abstract class JDBCDataSourceConfig extends Database{

    /**
     * 最大活跃连接数
     */
    protected JDBCAdapterDataSourceConfig config;

    /**
     * 连接者（模块）
     */
    protected String connectionUser;

    /**
     * 构造函数用于创建Database实例。
     *
     * @param hostname     数据库主机名
     * @param port         数据库端口
     * @param databaseName 数据库名称
     * @param username     数据库用户名
     * @param password     数据库密码
     */
    public JDBCDataSourceConfig(String hostname, Integer port, String databaseName, String username, String password) {
        super(hostname, port, databaseName, username, password);
    }


}
