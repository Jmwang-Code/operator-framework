package com.cn.jmw.processor.datasource;

/**
 * Database是一个抽象类，用于定义与数据库相关的基本信息和操作。
 * 该类实现了DatabaseAdapter接口，并为具体数据库适配器提供了通用的构造函数和字段。
 *
 * @author Jmwang
 */
public abstract class AbstractDatabase implements
        // 数据库适配器
        DatabaseAdapter,
        // 其他功能
        DbFunctionality {
    /**
     * 数据库主机名
     */
    protected String hostname;
    /**
     * 数据库端口
     */
    protected Integer port;
    /**
     * 数据库名称
     */
    protected String databaseName;
    /**
     * 数据库用户名
     */
    protected String username;
    /**
     * 数据库密码
     */
    protected String password;

    /**
     * 构造函数用于创建Database实例。
     *
     * @param hostname     数据库主机名
     * @param port         数据库端口
     * @param databaseName 数据库名称
     * @param username     数据库用户名
     * @param password     数据库密码
     */
    public AbstractDatabase(String hostname, Integer port, String databaseName, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
    }

    // ... 其他方法 ...

    /**
     * 获取数据库用户名。
     *
     * @return 数据库用户名
     */
    @Override
    public String getUsername() {
        // 返回用户名
        return this.username;
    }

    /**
     * 获取数据库密码。
     *
     * @return 数据库密码
     */
    @Override
    public String getPassword() {
        // 返回密码
        return this.password;
    }
}