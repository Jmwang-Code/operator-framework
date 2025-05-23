package com.cn.jmw.processor.datasource.enums;

/**
 * 数据源池类型枚举
 * <p>
 * 该枚举定义了常见的数据源池类型及其描述，用于标识和管理数据库连接池。
 * </p>
 *
 * @author Jmwang
 */
public enum DataSourcePool {
    /** Druid 数据源池 */
    DRUID("Druid 数据源池"),
    /** HikariCP 数据源池 */
    HIKARICP("HikariCP 数据源池"),
    /** C3P0 数据源池 */
    C3P0("C3P0 数据源池"),
    /** DBCP 数据源池 */
    DBCP("DBCP 数据源池"),
    /** Tomcat JDBC 数据源池 */
    TOMCAT_JDBC_POOL("Tomcat JDBC 数据源池");

    private final String description;

    /**
     * 构造函数，用于初始化数据源池枚举。
     *
     * @param description 数据源池的描述
     */
    DataSourcePool(String description) {
        this.description = description;
    }

    /**
     * 获取数据源池的描述。
     *
     * @return 数据源池描述
     */
    public String getDescription() {
        return description;
    }

}