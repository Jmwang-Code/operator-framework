package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.processor.datasource.JDBCAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import java.util.Arrays;
import java.util.List;

/**
 * OceanBaseJDBCAdapter类用于适配OceanBase数据库连接。
 * <p>
 * 该类扩展了JDBCAdapter，提供了与OceanBase相关的数据库操作。
 * </p>
 */
public class OceanBaseJDBCAdapter extends JDBCAdapter {
    /**
     * 构造函数用于创建OceanBaseJDBCAdapter实例。
     *
     * @param hostname     OceanBase服务器的主机名
     * @param port         OceanBase服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public OceanBaseJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password) {
        super(hostname, port, databaseName, username, password);
    }

    /**
     * 获取OceanBase的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:oceanbase://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true";
    }

    /**
     * 获取当前数据库的类型。
     *
     * @return 返回数据库枚举类型
     */
    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.OCEANBASE;
    }

    /**
     * 获取需要忽略的数据库列表。
     *
     * @return 返回一个包含被忽略的数据库名称的列表
     */
    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("information_schema", "mysql", "SYS", "LBACSYS", "ORAAUDITOR", "oceanbase");
    }
}