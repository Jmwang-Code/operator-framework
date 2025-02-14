package com.cn.jmw.processor.datasource.jdbc.adapter;

//import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class DorisConnectionTest {
    // Doris JDBC URL (替换为实际的 FE 地址)
    private static final String JDBC_URL = "jdbc:mysql://192.168.10.202:9030/bds_log";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456aA!@";

    // 存储连接的 Map，Key 可以是线程 ID 或其他唯一标识
    private static final Map<String, Connection> connectionMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
//        // 获取并存储连接
//        for (int i = 0; i < 5; i++) {
//            String connectionKey = "connection-" + i; // 使用唯一标识
//            Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
//            connectionMap.put(connectionKey, connection);
//
//            // 模拟查询
//            executeQuery(connection, i);
//        }
//
//        // 检查 Doris 的连接状态，此时连接仍然存活
//        System.out.println("存储在Map中的连接，检查Doris进程列表...");
//
//        // 关闭所有连接
//        closeAllConnections();
//
//        System.out.println("所有连接均已关闭，请再次检查Doris进程列表...");

//        testWithDruid();

//        testWithJdbc();
    }

    // 执行查询的方法
    private static void executeQuery(Connection connection, int queryId) throws Exception {
        String query = "SELECT COUNT(*) FROM bds_asset_info"; // 替换为实际表名
        try (PreparedStatement statement = connection.prepareStatement(query);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                System.out.println("Query " + queryId + " Result: " + resultSet.getInt(1));
            }
        }
    }

    // 关闭所有连接
    private static void closeAllConnections() throws InterruptedException {
        for (Map.Entry<String, Connection> entry : connectionMap.entrySet()) {
            try {
                Connection connection = entry.getValue();
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                    System.out.println("Closed connection: " + entry.getKey());
                }
            } catch (Exception e) {
                System.err.println("Error closing connection: " + entry.getKey() + " - " + e.getMessage());
            }
        }

        // 清空 Map
        connectionMap.clear();
        Thread.sleep(100000);
    }

    // 测试 Druid 连接池
//    private static void testWithDruid() throws Exception {
//        // 配置 Druid 数据源
//        DruidDataSource dataSource = new DruidDataSource();
//        dataSource.setUrl(JDBC_URL);
//        dataSource.setUsername(USERNAME);
//        dataSource.setPassword(PASSWORD);
//        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
//
//        // 设置连接池参数
//        dataSource.setInitialSize(5);       // 初始化连接数
//        dataSource.setMaxActive(10);       // 最大连接数
//        dataSource.setMinIdle(2);          // 最小空闲连接
//        dataSource.setMaxWait(3000);       // 最大等待时间
//        dataSource.setTestWhileIdle(true); // 空闲时测试连接是否可用
//        dataSource.setValidationQuery("SELECT 1");
//
//        for (int i = 0; i < 20; i++) {
//            try (Connection connection = dataSource.getConnection();
//                     PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM bds_asset_info")) {
//                ResultSet resultSet = statement.executeQuery();
//                while (resultSet.next()) {
//                    System.out.println("Druid Query Result: " + resultSet.getInt(1));
//                }
//            }
//            Thread.sleep(500); // 模拟间隔
//        }
//
//        // 关闭连接池
//        dataSource.close();
//        Thread.sleep(100000);
//    }

    // 测试普通 JDBC 连接
    private static void testWithJdbc() throws Exception {
        for (int i = 0; i < 20; i++) {
            try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
                 PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM bds_asset_info")) {
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    System.out.println("JDBC Query Result: " + resultSet.getInt(1));
                }
            }
            Thread.sleep(500); // 模拟间隔
        }
        Thread.sleep(100000);
    }
}
