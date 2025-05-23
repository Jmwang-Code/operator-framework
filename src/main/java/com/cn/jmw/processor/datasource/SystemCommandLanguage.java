package com.cn.jmw.processor.datasource;

import com.cn.jmw.processor.datasource.pojo.ShowTableStatusResult;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * 系统命令语言
 *
 * @author Jmwang
 */
public interface SystemCommandLanguage {

    /**
     * 查看指定表的状态
     *
     * @param dbName 表名
     * @return 表状态
     */
    Map<String, ShowTableStatusResult> showTableStatus(String dbName);

    /**
     * 查看指定表对应索引的状态
     *
     * @param tableName 表名
     * @param indexName 索引名
     * @return 表状态
     */
    Map<String, ShowTableStatusResult> showWideTableStatus(String tableName,String indexName);

    /**
     * <p>资源管理 数据表阈值判定清理</p>
     *
     * 定时任务达到阈值的时候 ，doris 表数据 批量删除
     * 计算当前阈值 比如百分之95 ，定时任务会降低掉百分之90
     * 大概计算每条数据的大概大小
     * 1. 找到最早时间
     * 2. 通过最早时间分割 （天时分）积累时间段
     * 3. 积累起来的时间段后，判断是否超过阈值
     * 4. 接近百分之90就删除 这个时间段之前的数据
     *
     * @param tableNames 表名称
     * @param limitSizes 限制每张表最大多少GB
     * @param sortTimeFields 排序字段
     * @param threshold  阈值
     * @return 成功失败
     */
    boolean dataTableThresholdDeterminationCleaning(List<String> tableNames, List<Double> limitSizes, List<String> sortTimeFields, double threshold);//,List<Integer> curSizes,List<Long> rowsCounts

    /**
     * 计算当前数据库中最早和最迟进入库中的数据天数差
     *
     * @param tableName 表名称
     * @param sortTimeField 排序字段
     * @return 最早和最迟进入库中的数据天数差
     */
    int getEarliestAndLatestDays(String tableName,String sortTimeField);

    /**
     * 获取索引
     *
     * @param connection 数据库连接
     * @param dbName 数据库
     * @param tableName 数据表
     * @return 索引
     */
    Map<String,Map<String, Object>> getIndexInfo(Connection connection, String dbName, String tableName);

    /**
     * 获取主键
     *
     * @param dbName 数据库
     * @param tableName 数据表
     * @return 主键
     */
    List<Map<String, Object>> getPrimaryKeyInfo(String dbName,String tableName);

    /**
     *  获取验证语句
     *
     * @return 验证语句
     */
    String getValidationQuery();
}