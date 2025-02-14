package com.cn.jmw.processor.datasource.jdbc.inter.doris;

import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.pojo.ProcessListPojo;
import com.cn.jmw.processor.datasource.pojo.RoutineLoadResult;
import com.cn.jmw.processor.datasource.pojo.ShowPartitionResult;
import com.cn.jmw.processor.datasource.pojo.StreamLoadResult;

import java.io.IOException;
import java.util.List;

/**
 * Doris特性功能 比如StreamLoad、RoutineLoad
 */
public interface Doris {

    /**
     * 流式导入
     *
     * @param data      要加载的 JSON 数据 / CSV 路径 / ORC 路径 / Parquet 路径
     * @param tableName 要将数据加载到其中的表的名称
     * @param columns   列的名称关系
     * @throws IOException 如果发生 IO 错误
     */
    StreamLoadResult streamLoad(String data, String tableName, String columns, FileTypeEnum importFileType) throws IOException;

    /**
     * 创建常规导入
     * <p>
     * Kafka CSV和JSON格式导入
     *
     * @param databaseName        数据库名称
     * @param routineLoadName     加载任务名称（ID、标识）自定义为了找到对应的任务
     * @param targetTableName     目标表名称
     * @param columnsTerminatedBy 列分隔符 (,)
     * @param columns             列映射 (field1,field2,...)
     * @param kafkaBrokerList     Kafka Broker 列表(broker1_ip:9092,broker2_ip:9092)
     * @param kafkaTopic          Kafka 主题(TOPIC)
     * @param kafkaPartitions     Kafka 分区(PARTITION)
     * @param kafkaOffsets        Kafka 偏移量(<h2>OFFSET_BEGINNING</h2> <h2>OFFSET_END</h2> <h2>offset 可以指定从大于等于0</h2> <h2>时间格式，如："2021-05-22 11:00:00"</h2>)
     * @throws IOException 如果发生 IO 错误
     */
    boolean createRoutineLoad(String databaseName, String routineLoadName, String targetTableName,
                              String columnsTerminatedBy, String columns, String kafkaBrokerList,
                              String kafkaTopic, String groupId, String kafkaPartitions,
                              String kafkaOffsets, FileTypeEnum fileTypeEnum);

    /**
     * 查看常规导入列表
     *
     * @param routineLoadName 加载任务名称
     * @return 常规导入列表
     */
    List<RoutineLoadResult> showRoutineLoadFor(String routineLoadName);

    /**
     * 资源管理 数据表阈值判定清理
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
     boolean determineCleaningBasedOnPartitionDataTableThreshold(List<String> dbNames,List<String> tableNames,List<Double> limitSizes,List<String> sortTimeFields,double threshold);

    /**
     * 查看分区表的分区信息
     *
     * 获取到分区表的最早和最迟进入库中的数据天数，以及对应一下PartitionKey字段
     * @param dbName 库名称
     * @param tableName 表名称
     * @param sortTimeField 排序字段(event_time)
     * @return 最早和最迟进入库中的数据天数差
     */
    List<ShowPartitionResult> showPartitions(String dbName, String tableName, String sortTimeField);

    /**
     * doris —— query任务的查询
     */
    String getQueryIDList(String databaseName, String likeSql);

    /**
     * 获取当前数据库中正在运行的进程列表。
     * 
     * 该方法根据提供的 `ProcessListPojo` 对象中的条件构建 SQL 查询，
     * 从 `information_schema.PROCESSLIST` 表中检索与条件匹配的进程信息。
     * 返回一个包含所有匹配进程的 `ProcessListPojo` 对象列表。
     *
     * @param processListPojo 包含查询条件的对象
     * @return List<ProcessListPojo> 匹配的进程列表
     */
    List<ProcessListPojo> getProcessList(ProcessListPojo processListPojo);

    /**
     * 终止指定的查询任务。
     * 
     * 该方法根据提供的查询 ID，强制停止正在执行的查询。 
     * 这在需要取消长时间运行的查询或释放系统资源时非常有用。
     *
     * @param queryId 要终止的查询的唯一标识符,他是一个字符串不是整数
     * @return boolean 只指定成功删除不知道失败原因（只能指定如果是ture说明成功删除了当前的查询任务，如果是false不能确定是什么原因没有删除成功）
     */
    boolean killQuery(String queryId);

    /**
     * 终止指定的数据库连接。
     * 
     * 该方法根据提供的连接 ID，强制关闭与数据库的连接。
     * 这在需要释放资源或清理不再使用的连接时非常有用。
     *
     * @param connectionId 要终止的连接的唯一标识符
     * @return boolean 返回操作是否成功，成功时返回 true，失败时返回 false。
     */
    boolean killConnection(Integer connectionId);
}
