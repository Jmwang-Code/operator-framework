package com.cn.jmw.processor.datasource.jdbc.inter;

import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
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
     * 控制集群管理
     */
}
