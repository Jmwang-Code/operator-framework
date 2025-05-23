package com.cn.jmw.processor.datasource.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Jmwang
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShowPartitionResult {

    /**
     * TABLE_CATALOG 表格目录
     */
    private String tableCatalog;

    /**
     * TABLE_SCHEMA 表模式
     */
    private String tableSchema;

    /**
     * TABLE_NAME 表名
     */
    private String tableName;

    /**
     * PARTITION_NAME 分区名
     */
    private String partitionName;

    /**
     * SUBPARTITION_NAME 子分区名
     */
    private String subPartitionName;

    /**
     * PARTITION_ORDINAL_POSITION 分区排序
     */
    private Integer partitionOrdinalPosition;

    /**
     * SUBPARTITION_ORDINAL_POSITION 子分区排序
     */
    private Integer subPartitionOrdinalPosition;

    /**
     * PARTITION_METHOD 分区方法
     */
    private String partitionMethod;

    /**
     * SUBPARTITION_METHOD 子分区方法
     */
    private String subPartitionMethod;

    /**
     * PARTITION_EXPRESSION 分区表达式
     */
    private String partitionExpression;

    /**
     * SUBPARTITION_EXPRESSION 子分区表达式
     */
    private String subPartitionExpression;

    /**
     * PARTITION_DESCRIPTION 分区描述
     */
    private String partitionDescription;

    /**
     * TABLE_ROWS 表行数
     */
    private Long tableRows;

    /**
     * AVG_ROW_LENGTH 平均行长度
     */
    private Long avgRowLength;

    /**
     * DATA_LENGTH 数据长度
     */
    private Long dataLength;

    /**
     * MAX_DATA_LENGTH 最大数据长度
     */
    private Long maxDataLength;

    /**
     * INDEX_LENGTH 索引长度
     */
    private Long indexLength;

    /**
     * DATA_FREE 数据空间
     */
    private Long dataFree;

    /**
     * CREATE_TIME 创建时间
     */
    private String createTime;

    /**
     * UPDATE_TIME 更新时间 (我们判断依据就是修改时间)
     */
    private String updateTime;

    /**
     * CHECK_TIME 检查时间
     */
    private String checkTime;

    /**
     * CHECKSUM 校验和
     */
    private Long checksum;

    /**
     * PARTITION_COMMENT 分区描述
     */
    private String partitionComment;

    /**
     * NODEGROUP 节点组
     */
    private String nodeGroup;

    /**
     * TABLESPACE_NAME 表空间
     */
    private String tablespaceName;
}