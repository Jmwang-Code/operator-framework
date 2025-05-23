package com.cn.jmw.processor.datasource.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * StreamLoadResult类用于表示流加载操作的结果信息。
 * <p>
 * 该类包含有关交易ID、标签、状态、已加载行数等信息。
 * </p>
 *
 * @author Jmwang
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StreamLoadResult {
    /**
     * 交易ID。
     */
    @JsonProperty("TxnId")
    private long txnId;

    /**
     * 标签。
     */
    @JsonProperty("Label")
    private String label;

    /**
     * 注释。
     */
    @JsonProperty("Comment")
    private String comment;

    /**
     * 是否两阶段提交。
     */
    @JsonProperty("TwoPhaseCommit")
    private String twoPhaseCommit;

    /**
     * 状态。
     */
    @JsonProperty("Status")
    private String status;

    /**
     * 消息。
     */
    @JsonProperty("Message")
    private String message;

    /**
     * 总行数。
     */
    @JsonProperty("NumberTotalRows")
    private int numberTotalRows;

    /**
     * 已加载行数。
     */
    @JsonProperty("NumberLoadedRows")
    private int numberLoadedRows;

    /**
     * 已过滤行数。
     */
    @JsonProperty("NumberFilteredRows")
    private int numberFilteredRows;

    /**
     * 未选择行数。
     */
    @JsonProperty("NumberUnselectedRows")
    private int numberUnselectedRows;

    /**
     * 加载字节数。
     */
    @JsonProperty("LoadBytes")
    private long loadBytes;

    /**
     * 加载时间（毫秒）。
     */
    @JsonProperty("LoadTimeMs")
    private long loadTimeMs;

    /**
     * 开始事务时间（毫秒）。
     */
    @JsonProperty("BeginTxnTimeMs")
    private long beginTxnTimeMs;

    /**
     * 流加载PUT时间（毫秒）。
     */
    @JsonProperty("StreamLoadPutTimeMs")
    private long streamLoadPutTimeMs;

    /**
     * 读取数据时间（毫秒）。
     */
    @JsonProperty("ReadDataTimeMs")
    private long readDataTimeMs;

    /**
     * 写入数据时间（毫秒）。
     */
    @JsonProperty("WriteDataTimeMs")
    private long writeDataTimeMs;

    /**
     * 提交和发布时间（毫秒）。
     */
    @JsonProperty("CommitAndPublishTimeMs")
    private long commitAndPublishTimeMs;

    /**
     * 错误URL。
     */
    @JsonProperty("ErrorURL")
    private String errorUrl;

    /**
     * 新增资产数量
     */
    @JsonProperty("NewCount")
    private int isNewCount;

    /**
     * 更新资产
     */
    @JsonProperty("IsUpdateCount")
    private int isUpdateCount;

    /**
     * 废弃
     */
    @JsonProperty("IsDeleteCount")
    private int isDeleteCount;

}