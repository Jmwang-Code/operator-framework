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
public class RoutineLoadResult {

    private Long id;
    private String name;
    private String createTime;
    private String pauseTime;
    private String endTime;
    private String dbName;
    private String tableName;
    private Boolean isMultiTable;
    private String state;
    private String dataSourceType;
    private Integer currentTaskNum;
    private String jobProperties;
    private String dataSourceProperties;
    private String customProperties;
    private String statistic;
    private String progress;
    private String lag;
    private String reasonOfStateChanged;
    private String errorLogUrls;
    private String otherMsg;
    private String user;
    private String comment;
}
