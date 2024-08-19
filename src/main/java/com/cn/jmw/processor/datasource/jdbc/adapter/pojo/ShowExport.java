package com.cn.jmw.processor.datasource.jdbc.adapter.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * SHOW EXPORT 查看异步导出的命令
 * 这是一个doris查看导出的命令，用于查看导出进度等基本信息
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShowExport {

    String state;
    String progress;
    String taskInfo;
    String path;
    String createTime;
    String startTime;
    String finishTime;
    String timeout;
    String errorMsg;
    String outfileInfo;
}
