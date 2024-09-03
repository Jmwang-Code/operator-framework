package com.cn.jmw.processor.datasource.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShowTableStatusResult {

    private String name;
    private String engine;
    private String version;
    private String rowFormat;
    private Long rows;
    private Long avgRowLength;
    private Long dataLength;
    private Long maxDataLength;
    private Long indexLength;
    private Long dataFree;
    private Long autoIncrement;
    private String createTime;
    private String updateTime;
    private String checkTime;
    private String collation;
    private String checksum;
    private String createOptions;
    private String comment;

}
