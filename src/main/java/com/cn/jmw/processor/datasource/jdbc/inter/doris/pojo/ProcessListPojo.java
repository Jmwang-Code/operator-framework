package com.cn.jmw.processor.datasource.jdbc.inter.doris.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessListPojo {

    private String currentConnected;
    private Long id;
    private String user;
    private String host;
    private String loginTime;
    private String catalog;
    private String db;
    private String command;
    private Integer time;
    private String state;
    private String queryId;
    private String info;
    private String fe;
    private String cloudCluster;

}
