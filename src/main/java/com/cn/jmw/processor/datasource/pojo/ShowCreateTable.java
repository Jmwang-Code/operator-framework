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
public class ShowCreateTable {

    private String table;

    private String createTable;
}
