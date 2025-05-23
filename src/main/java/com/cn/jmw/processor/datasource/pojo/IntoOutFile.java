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
public class IntoOutFile {

    private Integer fileNumber;
    private Integer totalRows;
    private Long fileSize;
    private String url;
    private String errorMsg;
}
