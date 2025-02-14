package com.cn.jmw.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class InnerSQL{

    String columnName;

    String columnType;

    String columnComment;
}