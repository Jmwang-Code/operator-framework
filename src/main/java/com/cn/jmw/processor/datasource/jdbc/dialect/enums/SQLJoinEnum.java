package com.cn.jmw.processor.datasource.jdbc.dialect.enums;

public enum SQLJoinEnum {

    //JOIN类型

    JOIN,
    /**
     * 内连接
     */
    INNER_JOIN,
    /**
     * 左连接
     */
    LEFT_JOIN,
    /**
     * 右连接
     */
    RIGHT_JOIN,
    /**
     * 全连接
     */
    FULL_OUTER_JOIN,

}
