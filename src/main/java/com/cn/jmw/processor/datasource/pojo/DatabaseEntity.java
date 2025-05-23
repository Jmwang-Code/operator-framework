package com.cn.jmw.processor.datasource.pojo;

import java.util.Map;

import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DatabaseEntity类用于表示数据库的基本信息。
 * <p>
 * 该类包含数据库名称、数据库类型以及该数据库中的表信息。
 * </p>
 *
 * @author Jmwang
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseEntity {
    /**
     * 数据库ID （可能存在）
     */
    private String databaseId;

    /**
     * 数据库类型的枚举值。
     */
    private DatabaseEnum databaseEnum;

    /**
     * 数据库名称。
     */
    private String databaseName;

    /**
     * 数据库中的表信息列表。
     */
    private Map<String,TableEntity> tables;
}