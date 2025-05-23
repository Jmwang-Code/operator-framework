package com.cn.jmw.processor.datasource.enums;

import com.cn.jmw.annotation.NotRecommended;
import lombok.Getter;

/**
 * 文件类型枚举
 * <p>
 * 该枚举定义了常见文件格式及其名称，用于标识和管理文件类型。
 * </p>
 *
 * @author Jmwang
 */
public enum FileTypeEnum {

    /** ORC 文件格式 */
    @NotRecommended
    ORC("ORC"),
    /** 带列名的 CSV 文件格式 (导出)*/
    CSV("CSV"),
    /** 支持 csv 文件行首过滤 */
    @NotRecommended
    CSV_WITH_NAMES("CSV_WITH_NAMES"),
    /** Parquet 文件格式 */
    @NotRecommended
    PARQUET("PARQUET"),
    /** JSON 文件格式 (导入 导出)*/
    JSON("JSON");

    private final String name;

    /**
     * 构造函数，用于初始化文件类型枚举。
     *
     * @param name 文件类型的名称
     */
    FileTypeEnum(String name) {
        this.name = name;
    }

    /**
     * 获取文件类型的名称。
     *
     * @return 文件类型名称
     */
    public String getName() {
        return name;
    }

    /**
     * 根据名称查找对应的文件类型枚举。
     *
     * @param name 文件类型的名称，忽略大小写
     * @return 匹配的文件类型枚举，如果未找到或 name 为 null 则返回 null
     */
    public static FileTypeEnum getByName(String name) {
        if (name == null) {
            return null;
        }
        String upperCaseName = name.toUpperCase();
        for (FileTypeEnum fileTypeEnum : FileTypeEnum.values()) {
            if (fileTypeEnum.getName().equalsIgnoreCase(upperCaseName)) {
                return fileTypeEnum;
            }
        }
        return null;
    }
}