package com.cn.jmw.processor.datasource.enums;

public enum FileTypeEnum {

    ORC("ORC"),
    CSV_WITH_NAMES("CSV_WITH_NAMES"),
    PARQUET("PARQUET"),

    ;
    private String name;

    FileTypeEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static FileTypeEnum getByName(String name) {
        String upperCase = name.toUpperCase();
        for (FileTypeEnum fileTypeEnum : FileTypeEnum.values()) {
            if (fileTypeEnum.getName().equals(upperCase) || fileTypeEnum.getName().startsWith(upperCase)) {
                return fileTypeEnum;
            }
        }
        return null;
    }
}
