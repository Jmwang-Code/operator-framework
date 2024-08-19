package com.cn.jmw.processor.datasource.export;


import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
import com.cn.jmw.processor.datasource.jdbc.adapter.pojo.IntoOutFile;

public interface SynExport {

    /**
     * 同步导出
     */
    public IntoOutFile synExport(String sql,String path, String fileSize, FileTypeEnum fileTypeEnu);

}
