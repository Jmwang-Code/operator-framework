package cn.jt.bds.framework.processor.datasource.export;


import cn.jt.bds.framework.processor.datasource.enums.FileTypeEnum;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.pojo.IntoOutFile;

public interface SynExport {

    /**
     * 同步导出
     */
    public IntoOutFile synExport(String sql,String path, String fileSize, FileTypeEnum fileTypeEnu);

}
