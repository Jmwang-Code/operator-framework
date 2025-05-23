package com.cn.jmw.processor.datasource.jdbc.inter.doris.export;


import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
import com.cn.jmw.processor.datasource.pojo.IntoOutFile;

/**
 * SynExport接口定义了同步导出操作的方法。
 * <p>
 * 该接口提供了同步导出数据的方法。
 * </p>
 *
 * @author Jmwang
 */
public interface SynExport {

    /**
     * 同步导出数据。
     *
     * @param sql        要执行的SQL查询
     * @param path       导出文件路径
     * @param fileSize   导出文件大小
     * @param fileTypeEnu 文件类型枚举
     * @return 返回导出文件的信息
     */
    public IntoOutFile synExport(String sql,String path, String fileSize, FileTypeEnum fileTypeEnu);

}
