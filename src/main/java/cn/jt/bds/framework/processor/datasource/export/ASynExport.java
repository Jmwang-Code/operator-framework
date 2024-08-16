package cn.jt.bds.framework.processor.datasource.export;

import cn.jt.bds.framework.processor.datasource.jdbc.adapter.pojo.ShowExport;

import java.util.List;

public interface ASynExport {

    /**
     * 异步导出
     */
    void aSynExport(String dbName, String tableName,String where,String path,String columns,String jobId);

    /**
     * 查看当前异步作业状态
     */
    ShowExport queryASynExportJobStatus(String dbName, String jobId);

    /**
     * 查看当前异步作业状态
     */
    List<ShowExport> queryASynExportJobStatus(String dbName);

    /**
     * 中断任务
     */
    void stopASynExportJob(String jobId);

}
