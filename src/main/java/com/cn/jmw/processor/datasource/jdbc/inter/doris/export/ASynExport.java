package com.cn.jmw.processor.datasource.jdbc.inter.doris.export;

import com.cn.jmw.processor.datasource.pojo.ShowExport;

import java.util.List;

/**
 * ASynExport接口定义了异步导出操作的方法。
 * <p>
 * 该接口提供了异步导出数据、查询异步作业状态以及中断异步作业的方法。
 * </p>
 *
 * @author Jmwang
 */
public interface ASynExport {

    /**
     * 异步导出数据。
     *
     * @param dbName    数据库名称
     * @param tableName 表名称
     * @param where     查询条件
     * @param path      导出文件路径
     * @param columns   导出列
     * @param jobId     作业ID
     */
    void aSynExport(String dbName, String tableName, String where, String path, String columns, String jobId);

    /**
     * 查看当前异步作业状态。
     *
     * @param dbName 数据库名称
     * @param jobId  作业ID
     * @return 返回异步作业状态
     */
    ShowExport queryAsynExportJobStatus(String dbName, String jobId);

    /**
     * 查看当前异步作业状态。
     *
     * @param dbName 数据库名称
     * @return 返回异步作业状态列表
     */
    List<ShowExport> queryAsynExportJobStatus(String dbName);

    /**
     * 中断任务。
     *
     * @param jobId 作业ID
     */
    void stopAsynExportJob(String jobId);

}
