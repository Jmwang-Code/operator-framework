package com.cn.jmw.pojo;

import com.cn.jmw.processor.datasource.jdbc.dialect.SqlQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.ColumnEntity;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Optional;

/**
 * @author Jmwang
 */
@Data
@Builder
public class SqlQueryMontage {
    /**
     * 资产类别，表示资产的分类标识。
     */
    private int assetCategory;

    /**
     * 资产IP地址，表示资产的网络地址，可能为 null。
     */
    private String assetIp;

    /**
     * 数据库名称，表示查询所针对的数据库。
     */
    private String dbName;

    /**
     * 数据库表名，表示查询所针对的表。
     */
    private String dbTable;

    /**
     * 索引名称，表示表中的索引，可能为 null。
     */
    private String indexName;

    /**
     * 列实体，表示与查询相关的列信息，可能为 null。
     */
    private ColumnEntity columnEntity;

    /**
     * SQL查询构建器列表，用于生成SQL查询语句，可能为 null。
     */
    private List<SqlQueryBuilder> sqlQueryBuilders;

    /**
     * 原始SQL语句列表，表示完整的查询语句，可能为 null。
     */
    private List<String> sql;

    /**
     * 内部SQL列信息列表，表示表结构元数据，可能为 null。
     */
    private List<com.cn.jmw.pojo.InnerSql> innerSql;

    /**
     * 抽样结果，包含抽样方式、数量和时间等信息，可能不存在。
     */
    private Optional<com.cn.jmw.pojo.SampleResult> sampleResult;

    /**
     * 默认构造函数，用于创建一个空的SqlQueryMontage实例。
     */
    public SqlQueryMontage() {}

    /**
     * 带参数的构造函数，用于初始化所有字段。
     *
     * @param assetCategory     资产类别
     * @param assetIp           资产IP地址
     * @param dbName            数据库名称
     * @param dbTable           数据库表名
     * @param indexName         索引名称
     * @param columnEntity      列实体
     * @param sqlQueryBuilders  SQL查询构建器列表
     * @param sql               原始SQL语句列表
     * @param innerSql          内部SQL列信息列表
     * @param sampleResult      抽样结果
     */
    public SqlQueryMontage(int assetCategory, String assetIp, String dbName, String dbTable,
                           String indexName, ColumnEntity columnEntity, List<SqlQueryBuilder> sqlQueryBuilders,
                           List<String> sql, List<com.cn.jmw.pojo.InnerSql> innerSql, Optional<com.cn.jmw.pojo.SampleResult> sampleResult) {
        this.assetCategory = assetCategory;
        this.assetIp = assetIp;
        this.dbName = dbName;
        this.dbTable = dbTable;
        this.indexName = indexName;
        this.columnEntity = columnEntity;
        this.sqlQueryBuilders = sqlQueryBuilders;
        this.sql = sql;
        this.innerSql = innerSql;
        this.sampleResult = Optional.ofNullable(new com.cn.jmw.pojo.SampleResult());
    }

    /**
     * 获取资产类别。
     *
     * @return 资产类别
     */
    public int getAssetCategory() {
        return assetCategory;
    }

    /**
     * 设置资产类别。
     *
     * @param assetCategory 资产类别
     */
    public void setAssetCategory(int assetCategory) {
        this.assetCategory = assetCategory;
    }

    /**
     * 获取资产IP地址。
     *
     * @return 资产IP地址
     */
    public String getAssetIp() {
        return assetIp;
    }

    /**
     * 设置资产IP地址。
     *
     * @param assetIp 资产IP地址
     */
    public void setAssetIp(String assetIp) {
        this.assetIp = assetIp;
    }

    /**
     * 获取数据库名称。
     *
     * @return 数据库名称
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * 设置数据库名称。
     *
     * @param dbName 数据库名称
     */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * 获取数据库表名。
     *
     * @return 数据库表名
     */
    public String getDbTable() {
        return dbTable;
    }

    /**
     * 设置数据库表名。
     *
     * @param dbTable 数据库表名
     */
    public void setDbTable(String dbTable) {
        this.dbTable = dbTable;
    }

    /**
     * 获取索引名称。
     *
     * @return 索引名称
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * 设置索引名称。
     *
     * @param indexName 索引名称
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * 获取列实体。
     *
     * @return 列实体
     */
    public ColumnEntity getColumnEntity() {
        return columnEntity;
    }

    /**
     * 设置列实体。
     *
     * @param columnEntity 列实体
     */
    public void setColumnEntity(ColumnEntity columnEntity) {
        this.columnEntity = columnEntity;
    }

    /**
     * 获取SQL查询构建器列表。
     *
     * @return SQL查询构建器列表
     */
    public List<SqlQueryBuilder> getSqlQueryBuilders() {
        return sqlQueryBuilders;
    }

    /**
     * 设置SQL查询构建器列表。
     *
     * @param sqlQueryBuilders SQL查询构建器列表
     */
    public void setSqlQueryBuilders(List<SqlQueryBuilder> sqlQueryBuilders) {
        this.sqlQueryBuilders = sqlQueryBuilders;
    }

    /**
     * 获取原始SQL语句列表。
     *
     * @return 原始SQL语句列表
     */
    public List<String> getSql() {
        return sql;
    }

    /**
     * 设置原始SQL语句列表。
     *
     * @param sql 原始SQL语句列表
     */
    public void setSql(List<String> sql) {
        this.sql = sql;
    }

    /**
     * 获取内部SQL列信息列表。
     *
     * @return 内部SQL列信息列表
     */
    public List<com.cn.jmw.pojo.InnerSql> getInnerSql() {
        return innerSql;
    }

    /**
     * 设置内部SQL列信息列表。
     *
     * @param innerSql 内部SQL列信息列表
     */
    public void setInnerSql(List<com.cn.jmw.pojo.InnerSql> innerSql) {
        this.innerSql = innerSql;
    }

    /**
     * 获取抽样结果。
     *
     * @return 抽样结果的Optional包装
     */
    public Optional<com.cn.jmw.pojo.SampleResult> getSampleResult() {
        return sampleResult;
    }

    /**
     * 设置抽样结果。
     *
     * @param sampleResult 抽样结果的Optional包装
     */
    public void setSampleResult(com.cn.jmw.pojo.SampleResult sampleResult) {
        this.sampleResult = Optional.ofNullable(sampleResult);
    }

    /**
     * 返回对象的字符串表示形式。
     *
     * @return 包含对象所有字段的字符串表示
     */
    @Override
    public String toString() {
        return "SqlQueryMontage{" +
                "assetCategory=" + assetCategory +
                ", assetIp='" + assetIp + '\'' +
                ", dbName='" + dbName + '\'' +
                ", dbTable='" + dbTable + '\'' +
                ", indexName='" + indexName + '\'' +
                ", columnEntity=" + columnEntity +
                ", sqlQueryBuilders=" + sqlQueryBuilders +
                ", sql=" + sql +
                ", innerSql=" + innerSql +
                ", sampleResult=" + sampleResult +
                '}';
    }

}
