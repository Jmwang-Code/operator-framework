package cn.jt.bds.framework.processor.datasource.jdbc.dialect.pojo;

import cn.jt.bds.framework.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryTable {

    // 表名
    private String table;

    // 表别名
    private String tableAlias;

    // SQL查询构建器
    private SQLQueryBuilder sqlQueryBuilder;

    /**
     * 构造一个QueryTable实例，使用SQL查询构建器和字段别名。
     *
     * @param sqlQueryBuilder SQL查询构建器
     * @param tableAlias      字段别名
     */
    public QueryTable(SQLQueryBuilder sqlQueryBuilder, String tableAlias) {
        this.tableAlias = tableAlias;
        this.sqlQueryBuilder = sqlQueryBuilder;
    }

    /**
     * 构造一个QueryTable实例，仅使用表名。
     *
     * @param table 表名
     */
    public QueryTable(String table) {
        this.table = table;
    }

    /**
     * 构造一个QueryTable实例，仅使用表名。
     *
     * @param table 表名
     * @param tableAlias      字段别名
     */
    public QueryTable(String table, String tableAlias) {
        this.table = table;
        this.tableAlias = tableAlias;
    }
}
