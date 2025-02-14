package com.cn.jmw.pojo;

import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.ColumnEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SQLQueryMontage {
    int asset_category;

    String asset_ip;

    String db_name;

    String db_table;

    String index;

    ColumnEntity columnEntity;

    List<SQLQueryBuilder> sqlQueryBuilders;

    List<String> sql;

    List<InnerSQL> innerSQLS;

    /**
     * 抽样方式
     * 抽样数量
     * 抽样时间
     */
    final SampleResult sampleResult = new SampleResult();

}
