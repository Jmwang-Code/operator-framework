package cn.jt.bds.framework.processor.datasource.jdbc.dialect.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class QuerySQLResult{

    String sql;
    Object[] data;

}