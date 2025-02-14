package com.cn.jmw.processor.datasource.pojo;

import com.cn.jmw.processor.datasource.enums.DataSourcePool;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JDBCAdapterDataSourceConfig extends AdapterDataSourceConfig{

    private DataSourcePool dataSourcePool = DataSourcePool.HIKARICP;

    /**
     * 最大活跃连接数
     */
    private int maxActive = 10;

    /**
     * 最小空闲连接数
     */
    private int minIdle = 10;
}
