package com.cn.jmw.processor.base;

import com.cn.jmw.pojo.SQLQueryMontage;
import com.cn.jmw.processor.BaseProcessor;
import com.cn.jmw.processor.datasource.Database;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.JDBCConnectionEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * 算法采样
 *
 * 抽查 n 条
 */
public class AlgorithmicSamplingProcessor extends BaseProcessor<List<SQLQueryMontage>, List<SQLQueryMontage>> {

    @Override
    public List<SQLQueryMontage> process(List<SQLQueryMontage> input, Object... data) throws Exception {
        if (input==null || input.size()==0){
            return new ArrayList<>();
        }

        if (data == null) {
            return List.of();
        }
        JDBCConnectionEntity jdbcConnectionEntity = (JDBCConnectionEntity) data[0];
        if (jdbcConnectionEntity == null) {
            return List.of();
        }

        DatabaseEnum dbType = jdbcConnectionEntity.getDbType();
        Database database = DatabaseAdapterFactory.getDatabase(jdbcConnectionEntity, dbType.getAdapterClass());
        for (int i = 0; i < input.size(); i++) {
            SQLQueryMontage sqlQueryMontage = input.get(i);
            /**
             * 当数据量大于M条的时候，如果数量小于M条直接全拿出来
             *
             * TODO 暂时不考虑全量的全量N点抽样区间
             * 全量
             * 有索引 和 (数字OR时间)
             * 全量N点抽样区间
             * 用N个点把整体数据分割开，然后每个点从头拆选M条数据
             * 如果第一次筛选完的数据，达不到M条
             * 第二次就一定会移动limit补全到M条
             *
             * 增量
             *
             */
            List<String> list;
            if (data!=null && data.length>1){
                list = database.addRandomSampling(sqlQueryMontage, 10, 1000);
            }else {
                list = database.addRandomSampling(sqlQueryMontage, 10, 10000);
            }

            if (list!=null && list.size()>0){
                sqlQueryMontage.setSql(list);
            }
        }

        return input;
    }
}
