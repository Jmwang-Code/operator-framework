package cn.jt.bds.framework.processor.datasource.jdbc.dialect;

import cn.jt.bds.framework.processor.datasource.enums.DatabaseEnum;
import cn.jt.bds.framework.processor.datasource.jdbc.adapter.DorisJDBCAdapter;
import cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLFunctionEnum;
import cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;
import cn.jt.bds.framework.processor.datasource.jdbc.dialect.pojo.QueryCondition;
import cn.jt.bds.framework.processor.datasource.jdbc.dialect.pojo.QueryConditionBuilder;
import cn.jt.bds.framework.processor.datasource.pojo.JDBCConnectionEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.jooq.*;
import org.junit.Test;

import java.util.Arrays;

import static cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLFunctionEnum.DISTINCT;
import static cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLJoinEnum.*;
import static org.junit.Assert.assertEquals;

public class SQLQueryBuilderTest {

    @Test
    public void 复杂嵌套拼接() throws JsonProcessingException {
        JDBCConnectionEntity jdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "", "root", "123456aA!@");
//        DorisJDBCAdapter adapter = DatabaseAdapterFactory.getAdapter(jdbcConnectionEntity, DorisJDBCAdapter.class);

        // 创建多个嵌套查询
        SQLQueryBuilder nestedQueryBuilder1 = new SQLQueryBuilder();
        nestedQueryBuilder1.setSqlDialect(SQLDialect.MYSQL);
        nestedQueryBuilder1.tableName("nested_table1");
        nestedQueryBuilder1
                .addField("nested_field1", "nf1")
                .addField("nested_field2", "nf2");
        nestedQueryBuilder1.addAndCondition("nested_field1", SQLOperatorEnum.EQUAL, "value1';--");

        SQLQueryBuilder nestedQueryBuilder2 = new SQLQueryBuilder();
        nestedQueryBuilder2.setSqlDialect(SQLDialect.MYSQL);
        nestedQueryBuilder2.tableName("nested_table2");
        nestedQueryBuilder2
                .addField("nested_field3", "nf3")
                .addField("nested_field4", "nf4");
        nestedQueryBuilder2.addAndCondition("nested_field3", SQLOperatorEnum.EQUAL, "value2");

        // 创建嵌套的查询条件
        QueryCondition nestedCondition1 = new QueryCondition("field11", SQLOperatorEnum.EQUAL, 11);
        QueryCondition nestedCondition2 = new QueryCondition("field22", SQLOperatorEnum.EQUAL, nestedQueryBuilder1);
        QueryCondition nestedCondition = new QueryCondition(Arrays.asList(nestedCondition1, nestedCondition2), SQLOperatorEnum.OR);

        /**
         * 函数
         */
        // 定义一个最内层的嵌套函数条件
        QueryCondition innerCondition = new QueryCondition(
                SQLFunctionEnum.SUM,
                "inner_field",
                SQLOperatorEnum.GT,
                10,
                SQLOperatorEnum.AND,
                Arrays.asList("inner_field")
        );

        // 定义一个中间层的嵌套函数条件，包含最内层的嵌套函数
        QueryCondition midCondition = new QueryCondition(
                SQLFunctionEnum.AVG,
                "mid_field",
                SQLOperatorEnum.EQUAL,
                innerCondition,
                SQLOperatorEnum.AND,
                Arrays.asList(innerCondition)
        );

        // 定义一个最外层的嵌套函数条件，包含中间层的嵌套函数
        QueryCondition outerCondition = new QueryCondition(
                SQLFunctionEnum.COUNT,
                "outer_field",
                SQLOperatorEnum.LE,
                100,
                SQLOperatorEnum.AND,
                Arrays.asList(midCondition)
        );

        QueryCondition havingCondition = new QueryCondition(
                SQLFunctionEnum.SUM,
                "field3",
                SQLOperatorEnum.GT,
                100,
                SQLOperatorEnum.AND,
                Arrays.asList("field3")
        );


        /**
         * main构建器
         */
        QueryCondition complexConditionA = new QueryCondition("A", SQLOperatorEnum.EQUAL, 1);
        QueryCondition complexConditionB = new QueryCondition("B", SQLOperatorEnum.EQUAL, 2);
        QueryCondition complexConditionC = new QueryCondition("C", SQLOperatorEnum.EQUAL, 3);
        QueryCondition complexConditionBANDC = new QueryCondition(Arrays.asList(complexConditionB, complexConditionC), SQLOperatorEnum.OR);

        SQLQueryBuilder queryBuilderX = new SQLQueryBuilder();
        queryBuilderX.setSqlDialect(SQLDialect.MYSQL)
                //SELECT field1 AS f1,field2 AS f2
                // FROM main_table AS mt
                .tableName("main_table", "mt")
//                .addField("field1", "f1")
//                .addField("field2", "f2")
//                .addField("field3")
                //WHERE
//                .addNestingConditions(complexConditionA, complexConditionBANDC)
//                .addOrCondition("field1", SQLOperatorEnum.EQUAL, 1)
                .addOrCondition("field2", SQLOperatorEnum.IN, nestedQueryBuilder1)
                .addOrCondition("field3", SQLOperatorEnum.EQUAL, nestedQueryBuilder2)
                .addCondition(nestedCondition);
//                .addCondition(outerCondition)
        //JOIN
//                .addJoin(LEFT_JOIN, "table2", "t2", "mt.field1 = t2.field1")
//                .addJoin(INNER_JOIN, nestedQueryBuilder1, "nt1", "mt.field2 = nt1.nf1")
//                .addJoin(RIGHT_JOIN, nestedQueryBuilder2, "nt2", "mt.field3 = nt2.nf3")
        //GROUP BY
//                .addGroup("field1")
//                .addGroup("field2")
//                .addGroup("field3")
        //HAVING
//                .addHavingConditions(SQLFunctionEnum.COUNT, "field1", SQLOperatorEnum.GT, 1, SQLOperatorEnum.AND, Arrays.asList("关键是狗"))
//                .addHavingConditions(SQLFunctionEnum.SUM, "field2", SQLOperatorEnum.LT, 10, SQLOperatorEnum.OR, Arrays.asList("信不信我让你飞起来"))
        //ORDER
//                .addOrder("field1", SQLOperatorEnum.ASC)
//                .addOrder("field2", SQLOperatorEnum.DESC)
//                .addOrder("field3", SQLOperatorEnum.ASC)
//                //LIMIT OFFSET
//                .limit(100)
//                .offset(10);

        String jsonString = DorisJDBCAdapter.objectMapper.writeValueAsString(queryBuilderX);

        // 最后，调用 buildSQL 方法来生成 SQL 语句
        long l = System.currentTimeMillis();
        String sql = queryBuilderX.buildSQL();
//        System.out.println("执行耗时：" + (System.currentTimeMillis() - l) + "ms");
        System.out.println(sql);
        System.out.println(queryBuilderX.buildQuerySQLResult().getSql());
        System.out.println(queryBuilderX.buildQuerySQLResult().getData().length);
//        adapter.getDialectSQL(queryBuilder);
    }

    @Test
    public void 文本注入_单引号注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, 8)
                .addOrCondition("1", SQLOperatorEnum.EQUAL, 1)
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where (true or `asset_category` = 8 or `1` = 1)
        //成功防止sql注入
    }

    @Test
    public void 文本注入_注释符注入() {
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'8'; --");
        String s = sqlQueryBuilder.buildSQL();
        //select * from `bds_asset_info` where `asset_category` = 'admin''; --'
        //成功防止sql注入
    }

    //文本注入.分号注入
    @Test
    public void 文本注入_分号注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'8'; TRUNCATE TABLE bds_asset_info; --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = '8''; TRUNCATE TABLE users; --'
        //成功防止sql注入
    }

    //文本注入.UNION联合查询注入
    @Test
    public void 文本注入_Union注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addField("asset_id")
                .addField("asset_status")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'' UNION SELECT null, null, asset_category, asset_security_level --")
                .buildSQL();
        System.out.println(s);
        //select `bds_asset_info`.`asset_id`, `bds_asset_info`.`asset_status` from `bds_asset_info` where `asset_category` = "' UNION SELECT null, null, asset_category, asset_security_level --"
        //成功防止sql注入
    }

    //文本注入.时间延迟注入
    @Test
    public void 文本注入_时间延迟注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'13'; SELECT SLEEP(1); --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "'13'; SELECT SLEEP(1); --"
        //成功防止sql注入
    }

    //文本注入.错误消息注入
    @Test
    public void 文本注入_错误消息注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; SELECT 1/0; --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = ''; SELECT 1/0; --
        //成功防止sql注入
    }

    //布尔注入.基于注释的注入
    @Test
    public void 布尔注入_基于注释的注入() {
//        String s = new SQLQueryBuilder()
//                .tableName("bds_asset_info")
//                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'' OR '1'='1'")
//                .buildSQL();
//        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = 1
        //成功防止sql注入
        String s2 = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "' OR 1 = 1 --\"")
                .buildSQL();
        System.out.println(s2);
    }

    //布尔注入.基于井号的注入
    @Test
    public void 布尔注入_基于井号的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'admin' OR '1'='1'#")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "'admin' OR '1'='1'#"
        //成功防止sql注入
    }

    //布尔注入.基于单引号的注入
    @Test
    public void 布尔注入_基于单引号的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'admin' OR '1'='1'")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "'admin' OR '1'='1'"
        //成功防止sql注入
    }

    //布尔注入.基于通配符的注入
    @Test
    public void 布尔注入_基于通配符的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'0' OR asset_category LIKE '%'")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "'admin' OR '1'='1'"
        //成功防止sql注入
    }

    //布尔注入.基于等号的注入
    @Test
    public void 布尔注入_基于等号的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'admin' OR 1=1")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "'admin' OR 1=1"
        //成功防止sql注入
    }

    //布尔注入.基于分号的注入
    @Test
    public void 布尔注入_基于分号的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'admin'; TRUNCATE TABLE users; --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "'admin'; TRUNCATE TABLE users; --"
        //成功防止sql注入
    }

    //布尔注入.基于恶意关键字的注入
    @Test
    public void 布尔注入_基于恶意关键字的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'admin'; DROP TABLE users; --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "'admin' OR 1=1"
        //成功防止sql注入
    }

    //基于除零错误的注入
    @Test
    public void 布尔注入_基于除零错误的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; SELECT 1/0; --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "''; SELECT 1/0; --"
        //成功防止sql注入
    }

    //基于索引错误的注入
    @Test
    public void 布尔注入_基于索引错误的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; SELECT * FROM bds_asset_info; --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "''; SELECT * FROM bds_asset_info; --"
        //成功防止sql注入
    }

    //基于类型转换错误的注入
    @Test
    public void 布尔注入_基于类型转换错误的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; SELECT CONVERT(1, INT); --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "''; SELECT CONVERT(1, INT); --"
        //成功防止sql注入
    }

    //基于执行存储过程的注入
    @Test
    public void 布尔注入_基于执行存储过程的注入() {
        String s = new SQLQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; CALL sp_test(); --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = "''; CALL sp_test(); --"
        //成功防止sql注入
    }


    /**
     * select r.* , e.* from (SELECT /统计指标区/
     * count() AS c,
     * max( event_time ) AS latest_time,
     * min( event_time ) AS earliest_time,
     * max_by(event_id,event_time) as latest_log_id,
     * collect_set(event_id,20) as log_ids,
     * user_name
     * FROM
     * bds_login_key_log /索引区/
     * WHERE
     * event_time>"2024-04-10 00:00:00" /过滤条件区/
     * GROUP BY
     * user_name HAVING c > 100 /聚合指标区/) as r
     * JOIN bds_login_key_log as e where r.latest_log_id=e.event_id /获取最后一条日志/
     */
    @Test
    public void 告警SQL拼接() {
        QueryCondition queryCondition2 = new QueryCondition(Arrays.asList(new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00")
                , new QueryCondition("c", SQLOperatorEnum.GT, 100)), SQLOperatorEnum.AND);
        QueryCondition queryCondition1 = new QueryCondition(Arrays.asList(new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00")
                , new QueryCondition("c", SQLOperatorEnum.GT, 100)), SQLOperatorEnum.AND);

        QueryCondition queryCondition = new QueryCondition(Arrays.asList(queryCondition2
                , queryCondition1), SQLOperatorEnum.OR);
        SQLQueryBuilder queryBuilder = new SQLQueryBuilder()
                .tableName("A")
                .addCondition(queryCondition);

        System.out.println(queryBuilder.buildSQL());
        assertEquals(null, queryBuilder.buildSQL());
    }

    @Test
    public void 多层Condition嵌套测试() {

        QueryCondition queryCondition2 = new QueryCondition(Arrays.asList(new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00")
                , new QueryCondition("c", SQLOperatorEnum.GT, 100)), SQLOperatorEnum.AND);
        QueryCondition queryCondition1 = new QueryCondition(Arrays.asList(new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00")
                , new QueryCondition("c", SQLOperatorEnum.GT, 100)), SQLOperatorEnum.AND);

        QueryCondition queryCondition = new QueryCondition(Arrays.asList(queryCondition2
                , queryCondition1), SQLOperatorEnum.OR);
        queryCondition.setField("XXXX");
        queryCondition.setOperator(SQLOperatorEnum.EQUAL);
        queryCondition.setValue("YYYY");
        SQLQueryBuilder queryBuilder = new SQLQueryBuilder()
                .tableName("A")
                .addCondition(queryCondition);

        System.out.println(queryBuilder.buildSQL());


        SQLQueryBuilder queryBuilder1 = new SQLQueryBuilder()
                .tableName("a")
                .addCondition(
                        new QueryCondition(SQLOperatorEnum.AND,
                                new QueryCondition(SQLOperatorEnum.OR,
                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00"),
                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00")),
                                new QueryCondition(SQLOperatorEnum.OR,
                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00"),
                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00")))
                );

        System.out.println(queryBuilder1.buildSQL());

    }

    @Test
    public void 特殊函数测试() {
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder()
                .tableName("a")
                .addField("a", "B", DISTINCT)
                .addCondition("DBname", SQLOperatorEnum.LIKE, "b", SQLOperatorEnum.OR)
                .addCondition("DBname", SQLOperatorEnum.LIKE, "b", SQLOperatorEnum.OR)
                .addCondition("A", SQLOperatorEnum.MATCH_ANY, "b", SQLOperatorEnum.AND)
                .addCondition("A", SQLOperatorEnum.MATCH_ANY, "b", SQLOperatorEnum.OR);
        String key = "DBname";
        String value = "value";
        sqlQueryBuilder.addCondition(key, SQLOperatorEnum.MATCH_ANY, value, SQLOperatorEnum.OR);


        System.out.println(sqlQueryBuilder.buildSQL());
        System.out.println(sqlQueryBuilder.buildQuerySQLResult());

    }
}














