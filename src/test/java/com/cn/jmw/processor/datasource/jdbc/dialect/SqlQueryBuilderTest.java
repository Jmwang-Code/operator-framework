package com.cn.jmw.processor.datasource.jdbc.dialect;

import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.jdbc.adapter.DorisJdbcAdapter;
import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLFunctionEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.pojo.QueryCondition;
import com.cn.jmw.processor.datasource.pojo.JdbcConnectionEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.jooq.SQLDialect;
import org.junit.Test;

import java.util.Arrays;

import static com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLFunctionEnum.DISTINCT;
import static com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLJoinEnum.*;
import static org.junit.Assert.assertEquals;

public class SqlQueryBuilderTest {

    @Test
    public void 复杂嵌套拼接() throws JsonProcessingException {
        JdbcConnectionEntity JdbcConnectionEntity = new JdbcConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "", "root", "123456aA!@");
//        DorisJdbcAdapter adapter = DatabaseAdapterFactory.getAdapter(JdbcConnectionEntity, DorisJdbcAdapter.class);

        // 创建多个嵌套查询
        SqlQueryBuilder nestedQueryBuilder1 = new SqlQueryBuilder();
        nestedQueryBuilder1.setSqlDialect(SQLDialect.MYSQL);
        nestedQueryBuilder1.tableName("nested_table1");
        nestedQueryBuilder1
                .addField("nested_field1", "nf1")
                .addField("nested_field2", "nf2");
        nestedQueryBuilder1.addAndCondition("nested_field1", SQLOperatorEnum.EQUAL, "value1';--");

        SqlQueryBuilder nestedQueryBuilder2 = new SqlQueryBuilder();
        nestedQueryBuilder2.setSqlDialect(SQLDialect.MYSQL);
        nestedQueryBuilder2.tableName("nested_table2");
        nestedQueryBuilder2
                .addField("nested_field3", "nf3")
                .addField("nested_field4", "nf4");
        nestedQueryBuilder2.addAndCondition("nested_field3", SQLOperatorEnum.IS_NULL);

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

        SqlQueryBuilder queryBuilderX = new SqlQueryBuilder();
        queryBuilderX.setSqlDialect(SQLDialect.MYSQL)
                //SELECT field1 AS f1,field2 AS f2
                // FROM main_table AS mt
                .tableName("main_table", "mt")
                .addField("field1", "f1")
                .addField("field2", "f2")
                .addField("field3")
                //WHERE
//                .addNestingConditions(complexConditionA, complexConditionBANDC)
                .addOrCondition("field1", SQLOperatorEnum.EQUAL, 1)
                .addOrCondition("field2", SQLOperatorEnum.IN, nestedQueryBuilder1)
                .addOrCondition("field3", SQLOperatorEnum.EQUAL, nestedQueryBuilder2)
                .addCondition(nestedCondition)
                .addCondition(outerCondition)
        //JOIN
                .addJoin(LEFT_JOIN, "table2", "t2", "mt.field1 = t2.field1")
                .addJoin(INNER_JOIN, nestedQueryBuilder1, "nt1", "mt.field2 = nt1.nf1")
                .addJoin(RIGHT_JOIN, nestedQueryBuilder2, "nt2", "mt.field3 = nt2.nf3")
        //GROUP BY
                .addGroup("field1")
                .addGroup("field2")
                .addGroup("field3")
        //HAVING
                .addHavingConditions(SQLFunctionEnum.COUNT, "field1", SQLOperatorEnum.GT, 1, SQLOperatorEnum.AND, Arrays.asList("关键是狗"))
                .addHavingConditions(SQLFunctionEnum.SUM, "field2", SQLOperatorEnum.LT, 10, SQLOperatorEnum.OR, Arrays.asList("信不信我让你飞起来"))
        //ORDER
                .addOrder("field1", SQLOperatorEnum.ASC)
                .addOrder("field2", SQLOperatorEnum.DESC)
                .addOrder("field3", SQLOperatorEnum.ASC)
//                //LIMIT OFFSET
                .limit(100)
                .offset(10);

        String jsonString = DorisJdbcAdapter.objectMapper.writeValueAsString(queryBuilderX);

        // 最后，调用 buildSQL 方法来生成 SQL 语句
        long l = System.currentTimeMillis();
//        System.out.println("执行耗时：" + (System.currentTimeMillis() - l) + "ms");
        System.out.println(queryBuilderX.buildSQL());
        System.out.println(queryBuilderX.buildQuerySQLResult());
    }

    @Test
    public void 文本注入_单引号注入() {
        String s = new SqlQueryBuilder()
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
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'8'; --");
        String s = SqlQueryBuilder.buildSQL();
        //select * from `bds_asset_info` where `asset_category` = 'admin''; --'
        //成功防止sql注入
    }

    //文本注入.分号注入
    @Test
    public void 文本注入_分号注入() {
        String s = new SqlQueryBuilder()
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
        String s = new SqlQueryBuilder()
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
        String s = new SqlQueryBuilder()
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
        String s = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.NOT_EQUAL, "''; SELECT 1/0; --")
                .buildSQL();
        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = ''; SELECT 1/0; --
        //成功防止sql注入
    }

    //布尔注入.基于注释的注入
    @Test
    public void 布尔注入_基于注释的注入() {
//        String s = new SqlQueryBuilder()
//                .tableName("bds_asset_info")
//                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'' OR '1'='1'")
//                .buildSQL();
//        System.out.println(s);
        //select * from `bds_asset_info` where `asset_category` = 1
        //成功防止sql注入
        String s2 = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "' OR 1 = 1 --\"")
                .buildSQL();
        System.out.println(s2);
    }

    //布尔注入.基于井号的注入
    @Test
    public void 布尔注入_基于井号的注入() {
        String s = new SqlQueryBuilder()
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
        String s = new SqlQueryBuilder()
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
        String s = new SqlQueryBuilder()
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
        String s = new SqlQueryBuilder()
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
        String s = new SqlQueryBuilder()
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
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "'admin'; DROP TABLE users; --");
        System.out.println(SqlQueryBuilder.buildSQL());
        System.out.println(SqlQueryBuilder.buildQuerySQLResult());
        //select * from `bds_asset_info` where `asset_category` = "'admin' OR 1=1"
        //成功防止sql注入
    }

    //基于除零错误的注入
    @Test
    public void 布尔注入_基于除零错误的注入() {
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; SELECT 1/0; --");
        System.out.println(SqlQueryBuilder.buildSQL());
        System.out.println(SqlQueryBuilder.buildQuerySQLResult());
        //select * from `bds_asset_info` where `asset_category` = "''; SELECT 1/0; --"
        //成功防止sql注入
    }

    //基于索引错误的注入
    @Test
    public void 布尔注入_基于索引错误的注入() {
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; SELECT * FROM bds_asset_info; --");
        System.out.println(SqlQueryBuilder.buildSQL());
        System.out.println(SqlQueryBuilder.buildQuerySQLResult());
        //select * from `bds_asset_info` where `asset_category` = "''; SELECT * FROM bds_asset_info; --"
        //成功防止sql注入
    }

    //基于类型转换错误的注入
    @Test
    public void 布尔注入_基于类型转换错误的注入() {
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addOrCondition("asset_category", SQLOperatorEnum.EQUAL, "''; SELECT CONVERT(1, INT); --");
        System.out.println(SqlQueryBuilder.buildSQL());
        System.out.println(SqlQueryBuilder.buildQuerySQLResult());
        //select * from `bds_asset_info` where `asset_category` = "''; SELECT CONVERT(1, INT); --"
        //成功防止sql注入
    }

    //基于执行存储过程的注入
    @Test
    public void 布尔注入_基于执行存储过程的注入() {
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("bds_asset_info")
                .addAndCondition("asset_category", SQLOperatorEnum.IS_NOT_NULL);

        System.out.println(SqlQueryBuilder.buildSQL());
        System.out.println(SqlQueryBuilder.buildQuerySQLResult());
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
        SqlQueryBuilder queryBuilder = new SqlQueryBuilder()
                .tableName("A")
                .addCondition(queryCondition);

        System.out.println(queryBuilder.buildSQL());
        System.out.println(queryBuilder.buildQuerySQLResult());
        assertEquals(null, queryBuilder.buildSQL());
    }

    /**
     * 这既使用嵌套的方式又将嵌套的QueryCondition 上增加并列条件是不被允许的
     *
     *         queryCondition.setField("XXXX");
     *         queryCondition.setOperator(SQLOperatorEnum.EQUAL);
     *         queryCondition.setValue("YYYY");
     */
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
        SqlQueryBuilder queryBuilder = new SqlQueryBuilder()
                .tableName("A")
                .addCondition(queryCondition);

        System.out.println(queryBuilder.buildSQL());
        System.out.println(queryBuilder.buildQuerySQLResult());


//        SqlQueryBuilder queryBuilder1 = new SqlQueryBuilder()
//                .tableName("a")
//                .addCondition(
//                        new QueryCondition(SQLOperatorEnum.AND,
//                                new QueryCondition(SQLOperatorEnum.OR,
//                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-10 00:00:00"),
//                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-11 00:00:00")),
//                                new QueryCondition(SQLOperatorEnum.OR,
//                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-12 00:00:00"),
//                                        new QueryCondition("event_time", SQLOperatorEnum.GT, "2024-04-13 00:00:00")))
//                );
//
//        System.out.println(queryBuilder1.buildSQL());
//        System.out.println(queryBuilder1.buildQuerySQLResult());

    }

    @Test
    public void 特殊函数测试() {
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("a")
                .addField("a", "B", DISTINCT)
                .addCondition("DBname", SQLOperatorEnum.LIKE, "b", SQLOperatorEnum.OR)
                .addCondition("DBname", SQLOperatorEnum.LIKE, "value_with_special_characters_like_\'a_and_%", SQLOperatorEnum.OR)
                .addCondition("A", SQLOperatorEnum.MATCH_ANY, "b", SQLOperatorEnum.AND)
                .addCondition("A", SQLOperatorEnum.MATCH_ANY, "b", SQLOperatorEnum.OR);
        String key = "DBname";
        String value = "value";
        SqlQueryBuilder.addCondition(key, SQLOperatorEnum.MATCH_ANY, value, SQLOperatorEnum.OR);


        System.out.println(SqlQueryBuilder.buildSQL());
        System.out.println(SqlQueryBuilder.buildQuerySQLResult());

    }

    @Test
    public void 测试嵌套查询() {
        SqlQueryBuilder SqlQueryBuilderSubA = new SqlQueryBuilder()
                .tableName("平部表")
                .addAndCondition("字段平", SQLOperatorEnum.EQUAL, "平\"");

        // list=[中", 内", 平", A']
        //中 内 平 A
        //中 平 A
        SqlQueryBuilder SqlQueryBuilderSubSub = new SqlQueryBuilder()
                .tableName("内部表")
                .addAndCondition("字段内", SQLOperatorEnum.EQUAL, "内\"");

        SqlQueryBuilder SqlQueryBuilderSub = new SqlQueryBuilder()
                .tableName("中部表")
                .addAndCondition("字段中", SQLOperatorEnum.EQUAL, "中\"")
                .addAndCondition("B",SQLOperatorEnum.EQUAL,SqlQueryBuilderSubSub);

        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName(SqlQueryBuilderSub,"TABLEA")
                .addAndCondition("字段外", SQLOperatorEnum.EQUAL, SqlQueryBuilderSubA)
                .addAndCondition("A", SQLOperatorEnum.EQUAL, "A'");

        System.out.println(SqlQueryBuilder.buildSQL());

        System.out.println(SqlQueryBuilder.buildQuerySQLResult());

    }

    @Test
    public void 测试模式名称() {
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("Employees")
                .schemaName("dbo")
                .dbName("master")
                .addField("Email");
        System.out.println(SqlQueryBuilder.buildSQL().replaceAll("`",""));
    }

    @Test
    public void 测试OR字符串Condition(){
        SqlQueryBuilder SqlQueryBuilder = new SqlQueryBuilder()
                .tableName("Employees")
//                .addOrCondition("a", SQLOperatorEnum.EQUAL, "b")
//                .addOrCondition("c", SQLOperatorEnum.EQUAL, "bddd")
                .addOrStringCondition("c = 'd'")
                .addOrStringCondition("A = 'd'");
        System.out.println(SqlQueryBuilder.buildSQL().replaceAll("`",""));

        SqlQueryBuilder SqlQueryBuilder2 = new SqlQueryBuilder()
                .tableName("Employees")
//                .addAndCondition("a", SQLOperatorEnum.EQUAL, "b")
//                .addAndCondition("c", SQLOperatorEnum.EQUAL, "bddd")
                .addStringCondition("c = 'd'")
                .addStringCondition("A = 'd'");
        System.out.println(SqlQueryBuilder2.buildSQL().replaceAll("`",""));
    }
}














