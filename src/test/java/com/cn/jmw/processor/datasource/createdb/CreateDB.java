package com.cn.jmw.processor.datasource.createdb;

import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.jdbc.adapter.DorisJDBCAdapter;
import com.cn.jmw.processor.datasource.jdbc.adapter.MySQLJDBCAdapter;
import com.cn.jmw.processor.datasource.pojo.JDBCConnectionEntity;
import com.cn.jmw.processor.datasource.pojo.StreamLoadResult;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import jodd.util.ThreadFactoryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.laziji.commons.rereg.exception.RegexpIllegalException;
import org.laziji.commons.rereg.exception.TypeNotMatchException;
import org.laziji.commons.rereg.exception.UninitializedException;
import org.laziji.commons.rereg.model.Node;
import org.laziji.commons.rereg.model.OrdinaryNode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class CreateDB {

    private DorisJDBCAdapter dorisJDBCAdapter;

    private MySQLJDBCAdapter mySQLJDBCAdapter;

    @Before
    public void setUp() throws Exception {
        JDBCConnectionEntity dorisJdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "test", "root", "123456aA!@");
        dorisJDBCAdapter = DatabaseAdapterFactory.getAdapter(dorisJdbcConnectionEntity, DorisJDBCAdapter.class);

        JDBCConnectionEntity mysqlJdbcConnectionEntity = new JDBCConnectionEntity(DatabaseEnum.MYSQL, "192.168.10.222", 3306, "jt_bds", "root", "jt@2023!");
        mySQLJDBCAdapter = DatabaseAdapterFactory.getAdapter(mysqlJdbcConnectionEntity, MySQLJDBCAdapter.class);
    }

    /**
     * rows index   value       calculation
     * ————————————————————————————————————
     * 1    0       key         n%3==0
     * 2    1       value       n%3==2
     * 3    2       null        n%3==1
     */
    @Test
    public void insertFeature_library() throws SQLException {
        //SQL拼接
        StringBuilder sbSql = new StringBuilder("insert into system_feature_library(name,status,metadata_regex,regex) values ");

        Path path = new File("C:\\Users\\79283\\IdeaProjects\\jt-bds-base20240924\\jt-bds-base\\bds-framework\\bds-spring-boot-starter-structured\\src\\test\\java\\cn\\jt\\bds\\framework\\processor\\datasource\\createdb\\regex.txt").toPath();
        List<String> list;
        try {
            list = Files.readAllLines(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String key = null, value = null;
        for (int i = 0; i < list.size(); i++) {
            String s = list.get(i);
            if (i % 3 == 0) {
                // Key
                key = s.replace("'", "''"); // Escaping single quotes
                sbSql.append("(");
            } else if (i % 3 == 1) {
                value = s.replace("\"", "\\\"")
                        .replace("[/:-\\S]", "/:\\-")
                        .replace("\\", "\\\\")
                        .replace("'", "\\'");
                // Escaping single quotes
                sbSql.append("'").append(key).append("',")
                        .append(1).append(",")
                        .append("'(").append(key).append(")',")
                        .append("'").append(value).append("'")
                        .append("),");
            }
        }
        String substring = sbSql.toString().substring(0, sbSql.length() - 1);

        mySQLJDBCAdapter.executeDMLRUD(substring, null);
    }

    @Test
    public void createDB() throws SQLException {
        List<Map<String, Object>> selectRegexFromFeatureLibrary = mySQLJDBCAdapter.executeDMLC("select name,regex from system_feature_library where status = 1", null);
        for (int i = 0; i < selectRegexFromFeatureLibrary.size(); i++) {
            Map<String, Object> stringObjectMap = selectRegexFromFeatureLibrary.get(i);
            String name = (String) stringObjectMap.get("name");
            String regex = (String) stringObjectMap.get("regex");
            if (regex.startsWith("^")) {
                regex = regex.substring(1, regex.length());
            }
            if (regex.endsWith("$")) {
                regex = regex.substring(0, regex.length() - 1);
            }
            try {
                System.out.println(name);
                for (int j = 0; j < 10; j++) {
                    String random1 = random(regex, name);
                    System.out.println(random1);
                }
                System.out.println();
            } catch (Exception e) {
            }
        }

    }

    /**
     * random
     */
    public static String random(String expression, String title) throws RegexpIllegalException, TypeNotMatchException, UninitializedException, PatternSyntaxException {
        return random_reverse_regexp(expression, title);
    }

    /**
     * reverse-regexp
     */
    private static String random_reverse_regexp(String expression, String title) throws RegexpIllegalException, TypeNotMatchException, UninitializedException, PatternSyntaxException {
        Node node = new OrdinaryNode(expression);
        Pattern pattern = Pattern.compile(node.getExpression());
        String data = node.random();
        if (data.startsWith("^") || data.endsWith("$")) {
            data = data.substring(1, data.length() - 1);
        }
//        System.out.println("[" + pattern.matcher(data).matches() + "]" + data);
        if (!pattern.matcher(data).matches()) {
            return null;
        }
        return data;
    }

    static Random random = new Random();

    public static ExecutorService EXPORT_THREAD_POOL = new ThreadPoolExecutor(
            10,
            10,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1600),
            new ThreadFactoryBuilder().get(),
            new ThreadPoolExecutor.AbortPolicy());

    /**
     * RANDOM_DB
     * <p>
     * 100w数据 一张表 183秒 多线程后 77秒  优化后42
     * 10w数据 一张表 17秒  多线程后 9秒  优化后5
     * <p>
     * 100个10w的表 优化后 267秒
     *
     * 100个90w的表 优化后 2651秒
     */
    @Test
    public void random_DB() throws SQLException, InterruptedException {
        long l = System.currentTimeMillis();
        // 示例调用
        String databaseName = "test";
        for (int i = 1001; i < 1600; i++) {
//            tableName = tableName + SnowflakeIdUtil.generateId();
            String tableName = "bds_test_log" + i;
            //判断表是否存在
            List<Map<String, Object>> maps = dorisJDBCAdapter.executeDMLC("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '"+databaseName+"' AND table_name = '" + tableName + "'", null);
            Long count = (Long) maps.get(0).get("count(*)");

            Set<String> fieldNames = Set.of();
            if (count > 0) {
                //获取对应库表的对应字段
                List<Map<String, Object>> maps1 = dorisJDBCAdapter.executeDMLC(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + databaseName + "' AND TABLE_NAME = '" + tableName + "'",
                        null);
                fieldNames = maps1.stream()
                        .map(v -> (String)(v.get("COLUMN_NAME")))
                        .collect(Collectors.toSet());
            } else {
                fieldNames = generateRandomFieldNames(200);
                String createTableSQL = createDorisTableSQL(databaseName, tableName, fieldNames);
                dorisJDBCAdapter.executeDDL(createTableSQL, null);
            }

//            System.out.println(createTableSQL);
            JSONArray jsonArray = new JSONArray();
            //插入
            //sbSql 和 正则组
            List<Map<String, Object>> selectRegexFromFeatureLibrary = mySQLJDBCAdapter.executeDMLC("select name,regex from system_feature_library where status = 1", null);
            List<List<String>> list = insert_data(fieldNames, selectRegexFromFeatureLibrary, 100);
            List<String> list1 = fieldNames.stream().toList();
            //组装到jsonArray
            for (int j = 0; j < list.get(0).size(); j++) {
                JSONObject jsonObject = new JSONObject();
                for (int z = 0; z < list.size(); z++) {
                    jsonObject.put(list1.get(z), list.get(z).get(j));
                }
                jsonArray.add(jsonObject);
            }

            //字段拼接中间用,连接
            String fields = String.join(",", fieldNames);

            StreamLoadResult[] streamLoadResults;
            try {
                streamLoadResults = dorisJDBCAdapter.streamLoadBatch(jsonArray, tableName, 100000, null, FileTypeEnum.JSON);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(tableName+"表，运行结果"+streamLoadResults[0].getStatus());
        }

        //时间记录换算单位
        long l1 = System.currentTimeMillis();
        System.out.println((l1 - l) / 1000);
    }

    static final String hen = "-";

    /**
     * insert_data
     */
    private List<List<String>> insert_data(Set<String> fieldNames, List<Map<String, Object>> selectRegexFromFeatureLibrary, int count) throws SQLException, InterruptedException {
        List<List<String>> list = Collections.synchronizedList(new ArrayList<>());
        List<Future<List<String>>> futures = new ArrayList<>();

        // 使用多线程处理每个字段
        for (int i = 0; i < fieldNames.size(); i++) {
            final int index = i;  // 防止闭包问题
            Future<List<String>> future = EXPORT_THREAD_POOL.submit(() -> {
                List<String> stringList = new ArrayList<>();
                Map<String, Object> stringObjectMap = selectRegexFromFeatureLibrary.get(random.nextInt(selectRegexFromFeatureLibrary.size()));
                String regex = (String) stringObjectMap.get("regex");

                // 清洗正则表达式
                if (regex.startsWith("^")) {
                    regex = regex.substring(1);
                }
                if (regex.endsWith("$")) {
                    regex = regex.substring(0, regex.length() - 1);
                }

                for (int j = 0; j < count; j++) {
                    //十分之九的概率直接返回null
                    if (random.nextInt(9) == 0) {
                        try {
                            String random = random(regex, null);
                            stringList.add(random);
                        } catch (Exception e) {
                            e.printStackTrace();
                            stringList.add(hen);
                        }
                    } else {
                        stringList.add(hen);
                    }

//                    if (j % 1000000 == 0) {
//                        System.out.println("Thread " + index + " processed: " + j);
//                    }
                }
                return stringList;
            });
            futures.add(future);
        }

        // 收集所有结果
        for (Future<List<String>> future : futures) {
            try {
                list.add(future.get());
            } catch (ExecutionException e) {
                e.printStackTrace();
                list.add(Collections.emptyList());  // 添加空列表以避免影响整体结果
            }
        }

        return list;
    }

//    /**
//     * insert_data
//     */
//    private List<List<String>> insert_data(Set<String> fieldNames, List<Map<String, Object>> selectRegexFromFeatureLibrary, int count) throws SQLException {
//        List<List<String>> list = new ArrayList<>();
//        //随机找fieldNames个字段 从selectRegexFromFeatureLibrary 特征正则中
//        for (int i = 0; i < fieldNames.size(); i++) {
//            List<String> stringList = new ArrayList<>();
//            for (int j = 0; j < count; j++) {
//                Map<String, Object> stringObjectMap = selectRegexFromFeatureLibrary.get(i);
//                String regex = (String) stringObjectMap.get("regex");
//                if (regex.startsWith("^")) {
//                    regex = regex.substring(1, regex.length());
//                }
//                if (regex.endsWith("$")) {
//                    regex = regex.substring(0, regex.length() - 1);
//                }
//                try {
//                    String random = random(regex, null);
//                    stringList.add(random);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    stringList.add(null);
//                }
//
//                if (j%100000 == 0){
//                    System.out.println(j);
//                }
//            }
//            list.add(stringList);
//        }
//        return list;
//    }


    // 生成随机字段名称
    private static Set<String> generateRandomFieldNames(int count) {
        Set<String> fieldNames = new HashSet<>();
        for (int i = 0; i < count; i++) {
            fieldNames.add("field_" + random.nextInt(10000000)); // 随机生成字段名
        }
        return fieldNames;
    }

    // 创建Doris表的SQL语句
    private static String createDorisTableSQL(String databaseName, String tableName, Set<String> fieldNames) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE TABLE  IF NOT EXISTS `").append(databaseName).append("`.`").append(tableName).append("` (\n");

        // 添加随机字段
        for (String fieldName : fieldNames) {
            sqlBuilder.append("  `").append(fieldName).append("` varchar(1000) NULL COMMENT '随机字段',\n");
        }
        // index
        for (String fieldName : fieldNames) {
            sqlBuilder.append("  INDEX idx_").append(fieldName).append(" (`").append(fieldName).append("`) USING INVERTED,\n");
        }

        // 表的其他属性
        sqlBuilder.append(") ENGINE=OLAP\n")
                .append("COMMENT 'Auto-generated table'\n")
                .append("DISTRIBUTED BY HASH(`").append(fieldNames.stream().findFirst().get()).append("`) BUCKETS AUTO\n")
                .append("PROPERTIES (\n")
                .append("  \"replication_allocation\" = \"tag.location.default: 1\"\n")
                .append(");\n");

        return sqlBuilder.toString();
    }
}
