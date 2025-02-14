package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.common.exception.ServiceException;
import com.cn.jmw.pojo.SQLQueryMontage;
import com.cn.jmw.pojo.SampleResult;
import com.cn.jmw.processor.datasource.JDBCAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.ADBC;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.ADBCQueryResultHandler;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.export.ASynExport;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.Doris;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.export.SynExport;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.instantiation.Instantiation;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.pojo.ProcessListPojo;
import com.cn.jmw.processor.datasource.pojo.*;
import com.cn.jmw.processor.datasource.jdbc.dialect.Dialect;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.alibaba.fastjson.JSONArray;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import org.apache.arrow.adbc.core.*;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.commons.codec.binary.Base64;
import com.cn.jmw.processor.datasource.jdbc.inter.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

@Slf4j
public class DorisJDBCAdapter extends JDBCAdapter implements
        //方言-SQL生成器
        Dialect,
        //实例化-表实例化POJO
        Instantiation,
        //同步导出
        SynExport,
        //异步导出
        ASynExport,
        //Doris特性功能 比如StreamLoad、RoutineLoad
        Doris,
        //ADBC协议
        ADBC {

    // DORIS HTTP PORT
    private static final int DORIS_HTTP_PORT = 8030;

    private static final int DORIS_JDBC_PORT = 9030;

    private static final int DORIS_ADBC_FE_PORT = 8815;

    private static final int DORIS_ADBC_BE_PORT = 8816;

    private final FlightSqlDriver driver;

    private final Map<String, Object> parameters = new HashMap<>();

    /**
     * 构造函数用于创建DorisJDBCAdapter实例。
     *
     * @param hostname     Doris服务器的主机名
     * @param port         Doris服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public DorisJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config, String connectionUser) throws AdbcException {
        super(hostname, port, databaseName, username, password, config, connectionUser);

        final BufferAllocator allocator = new RootAllocator();
        this.driver = new FlightSqlDriver(allocator);
        AdbcDriver.PARAM_URI.set(parameters, Location.forGrpcInsecure(super.hostname, DORIS_ADBC_FE_PORT).getUri().toString());
        AdbcDriver.PARAM_USERNAME.set(parameters, super.username);
        AdbcDriver.PARAM_PASSWORD.set(parameters, super.password);
    }

    public DorisJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password, JDBCAdapterDataSourceConfig config) throws AdbcException {
        this(hostname, port, databaseName, username, password, config, null);
    }

    public DorisJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password) throws AdbcException {
        this(hostname, port, databaseName, username, password, new JDBCAdapterDataSourceConfig(), null);
    }


    /**
     * 获取Doris的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return "jdbc:mysql://" + super.hostname + ":" + super.port + "/" + super.databaseName
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true" +
                "&connectTimeout=100000000&socketTimeout=600000000&autoReconnect=true";
    }

    /**
     * 使用批量查询数据。
     *
     * @param sql    执行的SQL语句
     * @param params SQL语句的参数
     * @return 查询结果，返回一个Map列表
     * @throws SQLException 如果发生SQL错误
     */
    public List<Map<String, Object>> executeDMLC(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser)) {
            if (connection == null) {
                throw new SQLException("未能获得有效连接.");
            }
            return super.runner.query(connection, sql, new MapListHandler(), params);
        } catch (SQLException e) {
            log.error("执行DMLC查询失败： ", e);
            throw e;
        }
    }

    /**
     * 当存在大批量的输入最好在外部定义一个Connection 公用它
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public List<Map<String, Object>> executeDMLC(Connection connection, String sql, Object[] params) throws SQLException {
        return super.runner.query(connection, sql, new MapListHandler(), params);
    }


    /**
     * 执行批量查询操作，尚未实现。
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @return boolean
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public int executeDMLRUD(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser)) {
            return super.runner.update(connection, sql, params);
        } catch (SQLException e) {
            throw new SQLException("执行更新失败: " + e.getMessage(), e);
        }
    }

    /**
     * 执行批量查询操作，尚未实现。
     *
     * @param sql    要执行的SQL查询语句
     * @param params 查询参数数组，可能为null
     * @return boolean
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public void executeDDL(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            // 设置参数（如果有）
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            statement.execute();
        } catch (SQLException e) {
            throw new SQLException("执行DDL时出错: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("information_schema", "__internal_schema", "mysql", "test");
    }

    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.DORIS;
    }

    //非全字段映射
    public static final ObjectMapper objectMapper = new ObjectMapper();

    //全字段映射
    private static final ObjectMapper fullFieldObjectMapper = new ObjectMapper();

    /**
     * 在将 LocalDateTime 对象转换为 JSON 时，ObjectMapper 默认会将其转换为一个包含年、月、日、小时、分钟、秒和纳秒的数组。
     * 这是因为 LocalDateTime 对象包含这些字段，而 ObjectMapper 默认会将对象的每个字段转换为 JSON 的一个元素。
     * 然而，Doris 的 DATETIME 类型需要的是一个格式为 'YYYY-MM-DD HH:MI:SS' 的字符串，而不是一个数组。
     * 因此，当你尝试将这个数组加载到 Doris 的 DATETIME 列时，会失败。
     * 为了解决这个问题，你需要告诉 ObjectMapper 使用一个特定的日期时间格式来序列化 LocalDateTime 对象。你可以使用 DateTimeFormatter 来定义这个格式，然后使用 JavaTimeModule 将这个格式器添加到 ObjectMapper。
     */
    static {
        JavaTimeModule module = new JavaTimeModule();
        module.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);// 用于在序列化空 Java Bean 时不抛出异常，即使对象中没有任何属性，也不会导致序列化失败。
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);// 反序列化时忽略未在目标类中定义的属性，避免由于 JSON 数据中有多余字段而抛出异常
        objectMapper.registerModule(module);// 解决 LocalDateTime 的序列化
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);// 驼峰命名
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY); // 忽略 null 值和空字符串

        fullFieldObjectMapper.registerModule(module);// 解决 LocalDateTime 的序列化
        fullFieldObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    private final static HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    // 如果连接目标是 FE，则需要处理 307 redirect。
                    return true;
                }
            });

    /**
     * 使用流加载将数据加载到 Doris 中。
     * <h1>不推荐直接使用streamLoad，而是使用streamLoadBatch</h1>
     *
     * @param data      要加载的 JSON 数据 / CSV 路径 / ORC 路径 / Parquet 路径
     * @param tableName 要将数据加载到其中的表的名称
     * @param columns   列的名称关系
     * @throws IOException 如果发生 IO 错误
     */
    @Deprecated //不推荐直接使用streamLoad，而是使用streamLoadBatch
    @Override
    public StreamLoadResult streamLoad(String data, String tableName, String columns, FileTypeEnum importFileType) throws IOException {
        String url = "http://" + super.hostname + ":" + DORIS_HTTP_PORT + "/api/" + super.databaseName + "/" + tableName + "/_stream_load";

        String loadResult = "";

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(url);
            //开启DEBUG日志 --debug
//            put.setHeader("debug", "true"); // Enable debug logging

            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(username, password));
            put.setHeader("max_filter_ratio", "1");
//            put.setHeader("strict_mode", "false");

            // 可以在 Header 中设置 stream load 相关属性，这里我们设置 label 和 column_separator。
            Calendar calendar = Calendar.getInstance();
            String label = String.format("audit_%s%02d%02d_%02d%02d%02d_%s",
                    calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                    calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                    UUID.randomUUID().toString().replaceAll("-", ""));

            //指定导入数据格式 csv, json, arrow, csv_with_names
            put.setHeader("format", importFileType.getName());
            // 用于指定 Doris 该次导入的标签，标签相同的数据无法多次导入
            put.setHeader("label", label);
            // 用于指定导入文件中的列分隔符
            put.setHeader("column_separator", ",");
            // strip_outer_array: 布尔类型，为true表示json数据以数组对象开始且将数组对象中进行展平，默认值是false
            put.setHeader("strip_outer_array", "true");
            if (StringUtils.isNotBlank(columns)) {
                /**
                 * 部分列更新
                 *
                 * Doris 在主键模型的导入更新，提供了可以直接插入或者更新部分列数据的功能，不需要先读取整行数据，这样更新效率就大幅提升了。
                 */
                put.setHeader("partial_columns", "true");
                /**
                 * 设置 columns
                 * 指定导入文件中的列和 table 中的列的对应关系。
                 */
                put.setHeader("columns", columns);
            }

            // 设置导入文件。
            switch (importFileType) {
                case CSV_WITH_NAMES: {
                    File file = new File(data); // data应该是CSV文件的路径
                    FileEntity entity = new FileEntity(file, ContentType.create("text/csv", StandardCharsets.UTF_8));
                    put.setEntity(entity);
                    break;
                }
                case JSON: {
                    // 这里也可以使用 StringEntity 来传输任意数据。
                    StringEntity entity = new StringEntity(data, StandardCharsets.UTF_8);
                    put.setEntity(entity);
                    break;
                }
                case ORC: {
                    File orcFile = new File(data); // data应为ORC文件的路径
                    FileEntity orcEntity = new FileEntity(orcFile, ContentType.create("application/octet-stream"));
                    put.setEntity(orcEntity);
                    break;
                }
                case PARQUET: {
                    File parquetFile = new File(data); // data应为Parquet文件的路径
                    FileEntity parquetEntity = new FileEntity(parquetFile, ContentType.create("application/octet-stream"));
                    put.setEntity(parquetEntity);
                    break;
                }
                default: {
                    //https://doris.apache.org/zh-CN/docs/data-operate/import/stream-load-manual
                    throw new IOException("不支持的数据格式");
                }
            }

            try (CloseableHttpResponse response = client.execute(put)) {
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }

                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("流加载失败。状态： %s 加载结果: %s", statusCode, loadResult));
                }

                System.out.println("获取加载结果: " + loadResult);
            }
        }
        //loadResult字符串转换成StreamLoadResult
        StreamLoadResult streamLoadResult = objectMapper.readValue(loadResult, StreamLoadResult.class);

        return streamLoadResult;
    }

    @Override
    public boolean createRoutineLoad(String databaseName, String routineLoadName, String targetTableName,
                                     String columnsTerminatedBy, String columns, String kafkaBrokerList,
                                     String kafkaTopic, String groupId, String kafkaPartitions,
                                     String kafkaOffsets, FileTypeEnum fileTypeEnum) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement()) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("CREATE ROUTINE LOAD ").append(databaseName + "." + routineLoadName + " ON " + targetTableName + "\n");

            //如果是JSON
            if (FileTypeEnum.CSV_WITH_NAMES == fileTypeEnum && StringUtils.isNotBlank(columnsTerminatedBy)) {
                stringBuilder.append("COLUMNS TERMINATED BY \"" + columnsTerminatedBy + "\",\n");
            }

            if (StringUtils.isNotBlank(columns)) {
                stringBuilder.append("COLUMNS(" + columns + ")\n");
            }

            //如果是JSON
            if (FileTypeEnum.JSON == fileTypeEnum) {
                stringBuilder.append("PROPERTIES(");
                stringBuilder.append("\"format\"=\"" + fileTypeEnum.getName() + "\",\n");
                stringBuilder.append("\"max_error_number\" = \"9999999999\",\n");
                stringBuilder.append("\"jsonpaths\"=\"" + parseColumns(columns, columnsTerminatedBy) + "\"\n");
                stringBuilder.append(")\n");
            }

            //KAFKA配置项
            stringBuilder.append("FROM KAFKA(\n")
                    /**
                     * 考虑包装起来，从入参到内部参数
                     *
                     * 访问 SSL 认证的 Kafka 集群 property 参数示例
                     * "property.security.protocol" = "ssl",
                     * "property.ssl.ca.location" = "FILE:ca.pem",
                     * "property.ssl.certificate.location" = "FILE:client.pem",
                     * "property.ssl.key.location" = "FILE:client.key",
                     * "property.ssl.key.password" = "ssl_passwd"
                     *
                     * 访问 PLAIN 认证的 Kafka 集群 property 参数示例
                     * "property.security.protocol"="SASL_PLAINTEXT",
                     * "property.sasl.mechanism"="PLAIN",
                     * "property.sasl.username"="admin",
                     * "property.sasl.password"="admin_passwd"
                     *
                     * 访问 Kerberos 认证的 Kafka 集群 property 参数示例
                     * "property.security.protocol" = "SASL_PLAINTEXT",
                     * "property.sasl.kerberos.service.name" = "kafka",
                     * "property.sasl.kerberos.keytab" = "/etc/krb5.keytab",
                     * "property.sasl.kerberos.principal" = "doris@YOUR.COM"
                     */
                    .append("\"kafka_broker_list\"=\"" + kafkaBrokerList + "\",\n")
                    .append("\"kafka_topic\"=\"" + kafkaTopic + "\",\n")
                    .append("\"kafka_group\"=\"" + groupId + "\",\n")
                    .append("\"kafka_partitions\"=\"" + kafkaPartitions + "\",\n")
                    .append("\"kafka_offsets\"=\"" + kafkaOffsets + "\"\n")
                    .append(");");

            return statement.execute(stringBuilder.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param columns 字段多个用逗号隔开
     * @return
     * @throws JsonProcessingException
     */
    public static String parseColumns(String columns, String columnsTerminatedBy) throws JsonProcessingException {
        List<String> collect = Arrays.stream(columns.split(columnsTerminatedBy))
                .map(column -> "$." + column)  // 添加转义引号
                .collect(Collectors.toList());
        // 使用 ObjectMapper 将 List<String> 转换为 JSON 字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(collect);

        // 如果需要在 Java 字符串中表示这个 JSON 字符串字面量（包含转义的双引号）
        // 则需要对双引号进行转义
        String jsonStringLiteral = json.replace("\"", "\\\"");

        // 注意：此时 jsonStringLiteral 不是一个有效的 JSON 字符串，
        // 它只是一个在 Java 字符串中表示 JSON 字符串字面量的字符串。
        // 如果您直接打印它，它将显示为转义后的形式。
        return jsonStringLiteral;
    }

    @Override
    public List<RoutineLoadResult> showRoutineLoadFor(String routineLoadName) {
        StringBuilder stringBuilder = new StringBuilder();
        if (StringUtils.isBlank(routineLoadName)) {
            stringBuilder.append("SHOW ROUTINE LOAD");
        } else {
            stringBuilder.append("SHOW ROUTINE LOAD FOR ").append(routineLoadName + ";");
        }
        List<RoutineLoadResult> routineLoadResults = new ArrayList<>();
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(stringBuilder.toString());) {

            while (resultSet.next()) {
                RoutineLoadResult result = RoutineLoadResult.builder()
                        .id(resultSet.getLong("Id"))
                        .name(resultSet.getString("Name"))
                        .createTime(resultSet.getString("CreateTime"))
                        .pauseTime(resultSet.getString("PauseTime"))
                        .endTime(resultSet.getString("EndTime"))
                        .dbName(resultSet.getString("DbName"))
                        .tableName(resultSet.getString("TableName"))
                        .isMultiTable(resultSet.getBoolean("IsMultiTable"))
                        .state(resultSet.getString("State"))
                        .dataSourceType(resultSet.getString("DataSourceType"))
                        .currentTaskNum(resultSet.getInt("CurrentTaskNum"))
                        .jobProperties(resultSet.getString("JobProperties"))
                        .dataSourceProperties(resultSet.getString("DataSourceProperties"))
                        .customProperties(resultSet.getString("CustomProperties"))
                        .statistic(resultSet.getString("Statistic"))
                        .progress(resultSet.getString("Progress"))
                        .lag(resultSet.getString("Lag"))
                        .reasonOfStateChanged(resultSet.getString("ReasonOfStateChanged"))
                        .errorLogUrls(resultSet.getString("ErrorLogUrls"))
                        .otherMsg(resultSet.getString("OtherMsg"))
                        .user(resultSet.getString("User"))
                        .comment(resultSet.getString("Comment"))
                        .build();

                routineLoadResults.add(result);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return routineLoadResults;
    }

    @Override
    public boolean determineCleaningBasedOnPartitionDataTableThreshold(List<String> DBNames, List<String> tableNames, List<Double> limitSizes, List<String> sortTimeFields, double threshold) {
        log.info("——————————————————————————————————————————————数据表阈值清理———————————————————————————————————————————————");

        //showTableStatus,获取到所有表的信息关于，curSizes当前大小 rowsCounts行数
        Map<String, ShowTableStatusResult> stringShowTableStatusResultMap = showTableStatus();

        //showPartitions,获取到所有表的信息关于，curSizes当前大小 rowsCounts行数
        for (int i = 0; i < tableNames.size(); i++) {
            //库名
            String DBName = DBNames.get(i);
            //表名
            String tableName = tableNames.get(i);
            //限制GB
            Double limitSize = limitSizes.get(i);
            //阈值之下
            double allowSize = (double) (limitSize * threshold);
            //排序时间字段
            String sortTimeField = sortTimeFields.get(i);

            //查询Partition按照时间有多少个
            List<ShowPartitionResult> showPartitionResults = showPartitions(DBName, tableName, sortTimeField);
            if (showPartitionResults == null) {
                log.info("数据表阈值清理————表名: {}, 失效表", tableName);
                continue;
            }

            if (showPartitionResults.size() < 2) {
                log.info("数据表阈值清理————表名: {}, 分区不存在或者分区数量达不到清理最小值2", tableName);
                continue;
            }

            // 检查分区数量是否超过 180
            if (showPartitionResults.size() > 180) {
                log.info("数据表阈值清理————表名: {}, 当前分区数量: {}, 超过限制 180", tableName, showPartitionResults.size());
                while (showPartitionResults.size() > 180) {
                    ShowPartitionResult earliestPartition = showPartitionResults.get(0);
                    String partitionName = earliestPartition.getPartitionName();
                    String sqlDelete = String.format("alter table %s.%s drop partition %s;", DBName, tableName, partitionName);

                    try {
                        executeDDL(sqlDelete, null);
                        log.info("数据表阈值清理————删除最早分区: {}, 表名: {}", partitionName, tableName);
                        showPartitionResults.remove(earliestPartition);
                    } catch (SQLException e) {
                        log.error("数据表阈值清理————处理表: {}, 异常信息: {}", tableName, e.getMessage());
                        return false;
                    }
                }
            }

            //curSizes当前大小 rowsCounts行数
            ShowPartitionResult showPartitionResult = showPartitionResults.get(0);
            String partitionName = showPartitionResult.getPartitionName();
            String partitionKey = showPartitionResult.getPartitionKey();
            if (partitionKey == null || !partitionKey.equals(sortTimeField)) {
                log.info("数据表阈值清理————表名: {}, 分区字段不正确: {}", tableName, partitionKey);
                continue;
            }
            ShowTableStatusResult showTableStatusResult = stringShowTableStatusResultMap.get(tableName);
            if (showTableStatusResult == null) {
                log.info("数据表阈值清理————表名: {}, 失效表", tableName);
                continue;
            }
            //curSizes当前大小
            double curSizes = showTableStatusResult.getDataLength();
            //curSizes当前大小要做计算从b换算成GB
            curSizes = curSizes / 1024 / 1024 / 1024;

            //阈值之下就通过
            if (curSizes < allowSize) {
                //日志打印
                log.info("数据表阈值清理————表名: {}, 当前大小: {}GB, 允许大小: {}GB", tableName, curSizes, allowSize);
                continue;
            }

            /**
             * alter table bds_linux_history_log drop partition p20240701000000
             */
            String sqlDelete = "alter table %s.%s drop partition %s;";

            double newCurSizes = curSizes;
            while (newCurSizes > allowSize) {
                sqlDelete = String.format(sqlDelete, DBName, tableName, partitionName);

                try {
                    executeDDL(sqlDelete, null);
                    log.info("数据表阈值清理————正在处理表: {}", tableName);
                    executeDDL("drop " + DBName + "/" + tableName + "/" + partitionName, null);
                } catch (SQLException e) {
                    log.error("数据表阈值清理————处理表: {},  异常信息: {}", tableName, e.getMessage());
                    log.info("————————————————————————————————————————————————————————————————————————————————————————————————————————");
//                    throw new RuntimeException(e);
                    return false;
                }
            }
        }

        log.info("————————————————————————————————————————————————————————————————————————————————————————————————————————");
        return true;
    }

    @Override
    public List<ShowPartitionResult> showPartitions(String dbName, String tableName, String sortTimeField) {
        List<ShowPartitionResult> partitions = new ArrayList<>();
        String sql = "SHOW PARTITIONS FROM " + dbName + "." + tableName + " ORDER BY PartitionName ASC";

//        Connection connection = null;
//        PreparedStatement statement = null;
//        ResultSet resultSet = null;
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery();) {
//            connection = pool.getConnection(hostname + port + databaseName);
//            statement = connection.prepareStatement(sql);
//            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ShowPartitionResult partition = ShowPartitionResult.builder()
                        .partitionId(resultSet.getLong("PartitionId"))
                        .partitionName(resultSet.getString("PartitionName"))
                        .visibleVersion(resultSet.getInt("VisibleVersion"))
                        .visibleVersionTime(resultSet.getString("VisibleVersionTime"))
                        .state(resultSet.getString("State"))
                        .partitionKey(resultSet.getString("PartitionKey"))
                        .range(resultSet.getString("Range"))
                        .distributionKey(resultSet.getString("DistributionKey"))
                        .buckets(resultSet.getInt("Buckets"))
                        .replicationNum(resultSet.getInt("ReplicationNum"))
                        .storageMedium(resultSet.getString("StorageMedium"))
                        .cooldownTime(resultSet.getString("CooldownTime"))
                        .remoteStoragePolicy(resultSet.getString("RemoteStoragePolicy"))
                        .lastConsistencyCheckTime(resultSet.getString("LastConsistencyCheckTime"))
                        .dataSize(resultSet.getString("DataSize"))
                        .isInMemory(resultSet.getBoolean("IsInMemory"))
                        .replicaAllocation(resultSet.getString("ReplicaAllocation"))
                        .isMutable(resultSet.getBoolean("IsMutable"))
                        .syncWithBaseTables(resultSet.getBoolean("SyncWithBaseTables"))
                        .unsyncTables(resultSet.getString("UnsyncTables"))
                        .build();

                partitions.add(partition);
            }
        } catch (SQLException e) {
            log.error("数据表阈值清理————表名: {} ,showPartitions异常信息: {}", tableName, e.getMessage());
        }

        return partitions;
    }

    @Override
    public String getQueryIDList(String databaseName, String likeSql) {
        return "";
    }

    @Override
    public List<ProcessListPojo> getProcessList(ProcessListPojo processListPojo) {
        List<ProcessListPojo> processListPojoList = new ArrayList<>();

        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder();
        sqlQueryBuilder.tableName("PROCESSLIST").dbName("information_schema");
        sqlQueryBuilder
                .addAndCondition("CURRENT_CONNECTED", SQLOperatorEnum.EQUAL, processListPojo.getCurrentConnected())
                .addAndCondition("ID", SQLOperatorEnum.EQUAL, processListPojo.getId())
                .addAndCondition("USER", SQLOperatorEnum.EQUAL, processListPojo.getUser())
                .addAndCondition("HOST", SQLOperatorEnum.EQUAL, processListPojo.getHost())
                .addAndCondition("LOGIN_TIME", SQLOperatorEnum.EQUAL, processListPojo.getLoginTime())
                .addAndCondition("CATALOG", SQLOperatorEnum.EQUAL, processListPojo.getCatalog())
                .addAndCondition("DB", SQLOperatorEnum.EQUAL, processListPojo.getDb())
                .addAndCondition("COMMAND", SQLOperatorEnum.EQUAL, processListPojo.getCommand())
                .addAndCondition("TIME", SQLOperatorEnum.EQUAL, processListPojo.getTime())
                .addAndCondition("STATE", SQLOperatorEnum.EQUAL, processListPojo.getState())
                .addAndCondition("QUERY_ID", SQLOperatorEnum.EQUAL, processListPojo.getQueryId())
                .addAndCondition("INFO", SQLOperatorEnum.EQUAL, processListPojo.getInfo())
                .addAndCondition("FE", SQLOperatorEnum.EQUAL, processListPojo.getFe())
                .addAndCondition("CLOUD_CLUSTER", SQLOperatorEnum.EQUAL, processListPojo.getCloudCluster());
        String sql = sqlQueryBuilder.buildSQL();

        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery();) {
            while (resultSet.next()) {
                ProcessListPojo build = ProcessListPojo.builder()
                        .currentConnected(resultSet.getString("CURRENT_CONNECTED"))
                        .id(resultSet.getLong("ID"))
                        .user(resultSet.getString("USER"))
                        .host(resultSet.getString("HOST"))
                        .loginTime(resultSet.getString("LOGIN_TIME"))
                        .catalog(resultSet.getString("CATALOG"))
                        .db(resultSet.getString("DB"))
                        .command(resultSet.getString("COMMAND"))
                        .time(resultSet.getInt("TIME"))
                        .state(resultSet.getString("STATE"))
                        .queryId(resultSet.getString("QUERY_ID"))
                        .info(resultSet.getString("INFO"))
                        .fe(resultSet.getString("FE"))
                        .cloudCluster(resultSet.getString("CLOUD_CLUSTER"))
                        .build();
                processListPojoList.add(build);
            }
        } catch (SQLException e) {
            log.error("获取进程列表失败，异常信息: {}", e.getMessage());
        }
        return processListPojoList;
    }

    @Override
    public boolean killQuery(String queryId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement("KILL QUERY '" + queryId + "';");) {
            boolean b = statement.execute();
            return b;
        } catch (SQLException e) {
            log.error("删除查询任务失败，异常信息: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean killConnection(Integer connectionId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement("KILL CONNECTION " + connectionId );) {
            boolean b = statement.execute();
            return b;
        } catch (SQLException e) {
            log.error("杀死连接失败，异常信息: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 生成基本认证的HTTP头部。
     *
     * @param username 用户名
     * @param password 密码
     * @return 返回基本认证的HTTP头部字符串
     */
    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    /**
     * 将对象列表转换为 JSON 字符串。
     *
     * @param list 要转换的对象列表
     * @return JSON字符串
     * @throws IOException 如果发生 IO 错误
     */
    private String listToJson(List<?> list) throws IOException {
        return objectMapper.writeValueAsString(list);
    }

    /**
     * 将一个大列表分成指定大小进行流加载。
     *
     * @param list      要加载的对象列表
     * @param tableName 要将数据加载到其中的表的名称
     * @param size      每批次的大小
     * @param columns   列的名称关系
     * @return StreamLoadResult数组
     * @throws IOException 如果发生 IO 错误
     */
    public StreamLoadResult[] streamLoadBatch(List<?> list, String tableName, int size, String columns, FileTypeEnum importFileType) throws IOException {
        int total = list.size();
        if (list == null || total == 0) {
            return new StreamLoadResult[0];
        }

        int batch = total / size;
        StreamLoadResult[] results = new StreamLoadResult[batch + 1];
        int remainder = total % size;
        int start = 0;
        for (int i = 0; i < batch; i++) {
            List<?> subList = list.subList(start, start + size);
            StreamLoadResult streamLoadResult = streamLoad(listToJson(subList), tableName, columns, importFileType);
            if (streamLoadResult == null) {
                continue;
            }
            results[i] = streamLoadResult;
            start += size;
        }
        if (remainder > 0) {
            List<?> subList = list.subList(start, start + remainder);
            StreamLoadResult streamLoadResult = streamLoad(listToJson(subList), tableName, columns, importFileType);
            results[batch] = streamLoadResult;
        }

        return results;
    }

    public StreamLoadResult[] streamLoadBatch(JSONArray jsonArray, String tableName, int size, String columns, FileTypeEnum importFileType) throws IOException {
        int total = jsonArray.size();
        if (jsonArray == null || total == 0) {
            return new StreamLoadResult[0];
        }

        int batch = total / size;
        StreamLoadResult[] results = new StreamLoadResult[batch + 1];
        int remainder = total % size;
        int start = 0;
        for (int i = 0; i < batch; i++) {
            List<?> subList = jsonArray.subList(start, start + size);
            StreamLoadResult streamLoadResult = streamLoad(listToJson(subList), tableName, columns, importFileType);
            results[i] = streamLoadResult;
            start += size;
        }
        if (remainder > 0) {
            List<?> subList = jsonArray.subList(start, start + remainder);
            StreamLoadResult streamLoadResult = streamLoad(listToJson(subList), tableName, columns, importFileType);
            results[batch] = streamLoadResult;
        }

        return results;
    }

    @Override
    public String buildSQL(SQLQueryBuilder queryBuilder) {
        return queryBuilder.buildSQL();
    }

    @Override
    public Object instantiate(String databaseName, String tableName) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             ResultSet resultSet = connection.getMetaData().getColumns(databaseName, null, tableName, null)) {
            // 创建ByteBuddy对象
            ByteBuddy byteBuddy = new ByteBuddy();

            // 开始构建类
            tableName = toCamelCase(tableName);
            DynamicType.Builder<?> builder = byteBuddy.subclass(Object.class)
                    .name(tableName);

            // 记录字段名 fieldNames
            List<String> fieldNames = new ArrayList<>();
            // 记录字段类型 fieldTypes
            List<Class<?>> fieldTypes = new ArrayList<>();

            // 使用反射来设置字段的值
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String camelCaseColumnName = toCamelCase(columnName);
                fieldNames.add(camelCaseColumnName);
                String columnType = resultSet.getString("TYPE_NAME");
                Class<?> javaType = sqlTypeToJavaType(columnType);
                fieldTypes.add(javaType);

                // 将字段添加到类中
                builder = builder.defineField(camelCaseColumnName, javaType, Modifier.PUBLIC)
                        .defineMethod("get" + capitalize(camelCaseColumnName), javaType, Modifier.PUBLIC)
                        .intercept(FieldAccessor.ofBeanProperty())
                        .defineMethod("set" + capitalize(camelCaseColumnName), void.class, Modifier.PUBLIC)
                        .withParameter(javaType)
                        .intercept(FieldAccessor.ofBeanProperty());
            }

//            // 定义全参构造器
//            builder = builder.defineConstructor(Modifier.PUBLIC)
//                    .withParameters(fieldTypes.toArray(new Class<?>[0]))
//                    .intercept(new FieldSettingImplementation(fieldNames, fieldTypes));

            // Make the class
            DynamicType.Unloaded<?> unloadedType = builder.make();
//            unloadedType.saveIn(new File("C:\\Users\\79283\\IdeaProjects\\jt-bds-base2333\\bds-framework\\bds-spring-boot-starter-structured\\target\\classes")); // 保存 .class 文件到指定目录

            Class<?> loadedClass;
            try {
                loadedClass = Class.forName(unloadedType.getTypeDescription().getName());
            } catch (ClassNotFoundException e) {
                loadedClass = unloadedType.load(getClass().getClassLoader()).getLoaded();
            }

            return loadedClass.getDeclaredConstructor().newInstance(); // 使用无参构造器实例化

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void aSynExport(String dbName, String tableName, String where, String path, String columns, String jobId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement()) {
            StringBuilder sql = new StringBuilder("EXPORT TABLE " + dbName + "." + tableName + " \n");
            if (StringUtils.isNotBlank(where)) {
                sql.append("WHERE ");
                sql.append(where);
                sql.append("\n");
            }
            if (StringUtils.isBlank(path)) {
                path = "/tmp/";
            }
            sql.append("TO \"file://" + path + "\"\n" +
                    "PROPERTIES (\n");
            if (StringUtils.isBlank(columns)) {
                sql.append(" \"columns\" = \"" + columns + "\",");
            }
            sql.append("  \"label\" =\"" + jobId + "\",\n" +
                    "  \"format\" = \"csv_with_names\",\n" +
                    "  \"column_separator\" = \",\",\n" +
                    "  \"line_delimiter\" = \"\\n\",\n" +
                    "  \"max_file_size\" = \"2GB\",\n" +
                    "  \"with_bom\" = \"true\"\n" +
                    ");");
            statement.execute(sql.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ShowExport queryASynExportJobStatus(String dbName, String label) {
        //查询结果封装成ShowExport类
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW EXPORT FROM " + dbName + " WHERE LABEL like '%" + label + "%'");) {
            while (resultSet.next()) {
                String state = resultSet.getString("STATE");
                String progress = resultSet.getString("PROGRESS");
                String taskinfo = resultSet.getString("TASKINFO");
                String path = resultSet.getString("PATH");
                String createtime = resultSet.getString("CREATETIME");
                String starttime = resultSet.getString("STARTTIME");
                String finishtime = resultSet.getString("FINISHTIME");
                String timeout = resultSet.getString("TIMEOUT");
                String errormsg = resultSet.getString("ERRORMSG");
                String outfileinfo = resultSet.getString("OUTFILEINFO");

                //封装到ShowExport类
                ShowExport build = ShowExport.builder()
                        .state(state)
                        .progress(progress)
                        .taskInfo(taskinfo)
                        .path(path)
                        .createTime(createtime)
                        .startTime(starttime)
                        .finishTime(finishtime)
                        .timeout(timeout)
                        .errorMsg(errormsg)
                        .outfileInfo(outfileinfo)
                        .build();
                return build;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new ShowExport();
    }

    @Override
    public List<ShowExport> queryASynExportJobStatus(String dbName) {
        List<ShowExport> showExports = new ArrayList<>();
        //查询结果封装成ShowExport类
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW EXPORT FROM " + dbName)) {
            while (resultSet.next()) {
                String state = resultSet.getString("STATE");
                String progress = resultSet.getString("PROGRESS");
                String taskinfo = resultSet.getString("TASKINFO");
                String path = resultSet.getString("PATH");
                String createtime = resultSet.getString("CREATETIME");
                String starttime = resultSet.getString("STARTTIME");
                String finishtime = resultSet.getString("FINISHTIME");
                String timeout = resultSet.getString("TIMEOUT");
                String errormsg = resultSet.getString("ERRORMSG");
                String outfileinfo = resultSet.getString("OUTFILEINFO");

                //封装到ShowExport类
                ShowExport build = ShowExport.builder()
                        .state(state)
                        .progress(progress)
                        .taskInfo(taskinfo)
                        .path(path)
                        .createTime(createtime)
                        .startTime(starttime)
                        .finishTime(finishtime)
                        .timeout(timeout)
                        .errorMsg(errormsg)
                        .outfileInfo(outfileinfo)
                        .build();
                showExports.add(build);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return showExports;
    }

    @Override
    public void stopASynExportJob(String jobId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement()) {
            String sql = "CANCEL EXPORT FROM tpch1 WHERE LABEL like \"%" + jobId + "%\";";
            statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntoOutFile synExport(String sql, String path, String maximumFileSize, FileTypeEnum fileTypeEnum) {
        // 使用正则表达式来处理嵌套的 SELECT 语句
        Pattern pattern = Pattern.compile("(?i)^\\s*(\\(*\\s*)select(\\s+)");
        Matcher matcher = pattern.matcher(sql);

        StringBuilder SQLBuilder = new StringBuilder();

        if (matcher.find()) {
            String matched = matcher.group(1); // 获取匹配的左括号部分
            int leftParenthesesCount = matched.length() - matched.replace("(", "").length(); // 计算左括号数量
            String replacement = "SELECT /*+ SET_VAR(query_timeout = 300) */ ";

            // 根据左括号数量构造新的 SQL 语句
            SQLBuilder.append("(".repeat(leftParenthesesCount)).append(replacement).append(sql.substring(matcher.end())); // 使用repeat来重复左括号
        } else {
            throw new ServiceException();
        }

        SQLBuilder.append(" INTO OUTFILE \"file://").append(path + "\"\n")
                .append("FORMAT AS " + fileTypeEnum + "\n")
                .append("PROPERTIES(\n");

        if (fileTypeEnum == fileTypeEnum.CSV_WITH_NAMES) {
            SQLBuilder.append("\"column_separator\" = \",\",\n")
                    .append("\"line_delimiter\" = \"\\n\",\n");
        }

        SQLBuilder.append("\"max_file_size\" = \"" + maximumFileSize + "\",\n")
                .append("\"with_bom\" = \"true\"\n")
                .append(");");
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQLBuilder.toString())) {
            while (resultSet.next()) {
                Integer fileNumber = resultSet.getInt("FileNumber");
                Integer totalRows = resultSet.getInt("TotalRows");
                Long fileSize = resultSet.getLong("FileSize");
                String url = resultSet.getString("URL");
                IntoOutFile build = IntoOutFile.builder()
                        .FileNumber(fileNumber)
                        .TotalRows(totalRows)
                        .FileSize(fileSize)
                        .URL(url)
                        .build();
                return build;
            }
        } catch (Exception e) {
            String message = e.getMessage();
            IntoOutFile build = IntoOutFile.builder()
                    .errorMsg(message)
                    .build();
            return build;
        }
        return new IntoOutFile();
    }

    @Override
    public ShowCreateTable getCreateTableDDL(String dbName, String table) {
        //通过语句查FE
        String sql = "SHOW CREATE TABLE " + dbName + "." + table + ";";
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                String createTableDDL = resultSet.getString("Create Table");
                String tableCur = resultSet.getString("Table");
                ShowCreateTable build = ShowCreateTable.builder()
                        .create_table(createTableDDL)
                        .table(tableCur)
                        .build();
                return build;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * @param sql
     * @param handler
     * @return
     * @throws Exception
     */
    @Override
    public <T> T queryStreamADBC(String sql, ADBCQueryResultHandler<T> handler) throws Exception {
        T result = null;
        try (AdbcDatabase adbcDatabase = this.driver.open(parameters);
             AdbcConnection connection = adbcDatabase.connect();
             AdbcStatement stmt = connection.createStatement()) {

            stmt.setSqlQuery(sql);
            AdbcStatement.QueryResult queryResult = stmt.executeQuery();
            ArrowReader reader = queryResult.getReader();

            List<Map<String, Object>> batchResults = new ArrayList<>();
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                List<FieldVector> fieldVectors = root.getFieldVectors();

                // 构造批次数据
                for (int i = 0; i < root.getRowCount(); i++) {
                    Map<String, Object> row = new HashMap<>();
                    for (FieldVector fieldVector : fieldVectors) {
                        row.put(fieldVector.getField().getName(), fieldVector.getObject(i));
                    }
                    batchResults.add(row);
                }

                // 调用回调处理器
                result = handler.handle(batchResults);
                batchResults.clear(); // 清空批次数据
            }

            reader.close();
            queryResult.close();
            adbcDatabase.close();
        }

        return result;
    }


    @Override
    public <T> T jdbcConnectWithArrowFlightSQL(String sql, ResultSetHandler<T> handler) throws Exception {
        T result = null;
        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
        String DB_URL = "jdbc:arrow-flight-sql://" + hostname + ":" + DORIS_ADBC_FE_PORT
                + "?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false";
        String USER = username;
        String PASS = password;

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             Statement stmt = conn.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql)) {

            // 直接调用 handler 处理整个 ResultSet
            result = handler.handle(resultSet);
        }

        return result; // 返回处理后的结果
    }

    /**
     * <h1>在Doris中已经废弃，因为Doris不支持JDBC的方式进行流批式查询</h1>
     */
    @Deprecated
    @Override
    public <T> T queryStream(String sql, ResultSetHandler<T> handler, Integer size) throws SQLException {
        return super.queryStream(sql, handler, size);
    }

    private static class FieldSettingImplementation implements Implementation {
        private final List<String> fieldNames;
        private final List<Class<?>> fieldTypes;

        public FieldSettingImplementation(List<String> fieldNames, List<Class<?>> fieldTypes) {
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return new ByteCodeAppender.Simple(
                    new StackManipulation.Compound(
                            new StackManipulation() {
                                @Override
                                public boolean isValid() {
                                    return true;
                                }

                                @Override
                                public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                                    for (int i = 0; i < fieldNames.size(); i++) {
                                        methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // 加载this

                                        // 假设 fieldTypes.get(i) 返回的是字段的类型，我们需要根据类型决定加载指令
                                        Type fieldType = Type.getType(fieldTypes.get(i));
                                        int loadOpcode = fieldType.getOpcode(Opcodes.ILOAD); // 注意：这里需要根据实际类型调整

                                        // 对于非基本类型（如Object），应该使用 ALOAD，且参数索引直接从 1 开始
                                        if (fieldType.getSort() == Type.OBJECT || fieldType.getSort() == Type.ARRAY) {
                                            loadOpcode = Opcodes.ALOAD;
                                        }

                                        // 加载参数（注意这里直接使用 i + 1 作为索引）
                                        methodVisitor.visitVarInsn(loadOpcode, i + 1);

                                        // 存储到字段
                                        methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, implementationContext.getInstrumentedType().getInternalName(), fieldNames.get(i), fieldType.getDescriptor());
                                    }

                                    // 假设每个字段赋值操作占用两个字节码单元（这只是一个大致估计）
                                    return new Size(fieldNames.size() * 2, fieldNames.size() * 2);
                                }
                            }
                    )
            );
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }
    }

    private Class<?> sqlTypeToJavaType(String sqlType) {
        switch (sqlType) {
            case "VARCHAR":
            case "CHAR":
            case "LONGVARCHAR":
                return String.class;
            case "NUMERIC":
            case "DECIMAL":
                return java.math.BigDecimal.class;
            case "BIT":
                return Boolean.class;
            case "TINYINT":
                return Byte.class;
            case "SMALLINT":
                return Short.class;
            case "INTEGER":
                return Integer.class;
            case "BIGINT":
                return Long.class;
            case "REAL":
                return Float.class;
            case "FLOAT":
            case "DOUBLE":
                return Double.class;
            case "BINARY":
            case "VARBINARY":
            case "LONGVARBINARY":
                return byte[].class;
            case "DATE":
                return Date.class;
            case "TIME":
                return Time.class;
            case "TIMESTAMP":
                return Timestamp.class;
            default:
                return Object.class;
        }
    }

    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }

    private static String toCamelCase(String s) {
        String[] parts = s.split("_");
        StringBuilder camelCaseString = new StringBuilder(parts[0].toLowerCase());
        for (int i = 1; i < parts.length; i++) {
            camelCaseString.append(toProperCase(parts[i]));
        }
        return camelCaseString.toString();
    }

    private static String toProperCase(String s) {
        return s.substring(0, 1).toUpperCase() +
                s.substring(1).toLowerCase();
    }

    /**
     * 添加随机采样
     * <h1>不允许出现 ORDER BY、 LIMIT等字眼</h1>
     *
     * @param sqlQueryMontage
     * @return 增加随机抽样后的SQL
     */
    @Override
    public List<String> addRandomSampling(SQLQueryMontage sqlQueryMontage, int N, int M) {
        SampleResult sampleResult = sqlQueryMontage.getSampleResult();
        sampleResult.setSample_result("增量");
        sampleResult.setSample_time(LocalDateTime.now());

        SQLQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().get(0);
        String sql = sqlQueryBuilder.buildSQL();
        List<String> list = new ArrayList<>();
        if (StringUtils.isBlank(sql)) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }
        Matcher matcher = RandomSamplingCompile.matcher(sql);
        if (matcher.find()) {
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }
        //首先获取总量
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM").replaceAll("order by.+", ")");

        List<Map<String, Object>> countResult = null;
        try {
            countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.get(0).get("count(1)");
            // 验证抽样点和前后记录分布一定要小于表的总记录数
            if (N <= 0 || totalCount == 0 || M <= 0) {
                sampleResult.setSample_count(0);
                return list; // 无效参数直接返回
            }

            if (totalCount < M) {
                list.add(sql);
                sampleResult.setSample_count(totalCount);
                return list;
            }

        } catch (SQLException e) {
            e.printStackTrace();
//            throw new RuntimeException(e);
            return list;
        }

        String randomSampling = ") LIMIT " + M;
        if (sql.contains("order by")) {
            sql = sql.replaceAll("order by.+", randomSampling);
        } else {
            sql = sql.substring(0, sql.length() - 1) + randomSampling;
        }
        list.add(sql);
        return list;
    }

    /**
     * 获取索引
     *
     * @param dbName    数据库
     * @param tableName 数据表
     * @return 索引
     */
    @Override
    public Map<String, Map<String, Object>> getIndexInfo(Connection connection, String dbName, String tableName) {
        Map<String, Map<String, Object>> indexInfoList = new HashMap<>();
        try (ResultSet indexInfo = connection.getMetaData().getIndexInfo(null, dbName, tableName, false, false);) {

            while (indexInfo.next()) {
                Map<String, Object> indexDetails = new HashMap<>();
                String COLUMN_NAME = indexInfo.getString("COLUMN_NAME");
                String indexType = indexInfo.getString("TYPE");
                indexDetails.put("INDEX_TYPE", indexType);
                indexDetails.put("INDEX_NAME", indexInfo.getString("INDEX_NAME"));
                indexDetails.put("COLUMN_NAME", COLUMN_NAME);
                indexDetails.put("IS_UNIQUE", !indexInfo.getBoolean("NON_UNIQUE"));

                indexInfoList.put(COLUMN_NAME, indexDetails);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw exception(INDEX_INFO_GET_ERROR);
        }

        if (indexInfoList.isEmpty()) {
            throw exception(INDEX_INFO_GET_ERROR);
        }
        return indexInfoList;
    }

    /**
     * 获取（除忽略数据库外的全部元数据）数据库元数据信息。
     *
     * @return 数据库实体列表，包含数据库中的表信息
     * @throws SQLException 如果无法检索数据库元数据，将抛出异常
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        List<String> ignoreDatabases = getIgnoreDatabaseList(); // 获取需要忽略的数据库列表
        try (Connection connection = pool.getConnection(hostname + port + databaseName, config, connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet catalogs = connection.getMetaData().getCatalogs()) {  // 使用 try-with-resources 确保 ResultSet 被关闭
                while (catalogs.next()) {
                    String databaseName = catalogs.getString("TABLE_CAT");
                    // 如果数据库在忽略列表中，则跳过
                    if (ignoreDatabases.contains(databaseName)) {
                        continue;
                    }
                    if (StringUtils.isNotBlank(dbName) && !dbName.equals(databaseName)) {
                        continue;
                    }
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(databaseName);
                    // 存入DatabaseType
                    databaseEntity.setDatabaseEnum(getDatabaseType());
                    Map<String, TableEntity> tableEntities = new HashMap<>();
                    try (PreparedStatement preparedStatement = connection.prepareStatement("USE " + databaseName);
                         ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {  // 使用 try-with-resources 确保 ResultSet 被关闭
                        preparedStatement.execute();
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            //TODO 跳过所有物化视图不加载
                            if (tableName.contains("materialized_view")){
                                continue;
                            }
                            //TODO 查索引
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection, databaseName, tableName);

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            tableEntity.setTableComment(tables.getString("REMARKS")); // 获取表注释
                            Map<String, ColumnEntity> columnEntities = new HashMap<>();
                            try (ResultSet cols = metaData.getColumns(databaseName, null, tableName, "%")) {  // 使用 try-with-resources 确保 ResultSet 被关闭
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    String TYPE_NAME = cols.getString("TYPE_NAME");
                                    columnEntity.setColumnName(cols.getString("COLUMN_NAME")); // 获取列名
                                    columnEntity.setColumnType(TYPE_NAME); // 获取列类型
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE")); // 获取列大小
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable); // 获取可否为NULL
                                    columnEntity.setDefaultValue(cols.getString("COLUMN_DEF")); // 获取默认值
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0); // 获取自增状态
                                    columnEntity.setColumnComment(cols.getString("REMARKS")); // 获取列注释

                                    // 判断是什么类型的TYPE_NAME
                                    columnEntity.setType(getColumnType(TYPE_NAME));

                                    //TODO 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                    }

                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                                throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA); // 抛出无法检索数据库元数据的异常
                            }
                            tableEntity.setColumns(columnEntities); // 设置表的列信息
                            tableEntities.put(tableName, tableEntity);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA); // 抛出无法检索数据库元数据的异常
                    }
                    databaseEntity.setTables(tableEntities); // 设置数据库中的表信息
                    databaseEntities.add(databaseEntity);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA); // 抛出无法检索数据库元数据的异常
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA); // 抛出无法检索数据库元数据的异常
        }
        return databaseEntities; // 返回数据库实体列表
    }

    /**
     * 1字符串 2数字 0时间
     * <p>
     * 判断输入的字符串名称是什么类型的数据字段
     */
    public int getColumnType(String columnType) {
        switch (columnType) {
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                return 1;
            case "BIGINT":
            case "INT":
            case "LARGEINT":
                return 2;
            case "DATE":
            case "DATETIME":
                return 0;
            default:
                return 3;
        }
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        JDBCConnectionEntity root = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "bds_log", "root", "123456aA!@");

        DorisJDBCAdapter adapter = DatabaseAdapterFactory.getAdapter(root, DorisJDBCAdapter.class);

//        List<String> sampledData = adapter.N_point_sampling_method("SELECT * FROM bds_asset_info", 10, 10);
//        List<DatabaseEntity> databaseMetadata = adapter.getDatabaseMetadata();
//        System.out.println(databaseMetadata);
//        Map<String, Map<String, Object>> indexInfo = adapter.getIndexInfo("bds_log", "bds_asset_info");
//        System.out.println(indexInfo);
//        String s = adapter.addRandomSampling("SELECT * FROM bds_asset_info",500);
//        System.out.println(s);

        //ADBC
        final AtomicInteger[] atomicInteger = {new AtomicInteger()};
        final int[] j = {0};
        long start = System.currentTimeMillis();


//        try {
//            adapter.queryStreamADBC("select * from bds_log.bds_wangc_log;", new ADBCQueryResultHandler<Object>() {
//                @Override
//                public Object handle(List<Map<String, Object>> batch) throws Exception {
//                    j[0]+=batch.size();
//                    if (j[0]%406400==0){
//                        System.out.println(j[0]);
//                    }
//                    return null;
//                }
//            });
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println(j[0]);

//        List<Map<String, Object>> results;
//        try {
////            results = adapter.jdbcConnectWithArrowFlightSQL("select * from bds_log.bds_asset_info;", new CustomResultSetHandler());
//            adapter.jdbcConnectWithArrowFlightSQL("select * from bds_log.bds_asset_info;", new ResultSetHandler() {
//                @Override
//                public Object handle(ResultSet resultSet) throws SQLException {
//                    do {
//                    } while (resultSet.next());
//                    return null;
//                }
//            });
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println(j[0]);


//        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
//        String DB_URL = "jdbc:arrow-flight-sql://192.168.10.202:8815?useServerPrepStmts=false"
//                + "&cachePrepStmts=true&useSSL=false&useEncryption=false";
//        String USER = "root";
//        String PASS = "123456aA!@";
//
//        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
//             Statement stmt = conn.createStatement();
//             ResultSet resultSet = stmt.executeQuery("select * from bds_log.bds_wangc_log");) {
//            while (resultSet.next()) {
//                j[0]++;
//                if (j[0]%1000000==0){
//                    System.out.println(j[0]);
//                }
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println(j[0]);

        System.out.println("最后时间:" + (System.currentTimeMillis() - start) / 1000);

//        LocalDateTime now = LocalDateTime.now();
//        //转换成 正常的DateTime格式
//        String format = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
//
//        ProcessListPojo processListPojo = new ProcessListPojo("1", 2L,"3","4",LocalDateTime.now(), "5", "6", "7", 8,"9","10","11","12","13");
//        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder();
//        sqlQueryBuilder.dbName("information_schema")
//                .tableName("PROCESSLIST")
//                .addAndCondition("CURRENT_CONNECTED", SQLOperatorEnum.EQUAL, processListPojo.getCurrentConnected())
//                .addAndCondition("ID", SQLOperatorEnum.EQUAL, processListPojo.getId())
//                .addAndCondition("USER", SQLOperatorEnum.EQUAL, processListPojo.getUser())
//                .addAndCondition("HOST", SQLOperatorEnum.EQUAL, processListPojo.getHost())
//                .addAndCondition("LOGIN_TIME", SQLOperatorEnum.EQUAL, processListPojo.getLoginTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
//                .addAndCondition("CATALOG", SQLOperatorEnum.EQUAL, processListPojo.getCatalog())
//                .addAndCondition("DB", SQLOperatorEnum.EQUAL, processListPojo.getDb())
//                .addAndCondition("COMMAND", SQLOperatorEnum.EQUAL, processListPojo.getCommand())
//                .addAndCondition("TIME", SQLOperatorEnum.EQUAL, processListPojo.getTime())
//                .addAndCondition("STATE", SQLOperatorEnum.EQUAL, processListPojo.getState())
//                .addAndCondition("QUERY_ID", SQLOperatorEnum.EQUAL, processListPojo.getQueryId())
//                .addAndCondition("INFO", SQLOperatorEnum.EQUAL, processListPojo.getInfo())
//                .addAndCondition("FE", SQLOperatorEnum.EQUAL, processListPojo.getFe())
//                .addAndCondition("CLOUD_CLUSTER", SQLOperatorEnum.EQUAL, processListPojo.getCloudCluster());
//        String s = sqlQueryBuilder.buildSQL();
//        System.out.println(s);

        String tableName = "bds_fortress_log";
        String dbName = "bds_log";
        LocalDate endDate = LocalDate.of(2024, 8, 31); // 开始日期
        int partitionCount = 180;

        DateTimeFormatter partitionFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter valueFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        StringBuilder sqlBuilder = new StringBuilder();
        for (int i = 0; i < partitionCount; i++) {
            LocalDate partitionDate = endDate.minusDays(i); // 当前分区日期
            LocalDate previousDate = partitionDate.minusDays(1); // 上一个分区的日期
            String partitionName = "p" + partitionDate.format(partitionFormatter); // 分区名
            String lessThanValue = partitionDate.format(valueFormatter); // 范围上限


            try {
                adapter.executeDDL(String.format(
                        "ALTER TABLE %s.%s ADD PARTITION %s VALUES LESS THAN ('%s');\n",
                        dbName, tableName, partitionName, lessThanValue
                ), null);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            partitionDate = previousDate;
        }

        System.out.println(sqlBuilder.toString());
    }

}
