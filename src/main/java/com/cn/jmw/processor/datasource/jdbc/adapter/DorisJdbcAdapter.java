package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.annotation.NotRecommended;
import com.cn.jmw.common.exception.ServiceException;
import com.cn.jmw.pojo.SqlQueryMontage;
import com.cn.jmw.pojo.SampleResult;
import com.cn.jmw.processor.datasource.AbstractJdbcAdapter;
import com.cn.jmw.processor.datasource.ConnectionStringStrategy;
import com.cn.jmw.processor.datasource.GeneratedConnectionStringStrategy;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
import com.cn.jmw.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.Adbc;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.AdbcQueryResultHandler;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.export.ASynExport;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.Doris;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.export.SynExport;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.instantiation.Instantiation;
import com.cn.jmw.processor.datasource.jdbc.inter.doris.pojo.ProcessListPojo;
import com.cn.jmw.processor.datasource.pojo.*;
import com.cn.jmw.processor.datasource.jdbc.dialect.Dialect;
import com.cn.jmw.processor.datasource.jdbc.dialect.SqlQueryBuilder;
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
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;
import static com.cn.jmw.common.exception.util.ServiceExceptionUtil.exception;

/**
 * Doris 数据库适配器
 * <p>
 * 该类扩展自 AbstractJdbcAdapter，提供与 Doris 数据库的连接和操作功能，支持高效随机抽样。
 * </p>
 *
 * @author Jmwang
 */
@Slf4j
public class DorisJdbcAdapter extends AbstractJdbcAdapter implements
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
        //Adbc协议
        Adbc {

    /**
     * DORIS_HTTP DORIS_JDBC ADBC_FE ADBC_BE
     */
    private static final int DORIS_HTTP_PORT = 8030;
    private static final int DORIS_JDBC_PORT = 9030;
    private static final int DORIS_ADBC_FE_PORT = 8815;
    private static final int DORIS_ADBC_BE_PORT = 8816;

    private final FlightSqlDriver driver;

    private final Map<String, Object> parameters = new HashMap<>();

    /**
     * 构造函数，初始化适配器实例。
     *
     * @param hostname       主机名
     * @param port           端口号
     * @param databaseName   数据库名称
     * @param username       用户名
     * @param password       密码
     * @param config         数据源配置
     * @param connectionUser 连接用户
     * @param test           是否为测试模式
     */
    public DorisJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, String connectionUser, Boolean test, ConnectionStringStrategy strategy) throws AdbcException {
        super(hostname, port, databaseName, username, password, config, connectionUser, test,strategy);

        final BufferAllocator allocator = new RootAllocator();
        this.driver = new FlightSqlDriver(allocator);
        AdbcDriver.PARAM_URI.set(parameters, Location.forGrpcInsecure(super.hostname, DORIS_ADBC_FE_PORT).getUri().toString());
        AdbcDriver.PARAM_USERNAME.set(parameters, super.username);
        AdbcDriver.PARAM_PASSWORD.set(parameters, super.password);
    }

    public DorisJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test, ConnectionStringStrategy strategy) throws AdbcException {
        this(hostname, port, databaseName, username, password, config, null, test, strategy);
    }

//    public DorisJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, JdbcAdapterDataSourceConfig config, Boolean test) throws AdbcException {
//        this(hostname, port, databaseName, username, password, config, null, test, new GeneratedConnectionStringStrategy(null));
//    }
//
//    public DorisJdbcAdapter(String hostname, Integer port, String databaseName, String username, String password, Boolean test) throws AdbcException {
//        this(hostname, port, databaseName, username, password, new JdbcAdapterDataSourceConfig(), null, test, new GeneratedConnectionStringStrategy(null));
//    }

    /**
     * 获取Aliyun RDS的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String getConnectionString() {
        return connectionStringStrategy.getConnectionString(hostname, port, databaseName, config);
    }

    /**
     * 获取Doris的连接字符串。
     *
     * @return 返回连接字符串，包含连接所需的参数
     */
    @Override
    public String generateConnectionString() {
        return String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&autoReconnect=true&nullCatalogMeansCurrent=true&connectTimeout=100000000&socketTimeout=600000000&autoReconnect=true",
                hostname, port, databaseName);
    }

    @Override
    public DatabaseEnum getDatabaseType() {
        return DatabaseEnum.DORIS;
    }

    /**
     * 使用批量查询数据。
     *
     * @param sql    执行的SQL语句
     * @param params SQL语句的参数
     * @return 查询结果，返回一个Map列表
     * @throws SQLException 如果发生SQL错误
     */
    @Override
    public List<Map<String, Object>> executeDMLC(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
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
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
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
     * @return boolean 是否执行成功
     * @throws SQLException 如果数据库访问错误或其他错误
     */
    @Override
    public boolean executeDDL(String sql, Object[] params) throws SQLException {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            // 设置参数（如果有）
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            return statement.execute();
        } catch (SQLException e) {
            throw new SQLException("执行DDL时出错: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("information_schema", "__internal_schema", "mysql", "test", "hello");
    }

    /**
     * 非全字段映射
     */
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 全字段映射
     */
    private static final ObjectMapper FULL_FIELD_OBJECT_MAPPER = new ObjectMapper();

    /*
      在将 LocalDateTime 对象转换为 JSON 时，ObjectMapper 默认会将其转换为一个包含年、月、日、小时、分钟、秒和纳秒的数组。
      这是因为 LocalDateTime 对象包含这些字段，而 ObjectMapper 默认会将对象的每个字段转换为 JSON 的一个元素。
      然而，Doris 的 DATETIME 类型需要的是一个格式为 'YYYY-MM-DD HH:MI:SS' 的字符串，而不是一个数组。
      因此，当你尝试将这个数组加载到 Doris 的 DATETIME 列时，会失败。
      为了解决这个问题，你需要告诉 ObjectMapper 使用一个特定的日期时间格式来序列化 LocalDateTime 对象。你可以使用 DateTimeFormatter 来定义这个格式，然后使用 JavaTimeModule 将这个格式器添加到 ObjectMapper。
     */
    static {
        JavaTimeModule module = new JavaTimeModule();
        module.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        // 用于在序列化空 Java Bean 时不抛出异常，即使对象中没有任何属性，也不会导致序列化失败。
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        // 反序列化时忽略未在目标类中定义的属性，避免由于 JSON 数据中有多余字段而抛出异常
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 解决 LocalDateTime 的序列化
        OBJECT_MAPPER.registerModule(module);
        // 驼峰命名
        OBJECT_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        // 忽略 null 值和空字符串
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        // 解决 LocalDateTime 的序列化
        FULL_FIELD_OBJECT_MAPPER.registerModule(module);
        FULL_FIELD_OBJECT_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    private final static HttpClientBuilder HTTP_CLIENT_BUILDER = HttpClients
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
     * <p>
     * 不推荐直接使用streamLoad，而是使用streamLoadBatch
     *
     * @param data      要加载的 JSON 数据 / CSV 路径 / ORC 路径 / Parquet 路径
     * @param tableName 要将数据加载到其中的表的名称
     * @param columns   列的名称关系
     * @throws IOException 如果发生 IO 错误
     */
    @NotRecommended()
    @Override
    public StreamLoadResult streamLoad(String data, String tableName, String columns, FileTypeEnum importFileType) throws IOException {
        String url = String.format("http://%s:%d/api/%s/%s/_stream_load", hostname, DORIS_HTTP_PORT, databaseName, tableName);
        String loadResult = "";

        try (CloseableHttpClient client = HTTP_CLIENT_BUILDER.build()) {
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
                /*
                  部分列更新

                  Doris 在主键模型的导入更新，提供了可以直接插入或者更新部分列数据的功能，不需要先读取整行数据，这样更新效率就大幅提升了。
                 */
                put.setHeader("partial_columns", "true");
                /*
                  设置 columns
                  指定导入文件中的列和 table 中的列的对应关系。
                 */
                put.setHeader("columns", columns);
            }

            // 设置导入文件。
            setStreamLoadEntity(put, data, importFileType);

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
            }catch (Exception e){
                e.printStackTrace();
                log.error("获取加载结果失败: {}", e.getMessage());
            }
        }
        //loadResult字符串转换成StreamLoadResult

        return OBJECT_MAPPER.readValue(loadResult, StreamLoadResult.class);
    }

    /**
     * 设置 Stream Load 的请求实体。
     */
    private void setStreamLoadEntity(HttpPut put, String data, FileTypeEnum importFileType) throws IOException {
        switch (importFileType) {
            case CSV:
                File file = new File(data);
                put.setEntity(new FileEntity(file, ContentType.create("text/csv", StandardCharsets.UTF_8)));
                break;
            case CSV_WITH_NAMES:
                // 暂无该功能使用，预留
                break;
            case JSON:
                put.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
                break;
            case ORC:
            case PARQUET:
                File dataFile = new File(data);
                put.setEntity(new FileEntity(dataFile, ContentType.APPLICATION_OCTET_STREAM));
                break;
            default:
                throw new IOException("不支持的文件格式: " + importFileType);
        }
    }

    @Override
    public boolean createRoutineLoad(String databaseName, String routineLoadName, String targetTableName,
                                     String columnsTerminatedBy, String columns, String kafkaBrokerList,
                                     String kafkaTopic, String groupId, String kafkaPartitions,
                                     String kafkaOffsets, FileTypeEnum fileTypeEnum) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement()) {

            // 构建 CREATE ROUTINE LOAD 语句
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("CREATE ROUTINE LOAD ").append(databaseName).append(".")
                    .append(routineLoadName).append(" ON ").append(targetTableName).append("\n");

            // 添加列分隔符（仅适用于 CSV_WITH_NAMES）
            if (FileTypeEnum.CSV == fileTypeEnum && StringUtils.isNotBlank(columnsTerminatedBy)) {
                stringBuilder.append("COLUMNS TERMINATED BY \"").append(columnsTerminatedBy).append("\",\n");
            }

            // 添加列信息
            if (StringUtils.isNotBlank(columns)) {
                stringBuilder.append("COLUMNS(").append(columns).append(")\n");
            }

            // 添加属性（仅适用于 JSON）
            if (FileTypeEnum.JSON == fileTypeEnum) {
                stringBuilder.append("PROPERTIES(")
                        .append("\"format\"=\"").append(fileTypeEnum.getName()).append("\",\n")
                        .append("\"max_error_number\" = \"9999999999\",\n")
                        .append("\"jsonpaths\"=\"").append(parseColumns(columns, columnsTerminatedBy)).append("\"\n")
                        .append(")\n");
            }

            //KAFKA配置项
            stringBuilder.append("FROM KAFKA(\n")
                    /*
                      考虑包装起来，从入参到内部参数

                      访问 SSL 认证的 Kafka 集群 property 参数示例
                      "property.security.protocol" = "ssl",
                      "property.ssl.ca.location" = "FILE:ca.pem",
                      "property.ssl.certificate.location" = "FILE:client.pem",
                      "property.ssl.key.location" = "FILE:client.key",
                      "property.ssl.key.password" = "ssl_passwd"

                      访问 PLAIN 认证的 Kafka 集群 property 参数示例
                      "property.security.protocol"="SASL_PLAINTEXT",
                      "property.sasl.mechanism"="PLAIN",
                      "property.sasl.username"="admin",
                      "property.sasl.password"="admin_passwd"

                      访问 Kerberos 认证的 Kafka 集群 property 参数示例
                      "property.security.protocol" = "SASL_PLAINTEXT",
                      "property.sasl.kerberos.service.name" = "kafka",
                      "property.sasl.kerberos.keytab" = "/etc/krb5.keytab",
                      "property.sasl.kerberos.principal" = "doris@YOUR.COM"
                     */
                    .append("\"kafka_broker_list\"=\"").append(kafkaBrokerList).append("\",\n")
                    .append("\"kafka_topic\"=\"").append(kafkaTopic).append("\",\n")
                    .append("\"kafka_group\"=\"").append(groupId).append("\",\n")
                    .append("\"kafka_partitions\"=\"").append(kafkaPartitions).append("\",\n")
                    .append("\"kafka_offsets\"=\"").append(kafkaOffsets).append("\"\n")
                    .append(");");

            return statement.execute(stringBuilder.toString());
        } catch (Exception e) {
            log.error("创建例程加载失败: {}", e.getMessage());
            throw new RuntimeException("创建例程加载失败", e);
        }
    }

    /**
     * 解析列信息，将列名转换为 JSON 字符串格式。
     *
     * @param columns             字段多个用逗号隔开
     * @param columnsTerminatedBy 列分隔符
     * @return 转换后的 JSON 字符串字面量
     * @throws JsonProcessingException 如果 JSON 处理失败
     */
    public static String parseColumns(String columns, String columnsTerminatedBy) throws JsonProcessingException {
        // 将列名分割并添加前缀
        List<String> collect = Arrays.stream(columns.split(columnsTerminatedBy))
                // 添加转义引号
                .map(column -> "$." + column)
                .collect(Collectors.toList());

        // 使用 ObjectMapper 将 List<String> 转换为 JSON 字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(collect);

        // 如果需要在 Java 字符串中表示这个 JSON 字符串字面量（包含转义的双引号）
        // 则需要对双引号进行转义

        // 注意：此时 jsonStringLiteral 不是一个有效的 JSON 字符串，
        // 它只是一个在 Java 字符串中表示 JSON 字符串字面量的字符串。
        // 如果您直接打印它，它将显示为转义后的形式。
        return json.replace("\"", "\\\"");
    }

    /**
     * 显示指定例程加载的状态。
     *
     * @param routineLoadName 例程加载的名称，若为空则显示所有例程加载
     * @return 返回例程加载结果列表
     */
    @Override
    public List<RoutineLoadResult> showRoutineLoadFor(String routineLoadName) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SHOW ROUTINE LOAD");
        if (StringUtils.isNotBlank(routineLoadName)) {
            queryBuilder.append(" FOR ").append(routineLoadName);
        }

        List<RoutineLoadResult> routineLoadResults = new ArrayList<>();
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(queryBuilder.toString())) {

            while (resultSet.next()) {
                RoutineLoadResult result = RoutineLoadResult.builder()
                        // 构建 RoutineLoadResult 对象
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
            log.error("获取例程加载状态失败: {}", e.getMessage());
            throw new RuntimeException("获取例程加载状态失败", e);
        }
        return routineLoadResults;
    }

    @Override
    public boolean determineCleaningBasedOnPartitionDataTableThreshold(List<String> dbNames, List<String> tableNames, List<Double> limitSizes, double threshold, List<Integer> partitionCleaningTime) {

        //获取所有表的信息，包括当前大小和行数
        //showTableStatus,获取到所有表的信息关于，curSizes当前大小 rowsCounts行数
        Map<String, ShowTableStatusResult> stringShowTableStatusResultMap = showTableStatus(null);

        // 当前时间
        LocalDateTime now = LocalDateTime.now();

        // 遍历每个表进行阈值清理
        for (int i = 0; i < tableNames.size(); i++) {
            //库名
            String dbName = dbNames.get(i);
            //表名
            String tableName = tableNames.get(i);
            //限制GB
            Double limitSize = limitSizes.get(i);
            //阈值之下
            double allowSize = limitSize * threshold;

            // 计算时间范围：partitionCleaningTime 天之前
            LocalDateTime endTime = now.minusDays(partitionCleaningTime.get(i));
            // 不限制开始时间，只查早于 endTime 的分区
            LocalDateTime startTime = null;

            // 查询早于 endTime 的分区
            List<ShowPartitionResult> showPartitionResults = showPartitions(dbName, tableName,startTime, endTime);
            if (showPartitionResults == null || showPartitionResults.isEmpty()) {
                log.info("数据表阈值清理————表名: {}, 无符合条件的分区（早于 {} 天）", tableName, partitionCleaningTime.get(i));
//                continue;
            }

            // 检查分区数量是否超过 partitionCleaningTime
            if (partitionCleaningTime.get(i) > 1 && showPartitionResults.size() > 0) {
                log.info("数据表阈值清理————表名: {}, 找到早于 {} 天的分区数量: {}",
                        tableName, partitionCleaningTime.get(i), showPartitionResults.size());

                // 删除所有早于 endTime 的分区
                for (ShowPartitionResult partition : showPartitionResults) {
                    String partitionName = partition.getPartitionName();
                    String sqlDelete = String.format("ALTER TABLE %s.%s DROP PARTITION %s FORCE", dbName, tableName, partitionName);

                    try {
                        executeDDL(sqlDelete, null);
                        log.info("数据表阈值清理————删除分区: {}, 表名: {}", partitionName, tableName);
                    } catch (SQLException e) {
                        log.error("数据表阈值清理————处理表: {}, 删除分区: {}, 异常信息: {}",
                                tableName, partitionName, e.getMessage());
                        return false;
                    }
                }
            }

            // 检查表大小并清理
            ShowTableStatusResult showTableStatusResult = stringShowTableStatusResultMap.get(tableName);
            if (showTableStatusResult == null) {
                log.info("数据表阈值清理————表名: {}, 失效表", tableName);
                continue;
            }

            // 转换为 GB
            double curSizes = showTableStatusResult.getDataLength() / 1024.0 / 1024.0 / 1024.0;
            if (curSizes < allowSize) {
                log.info("数据表阈值清理————表名: {}, 当前大小: {}GB, 允许大小: {}GB", tableName, curSizes, allowSize);
                continue;
            }

            // 按大小清理分区（从最早的分区开始）
            // 获取所有分区
            showPartitionResults = showPartitions(dbName, tableName, null, null);
            if (showPartitionResults == null || showPartitionResults.isEmpty()) {
                log.info("数据表阈值清理————表名: {}, 无分区数据", tableName);
                continue;
            }

            String sqlDelete = "ALTER TABLE %s.%s DROP PARTITION %s FORCE";
            double newCurSizes = curSizes;
            while (newCurSizes > allowSize && !showPartitionResults.isEmpty()) {
                ShowPartitionResult earliestPartition = showPartitionResults.get(0);
                String partitionName = earliestPartition.getPartitionName();

                try {
                    executeDDL(String.format(sqlDelete, dbName, tableName, partitionName), null);
                    log.info("数据表阈值清理————按大小删除分区: {}, 表名: {}", partitionName, tableName);

                    // 更新表大小
                    Map<String, ShowTableStatusResult> updatedStatus = showTableStatus(null);
                    ShowTableStatusResult updatedResult = updatedStatus.get(tableName);
                    if (updatedResult != null) {
                        newCurSizes = updatedResult.getDataLength() / 1024.0 / 1024.0 / 1024.0;
                    } else {
                        log.warn("数据表阈值清理————表名: {}, 无法获取更新后的表状态", tableName);
                        break;
                    }

                    // 更新分区列表
                    showPartitionResults.remove(0);
                } catch (SQLException e) {
                    log.error("数据表阈值清理————处理表: {}, 删除分区: {}, 异常信息: {}",
                            tableName, partitionName, e.getMessage());
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public boolean determineCleaningBasedOnWideTableThreshold(String dbName,String tableName,List<String> indexNames, List<Double> limitSizes, double threshold, List<Integer> partitionCleaningTime) {

        if (indexNames.size() != limitSizes.size()) {
            log.error("indexNames 和 limitSizes 长度不匹配");
            return false;
        }

        //获取所有表的信息，包括当前大小和行数
        //showTableStatus,获取到所有表的信息关于，curSizes当前大小 rowsCounts行数
        Map<String, ShowTableStatusResult> stringShowTableStatusResultMap = showWideTableStatus(tableName, null);

        // 当前时间
        LocalDateTime now = LocalDateTime.now();

        // 遍历每个索引
        for (int i = 0; i < indexNames.size(); i++) {
            //检查每个宽表分区索引
            String indexName =  indexNames.get(i);
            //限制GB
            Double limitSize = limitSizes.get(i);
            //阈值之下
            double allowSize = limitSize * threshold;

            // 计算时间范围：partitionCleaningTime 天之前
            LocalDateTime endTime = now.minusDays(partitionCleaningTime.get(i));
            // 不限制开始时间，只查早于 endTime 的分区
            LocalDateTime startTime = null;

            // 查询早于 endTime 的分区
            List<ShowPartitionResult> showPartitionResults = showWideTablePartitions(dbName, tableName,indexName,startTime, endTime);
            if (showPartitionResults == null || showPartitionResults.isEmpty()) {
                log.info("数据表阈值清理————索引名: {}, 无符合条件的分区（早于 {} 天）", indexName, partitionCleaningTime.get(i));
//                continue;
            }

            // 检查分区数量是否超过 partitionCleaningTime
            if (partitionCleaningTime.get(i) > 0 && showPartitionResults.size() > 0) {
                log.info("数据表阈值清理————索引名: {}, 找到早于 {} 天的分区数量: {}",
                        indexName, partitionCleaningTime.get(i), showPartitionResults.size());

                // 删除所有早于 endTime 的分区
                for (ShowPartitionResult partition : showPartitionResults) {
                    String partitionName = partition.getPartitionName();
                    String sqlDelete = String.format("ALTER TABLE %s.%s DROP PARTITION %s FORCE", dbName, tableName, partitionName);

                    try {
                        executeDDL(sqlDelete, null);
                        log.info("数据表阈值清理————删除分区: {}, 索引名: {}", partitionName, indexName);
                    } catch (SQLException e) {
                        log.error("数据表阈值清理————处理索引: {}, 删除分区: {}, 异常信息: {}",
                                indexName, partitionName, e.getMessage());
                        return false;
                    }
                }
            }

            // 检查表大小并清理
            ShowTableStatusResult showTableStatusResult = stringShowTableStatusResultMap.get(tableName);
            if (showTableStatusResult == null) {
                log.info("数据表阈值清理————索引名: {}, 失效表", indexName);
                continue;
            }

            // 转换为 GB
            double curSizes = showTableStatusResult.getDataLength() / 1024.0 / 1024.0 / 1024.0;
            if (curSizes < allowSize) {
                log.info("数据表阈值清理————索引名: {}, 当前大小: {}GB, 允许大小: {}GB", indexName, curSizes, allowSize);
                continue;
            }

            // 按大小清理分区（从最早的分区开始）
            // 获取所有分区
            showPartitionResults = showWideTablePartitions(dbName, tableName, indexName,null, null);
            if (showPartitionResults == null || showPartitionResults.isEmpty()) {
                log.info("数据表阈值清理————索引名: {}, 无分区数据", indexName);
                continue;
            }

            showPartitionResults.sort(Comparator.comparing(p -> parsePartitionDate(p.getPartitionName(), indexName)));
            String sqlDelete = "ALTER TABLE %s.%s DROP PARTITION %s FORCE";
            double newCurSizes = curSizes;
            while (newCurSizes > allowSize && !showPartitionResults.isEmpty()) {
                ShowPartitionResult earliestPartition = showPartitionResults.get(0);
                String partitionName = earliestPartition.getPartitionName();
                sqlDelete = String.format(sqlDelete, dbName, tableName, partitionName);

                try {
                    executeDDL(sqlDelete, null);
                    log.info("数据表阈值清理————按大小删除分区: {}, 索引名: {}", partitionName, indexName);

                    // 更新表大小
                    Map<String, ShowTableStatusResult> updatedStatus = showWideTableStatus(tableName,indexName);
                    ShowTableStatusResult updatedResult = updatedStatus.get(tableName);
                    if (updatedResult != null) {
                        newCurSizes = updatedResult.getDataLength() / 1024.0 / 1024.0 / 1024.0;
                    } else {
                        log.warn("数据表阈值清理————索引名: {}, 无法获取更新后的表状态", indexName);
                        break;
                    }

                    // 更新分区列表
                    showPartitionResults.remove(0);
                } catch (SQLException e) {
                    log.error("数据表阈值清理————处理索引: {}, 删除分区: {}, 异常信息: {}",
                            indexName, partitionName, e.getMessage());
                    return false;
                }
            }
        }

        return false;
    }

    @Override
    public List<ShowPartitionResult> showWideTablePartitions(String tableSchema, String tableName, String indexName, LocalDateTime startTime, LocalDateTime endTime) {
        List<ShowPartitionResult> partitions = new ArrayList<>();

        // 动态添加 updateTime 条件
        List<Object> params = new ArrayList<>();

        // 基础 SQL
        StringBuilder sql = new StringBuilder(
                "SELECT\n" +
                        "    STR_TO_DATE(REGEXP_EXTRACT(PARTITION_DESCRIPTION, \"'(\\\\d{4}-\\\\d{2}-\\\\d{2})'\", 1),'%Y-%m-%d') AS partition_date,\n" +
                        "    REGEXP_EXTRACT(PARTITION_DESCRIPTION, '\"([^\"]*)\"', 1) AS partition_value,\n" +
                        "    PARTITION_NAME\n" +
                        "FROM\n" +
                        "    information_schema.partitions " +
                        "WHERE\n" +
                        "    table_schema = '"+tableSchema+"'" +
                        "    AND table_name = '"+tableName+"'"
        );

        sql.append("GROUP BY\n" +
                "    partition_date, partition_value,PARTITION_NAME\n" +
                "HAVING\n" +
                "    partition_value = '"+indexName+"'");

        if (startTime != null) {
            sql.append(" AND partition_date >= ?");
            params.add(startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }
        if (endTime != null) {
            sql.append(" AND partition_date <= ?");
            params.add(endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }

        sql.append(" ORDER BY\n" +
                "    partition_date ASC;");

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql.toString())) {

            // 设置参数
            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i + 1, params.get(i));
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ShowPartitionResult partition = ShowPartitionResult.builder()
                            .tableName(resultSet.getString("partition_value"))
                            .partitionName(resultSet.getString("PARTITION_NAME"))
                            .createTime(resultSet.getString("partition_date"))
                            .build();

                    partitions.add(partition);
                }
            }
        } catch (SQLException e) {
            log.error("数据表阈值清理——表名: {} ,showPartitions异常信息: {}", tableName, e.getMessage());
        }

        return partitions;
    }

    /**
     * 查看分区表的分区信息。
     * <p>
     * 获取到分区表的最早和最迟进入库中的数据天数，以及对应的PartitionKey字段。
     *
     * @param tableSchema      数据库名称
     * @param tableName   表名称
     * @param startTime   开始时间
     * @param endTime     结束时间
     * @return 最早和最迟进入库中的数据天数差
     */
    @Override
    public List<ShowPartitionResult> showPartitions(String tableSchema, String tableName ,LocalDateTime startTime,LocalDateTime endTime) {
        List<ShowPartitionResult> partitions = new ArrayList<>();

        // 动态添加 updateTime 条件
        List<Object> params = new ArrayList<>();

        // 基础 SQL
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM INFORMATION_SCHEMA.PARTITIONS " +
                        "WHERE TRUE "
        );

        if (tableSchema != null && tableName != null){
            sql.append("AND TABLE_SCHEMA = ? AND TABLE_NAME = ?");
            params.add(tableSchema);
            params.add(tableName);
        }

        if (startTime != null) {
            sql.append(" AND STR_TO_DATE(REGEXP_EXTRACT(PARTITION_DESCRIPTION, \"'(\\\\d{4}-\\\\d{2}-\\\\d{2}) \\\\d{2}:\\\\d{2}:\\\\d{2}'\", 1), '%Y-%m-%d') >= ?");
            params.add(startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }
        if (endTime != null) {
            sql.append(" AND STR_TO_DATE(REGEXP_EXTRACT(PARTITION_DESCRIPTION, \"'(\\\\d{4}-\\\\d{2}-\\\\d{2}) \\\\d{2}:\\\\d{2}:\\\\d{2}'\", 1), '%Y-%m-%d') <= ?");
            params.add(endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }

        sql.append(" ORDER BY PARTITION_NAME ASC");

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql.toString())) {

            // 设置参数
            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i + 1, params.get(i));
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ShowPartitionResult partition = ShowPartitionResult.builder()
                            .tableCatalog(resultSet.getString("TABLE_CATALOG"))
                            .tableSchema(resultSet.getString("TABLE_SCHEMA"))
                            .tableName(resultSet.getString("TABLE_NAME"))
                            .partitionName(resultSet.getString("PARTITION_NAME"))
                            .subPartitionName(resultSet.getString("SUBPARTITION_NAME"))
                            .partitionOrdinalPosition(resultSet.getInt("PARTITION_ORDINAL_POSITION"))
                            .subPartitionOrdinalPosition(resultSet.getInt("SUBPARTITION_ORDINAL_POSITION"))
                            .partitionMethod(resultSet.getString("PARTITION_METHOD"))
                            .subPartitionMethod(resultSet.getString("SUBPARTITION_METHOD"))
                            .partitionExpression(resultSet.getString("PARTITION_EXPRESSION"))
                            .subPartitionExpression(resultSet.getString("SUBPARTITION_EXPRESSION"))
                            .partitionDescription(resultSet.getString("PARTITION_DESCRIPTION"))
                            .tableRows(resultSet.getLong("TABLE_ROWS"))
                            .avgRowLength(resultSet.getLong("AVG_ROW_LENGTH"))
                            .dataLength(resultSet.getLong("DATA_LENGTH"))
                            .maxDataLength(resultSet.getLong("MAX_DATA_LENGTH"))
                            .indexLength(resultSet.getLong("INDEX_LENGTH"))
                            .dataFree(resultSet.getLong("DATA_FREE"))
                            .createTime(resultSet.getString("CREATE_TIME"))
                            .updateTime(resultSet.getString("UPDATE_TIME"))
                            .checkTime(resultSet.getString("CHECK_TIME"))
                            .checksum(resultSet.getLong("CHECKSUM"))
                            .partitionComment(resultSet.getString("PARTITION_COMMENT"))
                            .nodeGroup(resultSet.getString("NODEGROUP"))
                            .tablespaceName(resultSet.getString("TABLESPACE_NAME"))
                            .build();

                    partitions.add(partition);

                }
            }
        } catch (SQLException e) {
            log.error("数据表阈值清理——表名: {} ,showPartitions异常信息: {}", tableName, e.getMessage());
        }

        return partitions;
    }

    /**
     * 验证分区名是否符合格式。
     * 格式：P<YYYYMMDD|YYYY2dMM2dDD>[0-5位数字]<indexName>，时间必须是有效日期。
     *
     * @param partitionName 分区名
     * @param indexName     期望的后缀
     * @return 是否符合格式且时间有效，true 表示符合，false 表示不符合
     */
    private LocalDateTime parsePartitionDate(String partitionName, String indexName) {
        if (partitionName == null || indexName == null || (!partitionName.startsWith("p"))) {
            return null;
        }

        // 正则表达式：p(YYYYMMDD|YYYY2dMM2dDD)(\d{0,5})?_indexName
        String regex = "^p\\d{8}|\\d{4}2d\\d{2}2d\\d{2})(\\d*)?" + Pattern.quote(indexName) + "$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(partitionName);

        if (!matcher.matches()) {
            log.warn("分区名格式不正确: {}, 期望格式: p<YYYYMMDD|YYYY2dMM2dDD>[0-5位数字]<indexName>", partitionName);
            return null;
        }

        try {
            // 提取时间部分
            String datePart = matcher.group(1);
            if (datePart.contains("2d")) {
                // 格式：YYYY2dMM2dDD
                String[] parts = datePart.split("2d");
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                // 验证是否为有效日期
                // 若无效会抛异常
                return LocalDateTime.of(year, month, day, 0, 0, 0);
            } else {
                // 格式：YYYYMMDD
                int year = Integer.parseInt(datePart.substring(0, 4));
                int month = Integer.parseInt(datePart.substring(4, 6));
                int day = Integer.parseInt(datePart.substring(6, 8));
                // 验证是否为有效日期
                // 若无效会抛异常
                return LocalDateTime.of(year, month, day, 0, 0, 0);
            }
        } catch (DateTimeException | NumberFormatException e) {
            log.warn("分区名时间无效: {}, 错误: {}", partitionName, e.getMessage());
            return null;
        }
    }

    @Override
    public String getQueryIdList(String databaseName, String likeSql) {
        return "";
    }

    @Override
    public List<ProcessListPojo> getProcessList(ProcessListPojo processListPojo) {
        List<ProcessListPojo> processList = new ArrayList<>();
        SqlQueryBuilder sqlQueryBuilder = new SqlQueryBuilder();

        // 设置查询的表名和数据库名
        sqlQueryBuilder.tableName("PROCESSLIST").dbName("information_schema");

        // 添加查询条件
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

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {

            // 处理查询结果
            while (resultSet.next()) {
                ProcessListPojo process = ProcessListPojo.builder()
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
                processList.add(process);
            }
        } catch (SQLException e) {
            log.error("获取进程列表失败，异常信息: {}", e.getMessage());
        }
        return processList;
    }

    @Override
    public boolean killQuery(String queryId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement("KILL QUERY '" + queryId + "';")) {
            return statement.execute();
        } catch (SQLException e) {
            log.error("删除查询任务失败，异常信息: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean killConnection(Integer connectionId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             PreparedStatement statement = connection.prepareStatement("KILL CONNECTION " + connectionId + "';")) {
            return statement.execute();
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
        return OBJECT_MAPPER.writeValueAsString(list);
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
        if (total == 0) {
            return new StreamLoadResult[0];
        }

        // 计算批次数
        int batchCount = (total + size - 1) / size;
        StreamLoadResult[] results = new StreamLoadResult[batchCount];

        for (int i = 0; i < batchCount; i++) {
            int start = i * size;
            // 确保不超出列表范围
            int end = Math.min(start + size, total);
            List<?> subList = list.subList(start, end);
            results[i] = streamLoad(listToJson(subList), tableName, columns, importFileType);
        }

        return results;
    }

    /**
     * 将一个大 JSON 数组分成指定大小进行流加载。
     *
     * @param jsonArray      要加载的 JSON 数组
     * @param tableName      要将数据加载到其中的表的名称
     * @param size           每批次的大小
     * @param columns        列的名称关系
     * @param importFileType 导入文件类型
     * @return StreamLoadResult数组
     * @throws IOException 如果发生 IO 错误
     */
    public StreamLoadResult[] streamLoadBatch(JSONArray jsonArray, String tableName, int size, String columns, FileTypeEnum importFileType) throws IOException {
        int total = jsonArray.size();
        if (total == 0) {
            return new StreamLoadResult[0];
        }

        // 计算批次数
        int batchCount = (total + size - 1) / size;
        StreamLoadResult[] results = new StreamLoadResult[batchCount];

        for (int i = 0; i < batchCount; i++) {
            int start = i * size;
            // 确保不超出数组范围
            int end = Math.min(start + size, total);
            // 获取子数组
            List<?> subArray = jsonArray.subList(start, end);
            // 执行流加载
            results[i] = streamLoad(listToJson(subArray), tableName, columns, importFileType);
        }

        return results;
    }

    @Override
    public String buildSql(SqlQueryBuilder queryBuilder) {
        // 构建并返回 SQL 查询字符串
        return queryBuilder.buildSQL();
    }

    @Override
    public Object instantiate(String databaseName, String tableName) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
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

            DynamicType.Unloaded<?> unloadedType = builder.make();
//            unloadedType.saveIn(new File("C:\\Users\\79283\\IdeaProjects\\jt-bds-base2333\\bds-framework\\bds-spring-boot-starter-structured\\target\\classes")); // 保存 .class 文件到指定目录

            Class<?> loadedClass;
            try {
                loadedClass = Class.forName(unloadedType.getTypeDescription().getName());
            } catch (ClassNotFoundException e) {
                loadedClass = unloadedType.load(getClass().getClassLoader()).getLoaded();
            }

            // 使用无参构造器实例化
            return loadedClass.getDeclaredConstructor().newInstance();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void aSynExport(String dbName, String tableName, String where, String path, String columns, String jobId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement()) {

            // 构建并返回 SQL 查询字符串
            StringBuilder sql = new StringBuilder("EXPORT TABLE " + dbName + "." + tableName + " \n");

            // 添加 WHERE 条件
            if (StringUtils.isNotBlank(where)) {
                sql.append("WHERE ").append(where).append("\n");
            }

            // 设置默认路径
            if (StringUtils.isBlank(path)) {
                path = "/tmp/";
            }

            // 添加 TO 和 PROPERTIES 子句
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

            // 执行 SQL 语句
            statement.execute(sql.toString());
        } catch (Exception e) {
            throw new RuntimeException("异步导出失败: " + e.getMessage(), e);
        }
    }

    @Override
    public ShowExport queryAsynExportJobStatus(String dbName, String label) {
        //查询结果封装成ShowExport类
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW EXPORT FROM " + dbName + " WHERE LABEL like '%" + label + "%'")) {

            if (resultSet.next()) {
                // 封装查询结果到 ShowExport 类
                return ShowExport.builder()
                        .state(resultSet.getString("STATE"))
                        .progress(resultSet.getString("PROGRESS"))
                        .taskInfo(resultSet.getString("TASKINFO"))
                        .path(resultSet.getString("PATH"))
                        .createTime(resultSet.getString("CREATETIME"))
                        .startTime(resultSet.getString("STARTTIME"))
                        .finishTime(resultSet.getString("FINISHTIME"))
                        .timeout(resultSet.getString("TIMEOUT"))
                        .errorMsg(resultSet.getString("ERRORMSG"))
                        .outfileInfo(resultSet.getString("OUTFILEINFO"))
                        .build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new ShowExport();
    }

    @Override
    public List<ShowExport> queryAsynExportJobStatus(String dbName) {
        List<ShowExport> showExports = new ArrayList<>();
        // 查询所有导出任务状态
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW EXPORT FROM " + dbName)) {

            while (resultSet.next()) {
                // 封装查询结果到 ShowExport 类
                ShowExport build = ShowExport.builder()
                        .state(resultSet.getString("STATE"))
                        .progress(resultSet.getString("PROGRESS"))
                        .taskInfo(resultSet.getString("TASKINFO"))
                        .path(resultSet.getString("PATH"))
                        .createTime(resultSet.getString("CREATETIME"))
                        .startTime(resultSet.getString("STARTTIME"))
                        .finishTime(resultSet.getString("FINISHTIME"))
                        .timeout(resultSet.getString("TIMEOUT"))
                        .errorMsg(resultSet.getString("ERRORMSG"))
                        .outfileInfo(resultSet.getString("OUTFILEINFO"))
                        .build();
                showExports.add(build);
            }
        } catch (Exception e) {
            throw new RuntimeException("查询所有异步导出任务状态失败: " + e.getMessage(), e);
        }
        // 返回所有导出任务状态
        return showExports;
    }

    @Override
    public void stopAsynExportJob(String jobId) {
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement()) {
            String sql = "CANCEL EXPORT FROM tpch1 WHERE LABEL like \"%" + jobId + "%\";";
            statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException("停止异步导出任务失败: " + e.getMessage(), e);
        }
    }

    @Override
    public IntoOutFile synExport(String sql, String path, String maximumFileSize, FileTypeEnum fileTypeEnum) {
        // 使用正则表达式来处理嵌套的 SELECT 语句
        Pattern pattern = Pattern.compile("(?i)^\\s*(\\(*\\s*)select(\\s+)");
        Matcher matcher = pattern.matcher(sql);

        StringBuilder sqlBuilder = new StringBuilder();

        if (matcher.find()) {
            // 获取匹配的左括号部分
            String matched = matcher.group(1);
            // 计算左括号数量
            int leftParenthesesCount = matched.length() - matched.replace("(", "").length();
            String replacement = "SELECT /*+ SET_VAR(query_timeout = 300) */ ";

            // 根据左括号数量构造新的 SQL 语句
            sqlBuilder.append("(".repeat(leftParenthesesCount)).append(replacement).append(sql.substring(matcher.end()));
        } else {
            throw new ServiceException();
        }

        if (fileTypeEnum == fileTypeEnum.CSV) {
            sqlBuilder.append(" INTO OUTFILE \"file://").append(path).append("\"\n")
                    .append("FORMAT AS ").append(FileTypeEnum.CSV_WITH_NAMES);
        } else {
            sqlBuilder.append(" INTO OUTFILE \"file://").append(path).append("\"\n")
                    .append("FORMAT AS ").append(fileTypeEnum);
        }

        sqlBuilder.append("\n")
                .append("PROPERTIES(\n");

        if (fileTypeEnum == fileTypeEnum.CSV) {
            sqlBuilder.append("\"column_separator\" = \",\",\n")
                    .append("\"line_delimiter\" = \"\\n\",\n");
        }

        sqlBuilder.append("\"max_file_size\" = \"" + maximumFileSize + "\",\n")
                .append("\"with_bom\" = \"true\"\n")
                .append(");");

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sqlBuilder.toString())) {
            while (resultSet.next()) {
                Integer fileNumber = resultSet.getInt("FileNumber");
                Integer totalRows = resultSet.getInt("TotalRows");
                Long fileSize = resultSet.getLong("FileSize");
                String url = resultSet.getString("URL");
                return IntoOutFile.builder()
                        .fileNumber(fileNumber)
                        .totalRows(totalRows)
                        .fileSize(fileSize)
                        .url(url)
                        .build();
            }
        } catch (Exception e) {
            return IntoOutFile.builder()
                    .errorMsg(e.getMessage())
                    .build();
        }
        return new IntoOutFile();
    }

    @Override
    public ShowCreateTable getCreateTableDDL(String dbName, String table) {
        // 通过语句查询表的创建DDL
        String sql = "SHOW CREATE TABLE " + dbName + "." + table + ";";
        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                String createTableDDL = resultSet.getString("Create Table");
                String tableCur = resultSet.getString("Table");
                return ShowCreateTable.builder()
                        .createTable(createTableDDL)
                        .table(tableCur)
                        .build();
            }
        } catch (Exception e) {
            throw new RuntimeException("获取创建表DDL失败: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 使用ADBC协议 流批执行查询操作
     *
     * @param sql     要执行的SQL查询语句
     * @param handler 处理结果集的处理器
     * @param <T>     返回类型
     * @return 查询结果，由ResultSetHandler处理
     * @throws Exception 如果数据库访问错误或其他错误
     */
    @Override
    public <T> T queryStreamAdbc(String sql, AdbcQueryResultHandler<T> handler) throws Exception {
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
                    Map<String, Object> row = new HashMap<>(16);
                    for (FieldVector fieldVector : fieldVectors) {
                        row.put(fieldVector.getField().getName(), fieldVector.getObject(i));
                    }
                    batchResults.add(row);
                }

                // 调用回调处理器
                result = handler.handle(batchResults);
                // 清空批次数据
                batchResults.clear();
            }

            reader.close();
            queryResult.close();
        }

        return result;
    }


    @Override
    public <T> T jdbcConnectWithArrowFlightSql(String sql, ResultSetHandler<T> handler) throws Exception {
        T result;
        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
        String dbUrl = "jdbc:arrow-flight-sql://" + hostname + ":" + DORIS_ADBC_FE_PORT
                + "?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false";
        String user = username;
        String pass = password;

        try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
             Statement stmt = conn.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql)) {

            // 直接调用 handler 处理整个 ResultSet
            result = handler.handle(resultSet);
        }

        // 返回处理后的结果
        return result;
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
        return switch (sqlType) {
            case "VARCHAR", "CHAR", "LONGVARCHAR" -> String.class;
            case "NUMERIC", "DECIMAL" -> java.math.BigDecimal.class;
            case "BIT" -> Boolean.class;
            case "TINYINT" -> Byte.class;
            case "SMALLINT" -> Short.class;
            case "INTEGER" -> Integer.class;
            case "BIGINT" -> Long.class;
            case "REAL" -> Float.class;
            case "FLOAT", "DOUBLE" -> Double.class;
            case "BINARY", "VARBINARY", "LONGVARBINARY" -> byte[].class;
            case "DATE" -> Date.class;
            case "TIME" -> Time.class;
            case "TIMESTAMP" -> Timestamp.class;
            default -> Object.class;
        };
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
     * 添加高效随机抽样逻辑。
     * <p>
     * Doris 支持 SAMPLE 关键字，用于高效随机抽样，优于伪抽样。本方法根据总数计算抽样比例并生成相应 SQL。
     * </p>
     *
     * @param sqlQueryMontage SQL 查询组合对象
     * @param n               抽样点（未使用）
     * @param m               抽样数量
     * @return 包含高效随机抽样逻辑的 SQL 列表
     */
    @Override
    public List<String> addRandomSampling(SqlQueryMontage sqlQueryMontage, int n, int m) {
        Optional<SampleResult> optionalSampleResult = sqlQueryMontage.getSampleResult();
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleMethod("随机"));

        // 初始化返回结果
        List<String> resultList = new ArrayList<>();

        // 获取 SQL 构建器
        SqlQueryBuilder sqlQueryBuilder = sqlQueryMontage.getSqlQueryBuilders().getFirst();
        if (sqlQueryBuilder == null) {
            throw exception(RANDOM_SAMPLING_ERROR);
        }

        // 构建原始 SQL
        String sql = sqlQueryBuilder.buildSQL();

        // 检查 SQL 中是否包含不允许的关键字
        Matcher matcher = RANDOM_SAMPLING_COMPILE.matcher(sql);
        if (matcher.find()) {
            throw exception(RANDOM_SAMPLING_NOT_ALLOW_KEYWORD);
        }

        // 构建查询总数的 SQL
        String countSql = sql.replaceAll("(?i)SELECT\\s+.*?\\s+FROM", "SELECT COUNT(1) FROM").replaceAll("order by.+", ")");

        // 如果 sampleResult 存在，则设置抽样时间
        optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleTime(LocalDateTime.now()));

        try {
            // 执行查询获取总数
            List<Map<String, Object>> countResult = executeDMLC(countSql, null);
            long totalCount = countResult.isEmpty() ? 0 : (Long) countResult.getFirst().get("count(1)");

            // 检查参数有效性
            if (n <= 0 || totalCount == 0 || m <= 0) {
                optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(0));
                return resultList;
            }

            // 如果总数小于抽样数量 m，则返回完整 SQL
            if (totalCount < m) {
                resultList.add(sql);
                optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(totalCount));
                return resultList;
            }

            // 设置抽样数量
            optionalSampleResult.ifPresent(sampleResult -> sampleResult.setSampleCount(m));

            String randomSampling = ") LIMIT " + m;
            if (sql.contains("order by")) {
                sql = sql.replaceAll("order by.+", randomSampling);
            } else {
                sql = sql.substring(0, sql.length() - 1) + randomSampling;
            }

            resultList.add(sql);
            //TODO 需要测试.是否能高效随机抽样 并且确定那些版本号支持方法
//            double sampleRatio = (double) m / totalCount;
//            String randomSampling = " SAMPLE " + sampleRatio;
//            resultList.add(sql + randomSampling);

        } catch (SQLException e) {
            log.error("获取总记录数失败: {}", e.getMessage(), e);
            return resultList;
        }
        return resultList;
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
        Map<String, Map<String, Object>> indexInfoList = new HashMap<>(16);
        try (ResultSet indexInfo = connection.getMetaData().getIndexInfo(null, dbName, tableName, false, false)) {

            while (indexInfo.next()) {
                // 处理索引信息
                Map<String, Object> indexDetails = new HashMap<>(16);
                String columnName = indexInfo.getString("COLUMN_NAME");
                String indexType = indexInfo.getString("TYPE");
                indexDetails.put("INDEX_TYPE", indexType);
                indexDetails.put("INDEX_NAME", indexInfo.getString("INDEX_NAME"));
                indexDetails.put("COLUMN_NAME", columnName);
                indexDetails.put("IS_UNIQUE", !indexInfo.getBoolean("NON_UNIQUE"));

                indexInfoList.put(columnName, indexDetails);
            }
        } catch (SQLException e) {
            log.error("获取索引信息失败: {}", e.getMessage());
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
     */
    @Override
    public List<DatabaseEntity> getDatabaseMetadata() {
        return getDatabaseMetadata(null);
    }

    @Override
    public List<DatabaseEntity> getDatabaseMetadata(String dbName) {
        List<DatabaseEntity> databaseEntities = new ArrayList<>();
        // 获取需要忽略的数据库列表
        List<String> ignoreDatabases = getIgnoreDatabaseList();

        try (Connection connection = pool.getConnection(hostname + port + databaseName + username + password, config, connectionUser)) {
            DatabaseMetaData metaData = connection.getMetaData();

            try (ResultSet catalogs = connection.getMetaData().getCatalogs()) {
                while (catalogs.next()) {

                    String databaseName = catalogs.getString("TABLE_CAT");

                    // 跳过忽略的数据库或不匹配的数据库
                    if (ignoreDatabases.contains(databaseName) ||
                            (StringUtils.isNotBlank(dbName) && !dbName.equals(databaseName))) {
                        continue;
                    }

                    // 创建数据库实体并填充表信息
                    DatabaseEntity databaseEntity = new DatabaseEntity();
                    databaseEntity.setDatabaseName(databaseName);
                    // 存入 DatabaseType
                    databaseEntity.setDatabaseEnum(getDatabaseType());

                    Map<String, TableEntity> tableEntities = new HashMap<>(16);
                    try (PreparedStatement preparedStatement = connection.prepareStatement("USE " + databaseName);
                         ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {

                        preparedStatement.execute();
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");

                            // 跳过物化视图
                            if (tableName.contains("materialized_view")) {
                                continue;
                            }

                            // 查索引
                            Map<String, Map<String, Object>> indexInfo = getIndexInfo(connection, databaseName, tableName);

                            TableEntity tableEntity = new TableEntity();
                            tableEntity.setTableName(tableName);
                            // 获取表注释
                            tableEntity.setTableComment(tables.getString("REMARKS"));

                            Map<String, ColumnEntity> columnEntities = new HashMap<>(16);
                            // 使用 try-with-resources 确保 ResultSet 被关闭
                            try (ResultSet cols = metaData.getColumns(databaseName, null, tableName, "%")) {
                                while (cols.next()) {
                                    ColumnEntity columnEntity = new ColumnEntity();
                                    String typeName = cols.getString("TYPE_NAME");
                                    // 获取列名
                                    columnEntity.setColumnName(cols.getString("COLUMN_NAME"));
                                    // 获取列类型
                                    columnEntity.setColumnType(typeName);
                                    // 获取列大小
                                    columnEntity.setColumnSize(cols.getInt("COLUMN_SIZE"));
                                    // 获取可否为NULL
                                    columnEntity.setNullable(cols.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
                                    // 获取默认值
                                    columnEntity.setDefaultValue(cols.getString("COLUMN_DEF"));
                                    // 获取自增状态
                                    columnEntity.setAutoIncrement("YES".equals(cols.getString("IS_AUTOINCREMENT")) ? 1 : 0);
                                    // 获取列注释
                                    columnEntity.setColumnComment(cols.getString("REMARKS"));

                                    // 判断列类型
                                    columnEntity.setType(getColumnType(typeName));

                                    // 是否是索引
                                    if (indexInfo.containsKey(cols.getString("COLUMN_NAME"))) {
                                        columnEntity.setIsIndex(1);
                                    }

                                    columnEntities.put(columnEntity.getColumnName(), columnEntity);
                                }
                            } catch (SQLException e) {
                                log.error("获取列信息失败: {}", e.getMessage());
                                // 抛出无法检索数据库元数据的异常
//                                throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
                                continue;
                            }
                            // 设置表的列信息
                            tableEntity.setColumns(columnEntities);
                            tableEntities.put(tableName, tableEntity);
                        }
                    } catch (SQLException e) {
                        log.error("获取列信息失败: {}", e.getMessage());
                        // 抛出无法检索数据库元数据的异常
//                        throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
                        continue;
                    }
                    // 设置数据库中的表信息
                    databaseEntity.setTables(tableEntities);
                    databaseEntities.add(databaseEntity);
                }
            } catch (SQLException e) {
                log.error("获取目录失败: {}", e.getMessage());
                // 抛出无法检索数据库元数据的异常
                throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
            }
        } catch (SQLException e) {
            log.error("建立或者获取连接失败: {}", e.getMessage());
            // 抛出无法检索数据库元数据的异常
            throw exception(UNABLE_TO_RETRIEVE_DATABASE_METADATA);
        }
        return databaseEntities;
    }

    /**
     * 1字符串 2数字 0时间
     * <p>
     * 判断输入的字符串名称是什么类型的数据字段
     */
    public int getColumnType(String columnType) {
        return switch (columnType) {
            case "CHAR", "VARCHAR", "STRING" -> 1;
            case "BIGINT", "INT", "LARGEINT" -> 2;
            case "DATE", "DATETIME" -> 0;
            default -> 3;
        };
    }

}
