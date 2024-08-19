package com.cn.jmw.processor.datasource.jdbc.adapter;

import com.cn.jmw.processor.datasource.JDBCAdapter;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.enums.FileTypeEnum;
import com.cn.jmw.processor.datasource.export.ASynExport;
import com.cn.jmw.processor.datasource.export.SynExport;
import com.cn.jmw.processor.datasource.factory.DatabaseAdapterFactory;
import com.cn.jmw.processor.datasource.instantiation.Instantiation;
import com.cn.jmw.processor.datasource.jdbc.adapter.pojo.IntoOutFile;
import com.cn.jmw.processor.datasource.jdbc.adapter.pojo.ShowExport;
import com.cn.jmw.processor.datasource.jdbc.dialect.Dialect;
import com.cn.jmw.processor.datasource.jdbc.dialect.SQLQueryBuilder;
import com.cn.jmw.processor.datasource.pojo.JDBCConnectionEntity;
import com.cn.jmw.processor.datasource.pojo.StreamLoadResult;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class DorisJDBCAdapter extends JDBCAdapter implements Dialect, Instantiation, SynExport, ASynExport {

    // DORIS HTTP PORT
    private static final int DORIS_HTTP_PORT = 8030;

    /**
     * 构造函数用于创建DorisJDBCAdapter实例。
     *
     * @param hostname     Doris服务器的主机名
     * @param port         Doris服务器的端口号
     * @param databaseName 数据库名称
     * @param username     用户名
     * @param password     密码
     */
    public DorisJDBCAdapter(String hostname, Integer port, String databaseName, String username, String password) {
        super(hostname, port, databaseName, username, password);
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
                "&connectTimeout=100000&socketTimeout=600000&autoReconnect=true";
    }

    /**
     * 使用批量查询数据。
     *
     * @param sql    执行的SQL语句
     * @param params SQL语句的参数
     * @return 查询结果，返回一个Map列表
     * @throws SQLException 如果发生SQL错误
     */
    public List<Map<String, Object>> queryBatch(String sql, Object[] params) throws SQLException {
        return super.runner.query(pool.getConnection(hostname + port + databaseName), sql, new MapListHandler(), params);
    }

    @Override
    public List<String> getIgnoreDatabaseList() {
        return Arrays.asList("information_schema", "__internal_schema");
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
        objectMapper.registerModule(module);
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        fullFieldObjectMapper.registerModule(module);
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
     *
     * @param json      要加载的 JSON 数据
     * @param tableName 要将数据加载到其中的表的名称
     * @param columns   列的名称关系
     * @throws IOException 如果发生 IO 错误
     */
    private StreamLoadResult streamLoad(String json, String tableName, String columns) throws IOException {
        String url = "http://" + super.hostname + ":" + DORIS_HTTP_PORT + "/api/" + super.databaseName + "/" + tableName + "/_stream_load";

        String loadResult = "";

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(url);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(username, password));

            // 可以在 Header 中设置 stream load 相关属性，这里我们设置 label 和 column_separator。
            Calendar calendar = Calendar.getInstance();
            String label = String.format("audit_%s%02d%02d_%02d%02d%02d_%s",
                    calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                    calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                    UUID.randomUUID().toString().replaceAll("-", ""));
            //指定导入数据格式 csv, json, arrow, csv_with_names
            put.setHeader("format", "json");
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
            // 这里也可以使用 StringEntity 来传输任意数据。
            StringEntity entity = new StringEntity(json, StandardCharsets.UTF_8);
            put.setEntity(entity);

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
    public StreamLoadResult[] streamLoadBatch(List<?> list, String tableName, int size, String columns) throws IOException {
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
            StreamLoadResult streamLoadResult = streamLoad(listToJson(subList), tableName, columns);
            results[i] = streamLoadResult;
            start += size;
        }
        if (remainder > 0) {
            List<?> subList = list.subList(start, start + remainder);
            StreamLoadResult streamLoadResult = streamLoad(listToJson(subList), tableName, columns);
            results[batch] = streamLoadResult;
        }

        return results;
    }

    @Override
    public String getDialectSQL(SQLQueryBuilder queryBuilder) {
        long l = System.currentTimeMillis();
        String sql = queryBuilder.buildSQL();
        log.info("执行耗时：" + (System.currentTimeMillis() - l) + "ms");
        System.out.println();
        System.out.println(sql);
        return sql;
    }

    @Override
    public Object instantiate(String databaseName, String tableName) {
        try {
            DatabaseMetaData metaData = pool.getConnection(hostname + port + databaseName).getMetaData();
            ResultSet resultSet = metaData.getColumns(databaseName, null, tableName, null);

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
        try (Connection connection = pool.getConnection(hostname + port + databaseName);
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
                    "  \"max_file_size\" = \"2GB\"\n" +
                    ");");
            statement.execute(sql.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ShowExport queryASynExportJobStatus(String dbName, String label) {
        //查询结果封装成ShowExport类
        try (Connection connection = pool.getConnection(hostname + port + databaseName);
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
        try (Connection connection = pool.getConnection(hostname + port + databaseName);
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
        try (Connection connection = pool.getConnection(hostname + port + databaseName);
             Statement statement = connection.createStatement()) {
            String sql = "CANCEL EXPORT FROM tpch1 WHERE LABEL like \"%" + jobId + "%\";";
            statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntoOutFile synExport(String sql, String path, String maximumFileSize,FileTypeEnum fileTypeEnum) {
        //是否开头是以select开头的
        if (sql.toLowerCase().startsWith("select")) {
            //将前六个字符替换成?
            sql = "SELECT /*+ SET_VAR(query_timeout = 300, enable_partition_cache=false) */ " + sql.substring(6);
        }else {
            sql = sql.replace("select","SELECT /*+ SET_VAR(query_timeout = 300, enable_partition_cache=false) */ ");
        }
        try (Connection connection = pool.getConnection(hostname + port + databaseName);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql + " INTO OUTFILE \"file://" + path + "\"\n" +
                     "FORMAT AS "+fileTypeEnum+"\n" +
                     "PROPERTIES(\n" +
                     "  \"column_separator\" = \",\",\n" +
                     "  \"line_delimiter\" = \"\\n\",\n" +
                     "  \"max_file_size\" = \""+maximumFileSize+"\"" +
                     ");")) {
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

    public static void main(String[] args) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        JDBCConnectionEntity root = new JDBCConnectionEntity(DatabaseEnum.DORIS, "192.168.10.202", 9030, "bds_log", "root", "123456aA!@");

        DorisJDBCAdapter adapter = DatabaseAdapterFactory.getAdapter(root, DorisJDBCAdapter.class);
        Object instantiate = adapter.instantiate("bds_log", "bds_asset_info");
        System.out.println(1);
    }
}
