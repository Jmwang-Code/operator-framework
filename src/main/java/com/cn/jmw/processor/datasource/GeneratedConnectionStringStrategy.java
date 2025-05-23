package com.cn.jmw.processor.datasource;

import com.cn.jmw.common.exception.ServiceException;
import com.cn.jmw.processor.datasource.enums.DatabaseEnum;
import com.cn.jmw.processor.datasource.pojo.JdbcAdapterDataSourceConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static com.cn.jmw.common.exception.enums.StructuredErrorCodeConstants.*;

/**
 * 使用配置文件生成连接字符串的策略
 */
public class GeneratedConnectionStringStrategy implements ConnectionStringStrategy {
    private static final Logger logger = LoggerFactory.getLogger(GeneratedConnectionStringStrategy.class);
    private final DatabaseEnum dbType;
    private static final Map<DatabaseEnum, String> URL_TEMPLATES = loadUrlTemplates();

    public GeneratedConnectionStringStrategy(DatabaseEnum dbType) {
        this.dbType = dbType;
    }

    private static Map<DatabaseEnum, String> loadUrlTemplates() {
        try (InputStream is = GeneratedConnectionStringStrategy.class.getClassLoader().getResourceAsStream("db-connection-strings.json")) {
            if (is == null) {
                logger.error("找不到 db-connection-strings.json");
                throw new RuntimeException("数据库连接字符串配置文件缺失");
            }
            ObjectMapper mapper = new ObjectMapper();
            // 解析为正确的嵌套结构
            Map<String, Map<String, Map<String, String>>> config = mapper.readValue(is, Map.class);
            Map<String, Map<String, String>> databases = config.get("databases");
            if (databases == null) {
                logger.error("JSON 配置缺少 'databases' 字段");
                throw new RuntimeException("无效的数据库连接字符串配置：缺少 'databases'");
            }
            Map<DatabaseEnum, String> templates = new HashMap<>();
            for (Map.Entry<String, Map<String, String>> entry : databases.entrySet()) {
                String dbTypeName = entry.getKey();
                Map<String, String> dbConfig = entry.getValue();
                String urlTemplate = dbConfig != null ? dbConfig.get("urlTemplate") : null;
                if (urlTemplate == null || urlTemplate.isBlank()) {
                    logger.warn("数据库类型 {} 缺少有效的 urlTemplate，跳过", dbTypeName);
                    continue;
                }
                try {
                    DatabaseEnum dbType = DatabaseEnum.valueOf(dbTypeName);
                    templates.put(dbType, urlTemplate);
                } catch (IllegalArgumentException e) {
                    logger.warn("未知的数据库类型: {}，跳过", dbTypeName);
                }
            }
            if (templates.isEmpty()) {
                logger.error("未加载到任何有效的连接字符串模板");
                throw new RuntimeException("数据库连接字符串配置为空");
            }
            return templates;
        } catch (IOException e) {
            logger.error("加载连接字符串配置失败: {}", e.getMessage(), e);
            throw new RuntimeException("无法加载数据库连接字符串配置", e);
        }
    }

    @Override
    public String getConnectionString(String hostname, Integer port, String databaseName, JdbcAdapterDataSourceConfig config) {
        if (hostname == null || hostname.isBlank() || port == null) {
            throw new ServiceException(DATABASE_INPUT_TYPE_ERROR_1);
        }
        String template = URL_TEMPLATES.get(dbType);
        if (template == null) {
            throw new ServiceException(DATABASE_INPUT_TYPE_ERROR_3);
        }
        return String.format(template, hostname, port, databaseName);
    }
}