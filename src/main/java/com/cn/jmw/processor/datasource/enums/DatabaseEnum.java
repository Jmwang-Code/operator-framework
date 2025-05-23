package com.cn.jmw.processor.datasource.enums;

import com.cn.jmw.processor.datasource.jdbc.adapter.*;
import com.cn.jmw.processor.datasource.nosql.adapter.*;
import lombok.Getter;

/**
 * 数据源映射表枚举
 * <p>
 * 该枚举定义了支持的各种数据库类型及其对应的适配器类，用于在应用程序中适配不同的数据库连接。
 * 每个枚举值包含数据库名称、类型ID、分类（SQL 或 NOSQL）以及适配器类。
 * </p>
 *
 * @author Jmwang
 */
public enum DatabaseEnum {
    /** 阿里云 RDS 数据库 */
    ALIYUN_RDS("ALIYUN_RDS", 1, "SQL", AliyunRdsJdbcAdapter.class),
    /** 阿里云 ODPS 数据库 */
    ALIYUN_ODPS("ALIYUN_ODPS", 2, "NOSQL", AliyunOdpsJdbcAdapter.class),
    /** ClickHouse 数据库 */
    CLICKHOUSE("CLICKHOUSE", 3, "SQL", ClickHouseJdbcAdapter.class),
    /** DB2 数据库 */
    DB2("DB2", 4, "SQL", Db2JdbcAdapter.class),
    /** Derby 数据库 */
    DERBY("DERBY", 5, "SQL", DerbyJdbcAdapter.class),
    /** DM 数据库 */
    DM("DM", 6, "SQL", DmJdbcAdapter.class),
    /** Doris 数据库 */
    DORIS("DORIS", 7, "SQL", DorisJdbcAdapter.class),
    /** GaussDB 数据库 */
    GAUSSDB("GAUSSDB", 8, "SQL", GaussDbJdbcAdapter.class),
    /** GBase8A 数据库 */
    GBASE8A("GBASE8A", 9, "SQL", Gbase8JdbcAdapter.class),
    /** GBase 数据库 */
    GBASE("GBASE", 10, "SQL", GbaseJdbcAdapter.class),
    /** GoldenDB 数据库 */
    GOLDENDB("GOLDENDB", 11, "SQL", GoldenDbJdbcAdapter.class),
    /** Greenplum 数据库 */
    GREENPLUM("GREENPLUM", 12, "SQL", GreenplumJdbcAdapter.class),
    /** Hive 数据库 */
    HIVE("HIVE", 13, "NOSQL", HiveJdbcAdapter.class),
    /** KingBase8 数据库 */
    KINGBASE8("KINGBASE8", 14, "SQL", KingBase8JdbcAdapter.class),
    /** KunDB 数据库 */
    KUNDB("KUNDB", 16, "SQL", KunDbJdbcAdapter.class),
    /** MongoDB 数据库 */
    MONGODB("MONGODB", 17, "NOSQL", MongoDbJdbcAdapter.class),
    /** MySQL 数据库 */
    MYSQL("MYSQL", 18, "SQL", MySqlJdbcAdapter.class),
    /** OceanBase 数据库 */
    OCEANBASE("OCEANBASE", 19, "SQL", OceanBaseJdbcAdapter.class),
    /** OpenGauss 数据库 */
    OPENGAUSS("OPENGAUSS", 20, "SQL", OpenGaussJdbcAdapter.class),
    /** Oracle 数据库 */
    ORACLE("ORACLE", 21, "SQL", OracleJdbcAdapter.class),
    /** OSCAR 数据库 */
    OSCAR("OSCAR", 22, "SQL", OscarJdbcAdapter.class),
    /** PostgreSQL 数据库 */
    POSTGRESQL("POSTGRESQL", 23, "SQL", PostgreSqlJdbcAdapter.class),
    /** SelectDB 数据库 */
    SELECTDB("SELECTDB", 24, "NOSQL", SelectDbJdbcAdapter.class),
    /** SQLite3 数据库 */
    SQLITE3("SQLITE3", 25, "SQL", SQLite3JDBCAdapter.class),
    /** SQL Server 数据库 (MSSQL) */
    SQLSERVER("SQLSERVER", 26, "SQL", SqlServerJdbcAdapter.class),
    /** StarRocks 数据库 */
    STARROCKS("STARROCKS", 27, "SQL", StarRocksJdbcAdapter.class),
    /** Sybase 数据库 */
    SYBASE("SYBASE", 28, "SQL", SybaseJdbcAdapter.class),
    /** TDSQL 数据库 */
    TDSQL("TDSQL", 29, "SQL", TdSqlJdbcAdapter.class),
    /** TiDB 数据库 */
    TIDB("TIDB", 30, "SQL", TIDBJDBCAdapter.class),
    /** Polar 数据库 */
    POLAR("POLAR", 31, "SQL", PolarJdbcAdapter.class);

    /** 获取数据库名称。*/
    private final String name;
    private final int type;
    /** 获取数据库分类。*/
    private final String databaseCategory;
    /** 获取适配器类。*/
    private final Class<?> adapterClass;

    /**
     * 构造函数，用于初始化数据库枚举类型。
     *
     * @param name            数据库名称
     * @param type            数据库类型ID
     * @param databaseCategory 数据库分类（SQL 或 NOSQL）
     * @param adapterClass    适配器类
     */
    DatabaseEnum(String name, int type, String databaseCategory, Class<?> adapterClass) {
        this.name = name;
        this.type = type;
        this.databaseCategory = databaseCategory;
        this.adapterClass = adapterClass;
    }

    /**
     * 获取数据库名称。
     *
     * @return 数据库名称
     */
    public String getName() {
        return name;
    }

    /**
     * 获取数据库类型ID。
     *
     * @return 类型ID
     */
    public int getTypeId() {
        return type;
    }

    /**
     * 获取数据库分类。
     *
     * @return 数据库分类（SQL 或 NOSQL）
     */
    public String getDatabaseCategory() {
        return databaseCategory;
    }

    /**
     * 获取适配器类。
     *
     * @return 适配器类
     */
    public Class<?> getAdapterClass() {
        return adapterClass;
    }

    /**
     * 根据类型ID获取对应的数据库类型枚举。
     *
     * @param typeId 整型的类型ID
     * @return 对应的 DatabaseEnum 类型，如果未找到或 typeId 为 null 则返回 null
     */
    public static DatabaseEnum getDatabaseType(Integer typeId) {
        // 修复：处理typeId为null的情况
        if (typeId == null) {
            return null;
        }
        for (DatabaseEnum databaseEnum : DatabaseEnum.values()) {
            if (databaseEnum.getTypeId() == typeId) {
                return databaseEnum;
            }
        }
        return null;
    }
}