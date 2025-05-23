package cn.jt.bds.framework.pojo;

import java.util.Objects;

/**
 * InnerSql类用于表示SQL列的元数据信息。
 * <p>
 * 该类主要用于描述数据库表结构中的列信息，包含列名、列类型和列注释等属性。
 * 适用于需要操作数据库表元数据的场景，例如生成SQL语句或映射表结构。
 * </p>
 *
 * @author Jmwang
 */
public class InnerSql {

    /**
     * 列名，表示数据库表中的列名称，不能为空。
     */
    private String columnName;

    /**
     * 列类型，表示数据库表中列的数据类型，例如 "VARCHAR" 或 "INT"，可能为 null。
     */
    private String columnType;

    /**
     * 列注释，表示数据库表中列的说明或描述，可能为 null。
     */
    private String columnComment;

    /**
     * 默认构造函数，用于创建一个空的InnerSql实例。
     * <p>
     * 该构造函数适用于需要后续通过 setter 方法设置字段值的场景。
     * </p>
     */
    public InnerSql() {}

    /**
     * 带参数的构造函数，用于初始化列名、列类型和列注释。
     * <p>
     * 该构造函数适用于在创建对象时已知所有字段值的情况。
     * </p>
     *
     * @param columnName 列名，不能为空。
     * @param columnType 列类型，可能为 null。
     * @param columnComment 列注释，可能为 null。
     */
    public InnerSql(String columnName, String columnType, String columnComment) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnComment = columnComment;
    }

    /**
     * 获取列名。
     *
     * @return 列名，返回当前对象的 columnName 字段值。
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 设置列名。
     *
     * @param columnName 列名，用于更新当前对象的 columnName 字段值。
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * 获取列类型。
     *
     * @return 列类型，返回当前对象的 columnType 字段值。
     */
    public String getColumnType() {
        return columnType;
    }

    /**
     * 设置列类型。
     *
     * @param columnType 列类型，用于更新当前对象的 columnType 字段值。
     */
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    /**
     * 获取列注释。
     *
     * @return 列注释，返回当前对象的 columnComment 字段值。
     */
    public String getColumnComment() {
        return columnComment;
    }

    /**
     * 设置列注释。
     *
     * @param columnComment 列注释，用于更新当前对象的 columnComment 字段值。
     */
    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

    @Override
    public String toString() {
        return "InnerSql{" +
                "columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", columnComment='" + columnComment + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InnerSql innerSql = (InnerSql) o;
        return Objects.equals(columnName, innerSql.columnName) &&
                Objects.equals(columnType, innerSql.columnType) &&
                Objects.equals(columnComment, innerSql.columnComment);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(columnName);
        result = 31 * result + Objects.hashCode(columnType);
        result = 31 * result + Objects.hashCode(columnComment);
        return result;
    }
}