package com.cn.jmw.processor.datasource.jdbc.dialect.enums;

/**
 * SQLOperatorEnum 枚举表示 SQL 查询中可用的操作符。
 * <p>
 * 此枚举提供多种 SQL 操作符，如逻辑操作符、比较操作符及排序操作符，以便在构建 SQL 查询时使用。
 * </p>
 *
 * @author Jmwang
 */
public enum SQLOperatorEnum {
    /**
     * 减法操作符
     */
    SUBTRACTION("SUBTRACTION", "-"),

    /**
     * 加法操作符
     */
    ADDITION("ADDITION", "+"),

    MATCH_ANY("MATCH_ANY", "任意匹配"),

    MATCH_ALL("MATCH_ALL", "全部匹配"),

    /**
     * 逻辑或操作符。
     */
    OR("OR", "逻辑或"),

    /**
     * 逻辑与操作符。
     */
    AND("AND", "逻辑与"),

    /**
     * 等于操作符。
     */
    EQUAL("=", "等于"),

    /**
     * 不等于操作符。
     */
    NOT_EQUAL("!=", "不等于"),

    /**
     * NULL
     */
    IS_NULL("IS NULL", "为空"),

    /**
     * NOT NULL
     */
    IS_NOT_NULL("IS NOT NULL", "不为空"),

    /**
     * 大于操作符。
     */
    GT(">", "大于"),

    /**
     * 小于操作符。
     */
    LT("<", "小于"),

    /**
     * 大于或等于操作符。
     */
    GE(">=", "大于等于"),

    /**
     * 小于或等于操作符。
     */
    LE("<=", "小于等于"),

    /**
     * LIKE 操作符，用于模糊匹配。
     */
    LIKE("LIKE", "模式匹配"),

    /**
     * NOT LIKE 操作符，用于否定模糊匹配。
     */
    NOT_LIKE("NOT LIKE", "非模式匹配"),

    /**
     * START WITH 操作符，用于模糊匹配。
     */
    START_WITH("LIKE", "以...开头"),

    /**
     * END WITH 操作符，用于模糊匹配。
     */
    END_WITH("LIKE", "以...结尾"),

    /**
     * 用于模糊匹配的操作符，自动匹配包含指定值的记录。
     */
    CONTAINS("LIKE", "包含"),

    /**
     * 用于否定模糊匹配的操作符，自动匹配不包含指定值的记录。
     */
    NOT_CONTAINS("NOT LIKE", "不包含"),

    /**
     * IN 操作符，用于在一组值中匹配。
     */
    IN("IN", "在列表中"),

    /**
     * IN 操作符，用于在一组值中匹配。
     */
    NOT_IN("NOT_IN", "不在列表中"),

    /**
     * IN 操作符，用于在一组值中匹配。
     */
    IN_FILE("IN", "在清单中"),

    /**
     * IN 操作符，用于在一组值中匹配。
     */
    NOT_IN_FILE("NOT IN", "不在清单中"),

    /**
     * 升序排序操作符。
     */
    ASC("ASC", "升序"),

    /**
     * 降序排序操作符。
     */
    DESC("DESC", "降序"),
    ;

    private String operator;

    private String symbol;

    /**
     * SQLOperatorEnum 构造函数。
     *
     * @param operator 操作符的字符串表示
     */
    SQLOperatorEnum(String operator, String symbol) {
        this.operator = operator;
        this.symbol = symbol;
    }

    /**
     * 获取操作符的字符串表示。
     *
     * @return 操作符的字符串表示
     */
    public String getOperator() {
        return operator;
    }

    public String getSymbol() {
        return symbol;
    }

    public static SQLOperatorEnum getSymbol(String symbol) {
        for (SQLOperatorEnum operatorEnum : SQLOperatorEnum.values()) {
            if (operatorEnum.getSymbol().equals(symbol)) {
                return operatorEnum;
            }
        }
        return null;
    }
}