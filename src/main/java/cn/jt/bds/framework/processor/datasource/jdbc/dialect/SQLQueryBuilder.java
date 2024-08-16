package cn.jt.bds.framework.processor.datasource.jdbc.dialect;

import cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLFunctionEnum;
import cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLJoinEnum;
import cn.jt.bds.framework.processor.datasource.jdbc.dialect.enums.SQLOperatorEnum;
import cn.jt.bds.framework.processor.datasource.jdbc.dialect.pojo.*;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.common.util.StringUtils;
import lombok.NoArgsConstructor;
import org.jooq.Record;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SQLQueryBuilder类用于构建SQL查询，包括选择、条件、连接和排序等功能。
 * 它使用构建者模式使得查询的构建变得灵活和可读。
 */
@NoArgsConstructor
public class SQLQueryBuilder {
    //SQL方言
    @JsonProperty("SQLDialect")
    private SQLDialect sqlDialect = SQLDialect.MYSQL;
    //数据库名称
    @JsonProperty("schemaName")
    private String schemaName;
    //表列表
    @JsonProperty("tables")
    private QueryTable table;
    //字段列表
    @JsonProperty("fields")
    private List<QueryField> fields = new ArrayList<>();
    //where字段条件
    @JsonProperty("conditions")
    private List<QueryCondition> conditions = new ArrayList<>();
    //扩展String where
    @JsonProperty("stringConditions")
    private List<String> stringConditions = new ArrayList<>();
    //级联
    @JsonProperty("joins")
    private List<QueryJoin> joins = new ArrayList<>();
    //分组
    @JsonProperty("groups")
    private List<String> groups = new ArrayList<>();
    //having字段条件
    @JsonProperty("havingConditions")
    private List<QueryCondition> havingConditions = new ArrayList<>();
    //扩展String having字段条件
    @JsonProperty("stringHavingConditions")
    private List<String> stringHavingConditions = new ArrayList<>();
    //排序
    @JsonProperty("orders")
    private List<QueryOrderBy> orders = new ArrayList<>();
    //限制条数
    @JsonProperty("limit")
    private Integer limit;
    //起始条数
    @JsonProperty("offset")
    private Integer offset;

    /**
     * 向SQL查询构建器添加字段，只提供字段名。
     *
     * @param field 字段名
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addField(String field) {
        this.fields.add(new QueryField(new String[]{field}, null, null, null));
        return this;
    }

    /**
     * 向SQL查询构建器添加字段，只提供字段名。
     *
     * @param tableName 表名
     * @param field     字段名
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addField(String tableName, String field) {
        this.fields.add(new QueryField(new String[]{field}, null, null, tableName));
        return this;
    }

    /**
     * 向SQL查询构建器添加字段，包括字段名和别名。
     *
     * @param tableName  表名，允许为空（默认为主表）
     * @param field      字段名
     * @param fieldAlias 字段别名
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addField(String tableName, String field, String fieldAlias) {
        this.fields.add(new QueryField(new String[]{field}, fieldAlias, null, tableName));
        return this;
    }

    /**
     * 向SQL查询构建器添加字段，包括字段名、别名和函数类型。
     *
     * @param field        入参一个
     * @param fieldAlias   字段别名
     * @param functionEnum 字段函数类型
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addField(String field, String fieldAlias, SQLFunctionEnum functionEnum) {
        this.fields.add(new QueryField(new String[]{field}, fieldAlias, functionEnum, null));
        return this;
    }

    /**
     * 向SQL查询构建器添加字段，包括字段名、别名和函数类型。
     *
     * @param field        入参数组（可以为0-n）
     * @param fieldAlias   字段别名
     * @param functionEnum 字段函数类型
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addField(String[] field, String fieldAlias, SQLFunctionEnum functionEnum) {
        this.fields.add(new QueryField(field, fieldAlias, functionEnum, null));
        return this;
    }

    /**
     * 向SQL查询构建器添加多个字段
     *
     * @param fields 字段集合
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addFields(Collection<String> fields) {
        for (String field : fields) {
            this.fields.add(new QueryField(new String[]{field}, null, null, null));
        }
        return this;
    }

    /**
     * 向SQL查询构建器添加多个字段
     *
     * @param queryField 查询字段集合
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addFields(List<QueryField> queryField) {
        for (QueryField field : queryField) {
            this.fields.add(field);
        }
        return this;
    }

    /**
     * 向SQL查询构建器添加一个条件，包括字段名、操作符、条件值和操作类型（AND/OR）
     *
     * @param field        字段名
     * @param operator     操作符
     * @param value        条件值
     * @param operatorEnum 连接符
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addCondition(String field, SQLOperatorEnum operator, Object value, SQLOperatorEnum operatorEnum) {
        this.conditions.add(buildConditions(field, operator, value, operatorEnum));
        return this;
    }

    /**
     * 向SQL查询构建器添加一个AND条件。
     *
     * @param field    字段名
     * @param operator 操作符
     * @param value    条件值
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addAndCondition(String field, SQLOperatorEnum operator, Object value) {
        this.conditions.add(buildConditions(field, operator, value, SQLOperatorEnum.AND));
        return this;
    }

    /**
     * 向SQL查询构建器添加一个OR条件。
     *
     * @param field    字段名
     * @param operator 操作符
     * @param value    条件值
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addOrCondition(String field, SQLOperatorEnum operator, Object value) {
        this.conditions.add(buildConditions(field, operator, value, SQLOperatorEnum.OR));
        return this;
    }

    /**
     * 向SQL查询构建器添加条件。
     *
     * @param condition QueryCondition实例
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addCondition(QueryCondition condition) {
        this.conditions.add(condition);
        return this;
    }

    /**
     * ============================================================
     * <p>向SQL查询构建器添加字符串条件，切记如果使用"字符串"添加会丧失一定的安全度。
     * </p>
     * ============================================================
     *
     * @param condition 条件字符串
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addStringCondition(String condition) {
        if (StringUtils.isNotBlank(condition)) {
            this.stringConditions.add(condition);
        }
        return this;
    }

    /**
     * 向SQL查询构建器添加多个AND条件。
     *
     * @param conditions QueryCondition集合
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addAndConditions(Collection<QueryCondition> conditions) {
        for (QueryCondition condition : conditions) {
            this.conditions.add(buildConditions(condition.getField(), condition.getOperator(), condition.getValue(), SQLOperatorEnum.AND));
        }
        return this;
    }

    /**
     * 向SQL查询构建器添加多个OR条件。
     *
     * @param conditions QueryCondition集合
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addOrConditions(Collection<QueryCondition> conditions) {
        for (QueryCondition condition : conditions) {
            this.conditions.add(buildConditions(condition.getField(), condition.getOperator(), condition.getValue(), SQLOperatorEnum.OR));
        }
        return this;
    }

    /**
     * 向SQLQueryBuilder添加一个或多个条件（嵌套关系）。
     * <p>
     * 此方法允许添加多个“QueryCondition”实例。
     * 第一个条件直接添加到构建器的条件列表中。
     * 后续条件嵌套在前一个条件中，创建层次结构
     * 条件。
     * </p>
     *
     * @param conditions 要添加的“QueryCondition”实例数组。
     *                   如果没有提供条件，该方法将简单地返回
     *                   当前实例，无需进行任何更改。
     * @return 用于方法链的“SQLQueryBuilder”的当前实例。
     */
    public SQLQueryBuilder addNestingConditions(QueryCondition... conditions) {
        if (conditions == null || conditions.length == 0) {
            return this; // 如果没有条件，直接返回
        }

        for (int i = 0; i < conditions.length; i++) {
            QueryCondition queryCondition = conditions[i];
            if (i == 0) {
                // 第一个条件直接添加
                this.conditions.add(queryCondition);
            } else {
                // 将当前条件嵌套到前一个条件中
                QueryCondition previousCondition = this.conditions.get(this.conditions.size() - 1);
                previousCondition.addNestedCondition(queryCondition); // Assuming addNestedCondition is implemented properly
            }
        }
        return this;
    }

    /**
     * 向SQLQueryBuilder添加连接。
     *
     * @param type        连接类型（例如：INNER, LEFT, RIGHT等）
     * @param subQuery    子查询构建器
     * @param alias       别名
     * @param onCondition 条件
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addJoin(SQLJoinEnum type, SQLQueryBuilder subQuery, String alias, String onCondition) {
        this.joins.add(buildJoin(type, subQuery, alias, onCondition));
        return this;
    }

    /**
     * 向SQLQueryBuilder添加连接。
     *
     * @param type        连接类型（例如：INNER, LEFT, RIGHT等）
     * @param table       目标表
     * @param alias       别名
     * @param onCondition 连接条件
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addJoin(SQLJoinEnum type, String table, String alias, String onCondition) {
        this.joins.add(buildJoin(type, table, alias, onCondition));
        return this;
    }

    /**
     * 向SQLQueryBuilder添加分组字段。
     *
     * @param group 分组字段
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addGroup(String group) {
        this.groups.add(group);
        return this;
    }

    /**
     * 向SQLQueryBuilder添加多个分组字段。
     *
     * @param groups 分组字段集合
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addGroups(Collection<String> groups) {
        for (String group : groups) {
            this.groups.add(group);
        }
        return this;
    }

    public SQLQueryBuilder addHavingConditions(String field, SQLOperatorEnum operator, Object value) {
        this.addHavingConditions(buildHavingConditions(field, operator, value));
        return this;
    }

    public SQLQueryBuilder addHavingCondition(String field, SQLOperatorEnum operator, Object value, SQLOperatorEnum operatorEnum) {
        this.addHavingConditions(buildConditions(field, operator, value, operatorEnum));
        return this;
    }

    /**
     * 向SQLQueryBuilder添加having条件。
     *
     * @param function       函数类型
     * @param field          字段名
     * @param operator       操作符
     * @param value          条件值
     * @param joinOperator   连接操作符
     * @param functionParams 函数参数
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addHavingConditions(SQLFunctionEnum function, String field, SQLOperatorEnum operator, Object value, SQLOperatorEnum joinOperator, List<Object> functionParams) {
        this.addHavingConditions(buildHavingConditions(function, field, operator, value, joinOperator, functionParams));
        return this;
    }

    /**
     * 向SQLQueryBuilder添加having条件。
     *
     * @param havingConditions QueryCondition实例
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    private SQLQueryBuilder addHavingConditions(QueryCondition havingConditions) {
        this.havingConditions.add(havingConditions);
        return this;
    }

    /**
     * 向SQLQueryBuilder添加排序条件。
     *
     * @param field 字段名
     * @param order 排序方式（升序或降序）
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder addOrder(String field, SQLOperatorEnum order) {
        this.addOrder(buildOrder(field, order));
        return this;
    }

    /**
     * 向SQLQueryBuilder添加排序条件。
     *
     * @param order QueryOrderBy实例
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    private SQLQueryBuilder addOrder(QueryOrderBy order) {
        this.orders.add(order);
        return this;
    }

    /**
     * 设置SQL方言。
     *
     * @param sqlDialect SQL方言枚举
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder setSqlDialect(SQLDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
        return this;
    }

    /**
     * 设置表名和别名。
     *
     * @param table      表名
     * @param tableAlias 表别名
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder tableName(String table, String tableAlias) {
        this.table = buildTable(table, tableAlias);
        return this;
    }

    /**
     * 设置表名和别名。
     *
     * @param sqlQueryBuilder SQL查询构建器
     * @param tableAlias      表别名
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder tableName(SQLQueryBuilder sqlQueryBuilder, String tableAlias) {
        this.table = buildTable(sqlQueryBuilder, tableAlias);
        return this;
    }

    /**
     * 设置表名。
     *
     * @param table 表名
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder tableName(String table) {
        this.table = buildTable(table);
        return this;
    }

    /**
     * 设置限制条数。
     *
     * @param limit 限制的最大数量
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder limit(Integer limit) {
        return setLimit(limit);
    }

    /**
     * 设置限制条数。
     *
     * @param limit 限制的最大数量
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    private SQLQueryBuilder setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    /**
     * 设置起始条数。
     *
     * @param offset 起始偏移量
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    public SQLQueryBuilder offset(Integer offset) {
        return setOffset(offset);
    }

    /**
     * 设置起始条数。
     *
     * @param offset 起始偏移量
     * @return 当前SQLQueryBuilder实例，以支持方法链调用
     */
    private SQLQueryBuilder setOffset(Integer offset) {
        this.offset = offset;
        return this;
    }

    /**
     * 获取完整的表名（包括模式名）。
     *
     * @return 完整的表名
     */
    private String getFullTableName() {
        if (table != null && table.getSqlQueryBuilder() != null) {
            return table.getSqlQueryBuilder().buildSQL();
        }
        return StringUtils.isNotBlank(schemaName) ? "`" + schemaName + "`.`" + table.getTable() + "`" : "`" + table.getTable() + "`";
    }

    /**
     * 获取表别名或名称。
     *
     * @return 表别名或名称
     */
    private String getTableAliasOrName() {
        if (table == null) {
            return null;
        }
        return StringUtils.isNotBlank(table.getTableAlias()) ? table.getTableAlias() : table.getTable();
    }

    /**
     * 获取合格字段。
     *
     * @param queryField 查询字段
     * @return jOOQ Field对象
     */
    private Field<?> getQualifiedField(QueryField queryField) {
        Field<?> field;

        if (queryField.getFunction() != null) {
            field = DSL.field(queryField.getFunction().getFunction() + "(" + queryField.getFieldString() + ")");
        } else {
            return queryField.toField(getTableAliasOrName());
        }

        // 检查并应用别名
        if (queryField.getFieldAlias() != null && !queryField.getFieldAlias().isEmpty()) {
            return field.as(queryField.getFieldAlias());
        }

        return field;

    }

    /**
     * 构建jOOQ查询对象。
     *
     * @param create DSLContext对象
     * @return SelectJoinStep<Record>对象
     */
    private SelectJoinStep<Record> buildQuery(DSLContext create,boolean isBuildSQL) {
        List<Field<?>> selectFields = new ArrayList<>();
        for (QueryField queryField : fields) {
            selectFields.add(getQualifiedField(queryField));
        }

        SelectJoinStep<Record> query;


        //有一种当不存在selectFields值的时候，并且不存在groups的时候，select *
        if (selectFields.size() == 0 && groups.size() == 0) {
            if (StringUtils.isBlank(table.getTableAlias()) || getTableAliasOrName().equals(table.getTable())) {
                query = create.select().from(DSL.table(getFullTableName()));
            } else {
                query = create.select().from(DSL.table(getFullTableName()).as(getTableAliasOrName()));
            }
        } else {
            if (table == null) {
                query = (SelectJoinStep<Record>) create.select(selectFields);
            } else if (StringUtils.isBlank(table.getTableAlias()) || getTableAliasOrName().equals(table.getTable())) {
                query = create.select(selectFields).from(DSL.table(getFullTableName()));
            } else {
                query = create.select(selectFields).from(DSL.table(getFullTableName()).as(getTableAliasOrName()));
            }
        }

        SelectJoinStep<Record> finalQuery = query;
        stringConditions.stream().map(condition -> finalQuery.where(DSL.condition(condition))).collect(Collectors.toList());
        if (conditions != null && !conditions.isEmpty()) {
            Condition whereCondition = null;
            for (QueryCondition condition : conditions) {
                if (whereCondition == null) {
                    whereCondition = condition.buildCondition(query, null,isBuildSQL);
                } else if (SQLOperatorEnum.OR == (condition.getJoinOperator())) {
                    whereCondition = whereCondition.or(condition.buildCondition(query, null,isBuildSQL));
                } else {
                    whereCondition = whereCondition.and(condition.buildCondition(query, null,isBuildSQL));
                }
            }
            query.where(whereCondition);
        }

        for (QueryJoin join : joins) {
            switch (join.getType()) {
                case JOIN:
                    query = query.join(join.toTable(create)).on(DSL.condition(join.getOnCondition()));
                    break;
                case INNER_JOIN:
                    query = query.innerJoin(join.toTable(create)).on(DSL.condition(join.getOnCondition()));
                    break;
                case LEFT_JOIN:
                    query = query.leftJoin(join.toTable(create)).on(DSL.condition(join.getOnCondition()));
                    break;
                case RIGHT_JOIN:
                    query = query.rightJoin(join.toTable(create)).on(DSL.condition(join.getOnCondition()));
                    break;
                case FULL_OUTER_JOIN:
                    query = query.fullJoin(join.toTable(create)).on(DSL.condition(join.getOnCondition()));
                    break;
                default:
                    throw new UnsupportedOperationException("不支持的联接类型: " + join.getType());
            }
        }

        for (String group : groups) {
            query.groupBy(DSL.field(DSL.name(getTableAliasOrName(), group).quotedName()));
        }

        stringHavingConditions.stream().map(condition -> finalQuery.having(DSL.condition(condition))).collect(Collectors.toList());
        if (havingConditions != null && !havingConditions.isEmpty()) {
            Condition havingCondition = null;
            for (QueryCondition condition : havingConditions) {
                if (havingCondition == null) {
                    havingCondition = condition.buildCondition(query, null,isBuildSQL);
                } else if (SQLOperatorEnum.OR == (condition.getJoinOperator())) {
                    havingCondition = havingCondition.or(condition.buildCondition(query, null,isBuildSQL));
                } else {
                    havingCondition = havingCondition.and(condition.buildCondition(query, null,isBuildSQL));
                }
            }
            query.having(havingCondition);
        }

        if (orders != null && !orders.isEmpty()) {
            List<SortField<?>> sortFields = new ArrayList<>();
            for (QueryOrderBy queryOrderBy : orders) {
                SQLOperatorEnum order = queryOrderBy.getOrder();
                String field = queryOrderBy.getField();
                switch (order) {
                    case ASC:
                        sortFields.add(DSL.field(DSL.name(getTableAliasOrName(), field).quotedName()).asc());
                        break;
                    case DESC:
                        sortFields.add(DSL.field(DSL.name(getTableAliasOrName(), field).quotedName()).desc());
                        break;
                    default:
                        throw new IllegalStateException("不支持的排序类型: " + order);
                }
            }
            query.orderBy(sortFields);
        }

        if (limit != null) {
            query.limit(limit);
        }
        if (offset != null) {
            query.offset(offset);
        }

        return query;
    }

    /**
     * 构建SQL字符串。
     *
     * @return 生成的SQL字符串
     */
    public String buildSQL() {
        QuerySQLResult querySQLResult = buildPlaintextQuerySQL();
        return getRealSql(querySQLResult.getSql(), querySQLResult.getData());
    }

    /**
     * 构建占位符的SQL对象。
     *
     * @return 生成的占位符的SQL对象
     */
    public QuerySQLResult buildQuerySQLResult() {
        return buildPlaceHolderQuerySQLResult(true);
    }

    /**
     * 构建子查询的SQL字符串。
     *
     * @param create DSLContext对象
     * @return 生成的子查询SQL字符串
     */
    public String buildSubQuerySQL(DSLContext create) {
        QuerySQLResult querySQLResult = buildPlaintextQuerySQL(create);
        return getRealSql(querySQLResult.getSql(), querySQLResult.getData());
    }

    /**
     * 构建子查询占位符的SQL字符串。
     *
     * @return 生成的子查询SQL字符串
     */
    public String buildSubQuerySQL() {
        QuerySQLResult querySQLResult = buildPlaceHolderQuerySQLResult(true);
        return getRealSql(querySQLResult.getSql(), querySQLResult.getData());
    }

    /**
     * 占位符构建查询SQL字符串。并且留下对应的占位Object数组。
     * <p>
     * 返回到QuerySQLResult对象中
     * String sql;
     * Object[][] data;
     */
    private QuerySQLResult buildPlaintextQuerySQL(DSLContext create) {
        SelectJoinStep<Record> query = buildQuery(create,true);
        //query.getParams()的value参数写入Object数字
        Object[] array = query.getParams().entrySet().stream().map(
                param -> {
                    Object value = param.getValue().getValue();
                    if (value instanceof String
                            && StringUtils.isNotBlank((String) value)
                            && ((String) value).contains("'")
                            && !((String) value).contains("\"")) {
                        return "\"" + value + "\"";
                    } else if (value instanceof Integer
                            || value instanceof Long
                            || value instanceof Double
                            || value instanceof Float
                            || value instanceof Short) {
                        return value;
                    } else {
                        return "'" + param.getValue().getValue() + "'";
                    }
                }
        ).toArray();
        return new QuerySQLResult(query.getSQL(ParamType.INDEXED), array);
    }

    /**
     * 占位符构建查询SQL字符串。并且留下对应的占位Object数组。
     * <p>
     * 返回到QuerySQLResult对象中
     * String sql;
     * Object[][] data; buildPlaceHolderQuerySQLResult
     */
    private QuerySQLResult buildPlaintextQuerySQL() {
        DSLContext create = DSL.using(sqlDialect);
        SelectJoinStep<Record> query = buildQuery(create,false);
        //query.getParams()的value参数写入Object数字
        Object[] array = query.getParams().entrySet().stream().map(
                param -> {
                    Object value = param.getValue().getValue();
                    if (value instanceof String
                            && StringUtils.isNotBlank((String) value)
                            && ((String) value).contains("'")
                            && !((String) value).contains("\"")) {
                        return "\"" + value + "\"";
                    } else if (value instanceof Integer
                            || value instanceof Long
                            || value instanceof Double
                            || value instanceof Float
                            || value instanceof Short) {
                        return value;
                    } else {
                        return "\"" + ((String) value).replaceAll("\"", "") + "\"";
                    }
                }
        ).toArray();
        return new QuerySQLResult(query.getSQL(ParamType.INDEXED), array);
    }

    private String getRealSql(String sql, Object[] data) {
        StringBuilder realSql = new StringBuilder("(");
        int dataIdx = 0;

        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '?' && dataIdx < data.length) {
                Object value = data[dataIdx++];
                if (value instanceof String) {
                    realSql.append(value);
                } else if (value == null) {
                    realSql.append("NULL");
                } else {
                    realSql.append(value);
                }
            } else {
                realSql.append(ch);
            }
        }

        realSql.append(")");

        return realSql.toString();
    }

    private QuerySQLResult buildPlaceHolderQuerySQLResult(boolean isBuildSQL) {
        DSLContext create = DSL.using(sqlDialect);
        SelectJoinStep<Record> query = buildQuery(create,isBuildSQL);
        return new QuerySQLResult(query.getSQL(ParamType.INDEXED), query.getParams().values().toArray());
    }

    private QueryTable buildTable(String table, String tableAlias) {
        return new QueryTable(table, tableAlias);
    }

    private QueryTable buildTable(String table) {
        return new QueryTable(table);
    }

    private QueryTable buildTable(SQLQueryBuilder sqlQueryBuilder, String tableAlias) {
        return new QueryTable(sqlQueryBuilder, tableAlias);
    }


    /**
     * 构建连接对象。
     *
     * @param type        连接类型（例如：INNER, LEFT, RIGHT等）
     * @param table       目标表
     * @param alias       别名
     * @param onCondition 连接条件
     * @return QueryJoin对象
     */
    private QueryJoin buildJoin(SQLJoinEnum type, String table, String alias, String onCondition) {
        return new QueryJoin(type, table, alias, onCondition);
    }

    /**
     * 构建连接对象。
     *
     * @param type        连接类型（例如：INNER, LEFT, RIGHT等）
     * @param subQuery    子查询构建器
     * @param alias       别名
     * @param onCondition 连接条件
     * @return QueryJoin对象
     */
    private QueryJoin buildJoin(SQLJoinEnum type, SQLQueryBuilder subQuery, String alias, String onCondition) {
        return new QueryJoin(type, subQuery, alias, onCondition);
    }

    /**
     * 构建排序对象。
     *
     * @param field 字段名
     * @param order 排序方式
     * @return QueryOrderBy对象
     */
    private QueryOrderBy buildOrder(String field, SQLOperatorEnum order) {
        return new QueryOrderBy(field, order);
    }

    /**
     * 构建having条件对象。
     *
     * @param function       函数类型
     * @param field          字段名
     * @param operator       操作符
     * @param value          条件值
     * @param joinOperator   连接操作符
     * @param functionParams 函数参数
     * @return QueryCondition对象
     */
    private QueryCondition buildHavingConditions(SQLFunctionEnum function, String field, SQLOperatorEnum operator, Object value, SQLOperatorEnum joinOperator, List<Object> functionParams) {
        return new QueryCondition(function, field, operator, value, joinOperator, functionParams);
    }

    /**
     * 构建having条件对象。
     *
     * @param field    字段名
     * @param operator 操作符
     * @param value    条件值
     * @return QueryCondition对象
     */
    private QueryCondition buildHavingConditions(String field, SQLOperatorEnum operator, Object value) {
        return new QueryCondition(field, operator, value);
    }

    /**
     * 构建条件对象。
     *
     * @param field        字段名
     * @param operator     操作符
     * @param value        条件值
     * @param operatorEnum 操作类型（AND/OR）
     * @return QueryCondition对象
     */
    private QueryCondition buildConditions(String field, SQLOperatorEnum operator, Object value, SQLOperatorEnum operatorEnum) {
        return new QueryCondition(field, operator, value, operatorEnum);
    }

//    private QueryCondition buildConditions(SQLFunctionEnum function, String field, SQLOperatorEnum operator, Object value, SQLOperatorEnum joinOperator,List<Object> functionParams) {
//        return new QueryCondition(function,field, operator, value, joinOperator,functionParams);
//    }

    public SQLQueryBuilder addHavingConditions(List<QueryCondition> metricConditions) {
        this.havingConditions.addAll(metricConditions);
        return this;
    }

    public SQLQueryBuilder addStringHavingCondition(String havingCondition) {
        if (StringUtils.isNotBlank(havingCondition)) {
            this.stringHavingConditions.add(havingCondition);
        }
        return this;
    }


}
