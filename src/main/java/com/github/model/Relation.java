package com.github.model;

import com.github.util.A;
import com.github.util.Logs;
import com.github.util.U;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class Relation {

    /** database table name */
    private String table;

    /** The field name for the increment in the table */
    private List<String> incrementColumn;

    // above two properties must be set, the following don't need.

    /** es type <==> database table name. it not, will generate by table name(t_some_one ==> someOne) */
    private String type;

    /** whether to generate scheme of es on the database table structure */
    private boolean scheme = true;

    /** operate sql statement. if not, will generate by table name(select * from table_name) */
    private String sql;

    /**  number of each operation. will append in sql(select ... limit 50) */
    private Integer limit = 50;

    /** table column -> es field. if not, will generate by column(c_some_type ==> someType) */
    private Map<String, String> mapping;

    /**
     * primary key, will generate to id in es, query from db table, if not, can't create index in es
     *
     * @see org.elasticsearch.client.support.AbstractClient#prepareIndex(String, String, String)
     */
    List<String> keyList;

    void check() {
        U.assertNil(table, "must set (db table name)");
        U.assertException(A.isEmpty(incrementColumn), "must set (db table increment-column)");
        if (U.isNotBlank(limit)) {
            U.assert0(limit, "limit must greater 0");
        }
        if (U.isNotBlank(sql)) {
            sql = U.replaceBlank(sql);
        }
    }

    /** if not set the 「type」, generate from 「table name」 */
    public String useType() {
        if (U.isNotBlank(type)) {
            return type;
        }
        if (U.isNotBlank(table)) {
            return U.tableToType(table);
        }
        return U.EMPTY;
    }

    /** if not config the 「mapping」, generate from 「column name」 */
    public String useField(String column) {
        if (U.isBlank(column)) {
            return U.EMPTY;
        }

        if (A.isNotEmpty(mapping)) {
            String field = mapping.get(column);
            if (U.isNotBlank(field)) {
                return field;
            }
        }
        return U.columnToField(column);
    }

    /** generate desc sql */
    public String descSql() {
        return String.format("DESC `%s`", table);
    }

    public int loopCount(int count) {
        int loop = count / limit;
        if (count % limit != 0) {
            loop += 1;
        }
        return loop;
    }

    public String countSql(String param) {
        StringBuilder querySql = new StringBuilder();
        querySql.append(U.isNotBlank(sql)
                ? sql.trim().replaceFirst("(?i)SELECT (.*?) FROM ", "SELECT COUNT(*) FROM ")
                : String.format("SELECT count(*) FROM `%s`", table));
        appendWhere(param, querySql);

        return querySql.toString();
    }

    private void appendWhere(String param, StringBuilder querySql) {
        // param split length = increment column size
        if (U.isNotBlank(param)) {
            String params[] = param.split(U.SPLIT);
            if (incrementColumn.size() != params.length) {
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error("increment ({}) != param ({})", A.toStr(incrementColumn), param);
                }
            } else {
                querySql.append(querySql.toString().toUpperCase().contains(" WHERE ") ? " AND" : " WHERE");

                for (int i = 0; i < incrementColumn.size(); i++) {
                    String p = params[i];
                    if (U.isNotBlank(p)) {
                        String column = incrementColumn.get(i);
                        if (NumberUtils.isNumber(p)) {
                            querySql.append(String.format(" `%s` > %d", column, NumberUtils.toLong(p)));
                        } else {
                            querySql.append(String.format(" `%s` > '%s'", column, p));
                        }
                        if ((i + 1) != incrementColumn.size()) {
                            querySql.append(" AND");
                        }
                    }
                }
            }
        }
    }

    public String querySql(int page, String param) {
        StringBuilder querySql = new StringBuilder();
        querySql.append(U.isNotBlank(sql) ? sql : String.format("SELECT * FROM `%s`", table));

        // param split length = increment column size
        appendWhere(param, querySql);
        querySql.append(" ORDER BY");
        for (int i = 0; i < incrementColumn.size(); i++) {
            String column = incrementColumn.get(i);
            querySql.append(String.format(" `%s` ASC", column));
            if (i + 1 != incrementColumn.size()) {
                querySql.append(" ,");
            }
        }
        querySql.append(" LIMIT ").append(page * limit).append(", ").append(limit);
        return querySql.toString();
    }
}
