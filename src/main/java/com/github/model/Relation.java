package com.github.model;

import com.github.util.A;
import com.github.util.Logs;
import com.github.util.U;
import lombok.Getter;
import lombok.Setter;

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

    /** The field name for the increment in the table, if nil use incrementColumn */
    private List<String> incrementColumnAlias;

    /** es index <==> database table name. it not, will generate by table name(t_some_one ==> someOne) */
    private String index;

    /** whether to generate scheme of es on the database table structure */
    private boolean scheme = true;

    /** operate sql statement. if not, will generate by table name(select * from table_name) */
    private String sql;

    /**  number of each operation. will append in sql(select ... limit 500) */
    private Integer limit = 500;

    /** table column -> es field. if not, will generate by column(c_some_type ==> someType) */
    private Map<String, String> mapping;

    /**
     * primary key, will generate to id in es, query from db table, if not, can't create index in es
     *
     * @see org.elasticsearch.client.support.AbstractClient#prepareIndex(String, String, String)
     */
    private List<String> keyColumn;

    private String idPrefix;
    private String idSuffix;

    void check() {
        U.assertNil(table, "must set (db table name)");
        if (A.isEmpty(incrementColumn)) {
            U.assertException("must set (db table increment-column)");
        }
        if (incrementColumn.size() > 0) {
            if (A.isNotEmpty(incrementColumnAlias)) {
                if (incrementColumn.size() != incrementColumnAlias.size()) {
                    U.assertException("increment-column length must equals increment-column-alias length");
                }
            } else {
                incrementColumnAlias = incrementColumn;
            }
        }
        if (U.isNotBlank(limit)) {
            U.assert0(limit, "limit must greater 0");
        }
        if (U.isNotBlank(sql)) {
            sql = U.replaceBlank(sql);
        }
    }

    /** if not set the 「type」, generate from 「table name」 */
    public String useIndex() {
        if (U.isNotBlank(index)) {
            return index;
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
        String count = "SELECT COUNT(*) FROM ";

        StringBuilder querySql = new StringBuilder();
        if (U.isNotBlank(sql)) {
            querySql.append(sql.trim().replaceFirst("(?i)SELECT (.*?) FROM ", count));
        } else {
            querySql.append(count).append("`").append(table).append("`");
        }
        appendWhere(param, querySql);
        return querySql.toString();
    }

    private String sqlData(Object obj) {
        return U.isNumber(obj.toString()) ? obj.toString() : ("'" + obj + "'");
    }

    private void appendWhere(String param, StringBuilder querySql) {
        // param split length = increment column size
        if (U.isNotBlank(param)) {
            String[] params = param.split(U.SPLIT);
            if (incrementColumn.size() != params.length) {
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error("increment ({}) != param ({})", A.toStr(incrementColumn), param);
                }
            } else {
                String and = " AND ";
                String where = " WHERE ";

                boolean flag = querySql.toString().toUpperCase().contains(where);
                if (flag) {
                    querySql.append(and).append("( ");
                } else {
                    querySql.append(where);
                }
                for (int i = 0; i < incrementColumn.size(); i++) {
                    String tmp = params[i];
                    if (U.isNotBlank(tmp)) {
                        // gte(>=) will query for duplicate data, but will not miss, exclude by the following conditions
                        querySql.append(incrementColumn.get(i)).append(" >= ").append(sqlData(tmp)).append(and);
                    }
                }
                if (querySql.toString().endsWith(and)) {
                    querySql.delete(querySql.length() - and.length(), querySql.length());
                }
                if (flag) {
                    querySql.append(" )");
                }
            }
        }
    }

    public String querySql(int page, String param) {
        StringBuilder querySql = new StringBuilder();
        if (U.isNotBlank(sql)) {
            querySql.append(sql.trim());
        } else {
            querySql.append("SELECT * FROM `").append(table).append("`");
        }
        // param split length = increment column size
        appendWhere(param, querySql);
        querySql.append(" ORDER BY");
        for (int i = 0; i < incrementColumn.size(); i++) {
            String column = incrementColumn.get(i);
            querySql.append(String.format(" %s ASC", column));
            if (i + 1 != incrementColumn.size()) {
                querySql.append(",");
            }
        }
        querySql.append(" LIMIT ").append(page * limit).append(", ").append(limit);
        return querySql.toString();
    }
}
