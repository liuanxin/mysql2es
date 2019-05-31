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

    public String useKey() {
        return String.format("table(%s) <=> index(%s)", table, useIndex());
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

        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            // ignore case, dot can match all(include wrap tab)
            sbd.append(sql.trim().replaceFirst("(?is)SELECT (.*?) FROM ", count));
        } else {
            sbd.append(count).append("`").append(table).append("`");
        }
        appendWhere(param, sbd);
        return sbd.toString();
    }

    private void appendWhere(String param, StringBuilder sbd) {
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

                boolean flag = sbd.toString().toUpperCase().contains(where);
                if (flag) {
                    sbd.append(and).append("( ");
                } else {
                    sbd.append(where);
                }
                for (int i = 0; i < incrementColumn.size(); i++) {
                    String tmp = params[i];
                    if (U.isNotBlank(tmp)) {
                        // number use gt(>), else use gte(>=). gte will query for duplicate data, but will not miss
                        sbd.append(incrementColumn.get(i));
                        if (U.isNumber(tmp)) {
                            sbd.append(" > ").append(tmp);
                        } else {
                            sbd.append(" >= ").append("'").append(tmp).append("'");
                        }
                        sbd.append(and);
                    }
                }
                if (sbd.toString().endsWith(and)) {
                    sbd.delete(sbd.length() - and.length(), sbd.length());
                }
                if (flag) {
                    sbd.append(" )");
                }
            }
        }
    }

    public String querySql(int page, String param) {
        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            sbd.append(sql.trim());
        } else {
            sbd.append("SELECT * FROM `").append(table).append("`");
        }
        // param split length = increment column size
        appendWhere(param, sbd);
        sbd.append(" ORDER BY");
        for (String column : incrementColumn) {
            sbd.append(" ").append(column).append(" ASC,");
        }
        if (sbd.toString().endsWith(",")) {
            sbd.delete(sbd.length() - 1, sbd.length());
        }
        sbd.append(" LIMIT ").append(page * limit).append(", ").append(limit);
        return sbd.toString();
    }
}
