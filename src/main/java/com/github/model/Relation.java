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
    /**
     * database table name
     */
    String table;
    /**
     * The field name for the increment in the table
     */
    List<String> incrementColumn;

    // above two properties must be set, the following don't need.

    /**
     * es type <==> database table name. it not, will generate by table name(t_some_one ==> someOne)
     */
    String type;
    /**
     * whether to generate scheme of es on the database table structure
     */
    boolean scheme = true;
    /**
     * operate sql statement. if not, will generate by table name(select * from table_name)
     */
    String sql;
    /**
     * number of each operation. will append in sql(select ... limit 50)
     */
    Integer limit = 50;
    /**
     * table column -> es field. if not, will generate by column(c_some_type ==> someType)
     */
    Map<String, String> mapping;

    /**
     * primary key, will generate to id in es, query from db table, if not, can't create index in es
     *
     * @see org.elasticsearch.client.support.AbstractClient#prepareIndex(String, String, String)
     */
    List<String> keyList;

    void check() {
        U.assertNil(table, "must set (db table name)");
        U.assertException(A.isEmpty(incrementColumn), "must set (db table increment-column)");
    }

    /**
     * if not set the 「type」, generate from 「table name」
     */
    public String useType() {
        if (U.isNotBlank(type)) {
            return type;
        }
        if (U.isNotBlank(table)) {
            return U.tableToType(table);
        }
        return U.EMPTY;
    }

    /**
     * if not config the 「mapping」, generate from 「column name」
     */
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

    /**
     * generate desc sql
     */
    public String descSql() {
        return String.format("desc `%s`", table);
    }

    /**
     * generate query sql
     */
    public String querySql(String param) {
        StringBuilder querySql = new StringBuilder();
        querySql.append(U.isNotBlank(sql) ? sql : String.format("SELECT * FROM `%s`", table));

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
                        if (i + 1 != incrementColumn.size()) {
                            querySql.append(" AND");
                        }
                    }
                }
            }
        }
        querySql.append(" ORDER BY");
        for (int i = 0; i < incrementColumn.size(); i++) {
            String column = incrementColumn.get(i);
            querySql.append(String.format(" `%s` ASC", column));
            if (i + 1 != incrementColumn.size()) {
                querySql.append(" ,");
            }
        }
        if (!querySql.toString().toUpperCase().contains(" LIMIT ")) {
            querySql.append(String.format(" LIMIT %s", limit));
        }
        return querySql.toString();
    }
}
