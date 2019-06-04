package com.github.model;

import com.github.util.A;
import com.github.util.U;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class Relation {

    private static final String GT = " > ";
    private static final String EQUALS = " = ";

    /** database table name */
    private String table;

    /** The field name for the increment in the table */
    private String incrementColumn;

    // above two properties must be set, the following don't need.

    /** The field name for the increment in the table, if nil use incrementColumn */
    private String incrementColumnAlias;

    /** es index <==> database table name. it not, will generate by table name(t_some_one ==> someOne) */
    private String index;

    // begin with 6.0, type will be remove, replace with _doc
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
    /** es type */
    private String type = "_doc";

    /** whether to generate scheme of es on the database table structure */
    private boolean scheme = true;

    /** operate sql statement. if not, will generate by table name(select * from table_name) */
    private String sql;

    /**  number of each operation. will append in sql(select ... limit 1000) */
    private Integer limit = 1000;

    /** table column -> es field. if not, will generate by column(c_some_type ==> someType) */
    private Map<String, String> mapping;

    /**
     * primary key, will generate to id in es, query from db table, if not, can't create index in es
     *
     * @see org.elasticsearch.client.support.AbstractClient#prepareIndex(String, String, String)
     */
    private List<String> keyColumn;

    /** if want to ignore some column in SQL */
    private List<String> ignoreColumn;

    /** when use time field to increment, need primary key to generate Page SQL */
    private String primaryKey = "id";

    /**
     * when same time field's count(SELECT COUNT(*) FROM table WHERE time = 'xxx') >= this value, page sql will change
     *
     * old: SELECT a,b FROM table LIMIT 1000000,1000
     * new: SELECT a,b FROM table c INNER JOIN (SELECT id FROM table WHERE time > 'xxx' LIMIT 1000000,1000) t on c.id = t.id
     */
    private Integer bigCountToSql = 5000;

    private String idPrefix;
    private String idSuffix;

    void check() {
        U.assertNil(table, "must set (db table name)");
        U.assertNil(incrementColumn, "must set (db table increment-column)");

        if (U.isBlank(incrementColumnAlias)) {
            incrementColumnAlias = incrementColumn;
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
        if (U.isBlank(column) || (A.isNotEmpty(ignoreColumn) && ignoreColumn.contains(column))) {
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
        return String.format("DESC %s", table);
    }

    public String countSql(String param) {
        return countSql(GT, param);
    }
    public String querySql(String param) {
        return querySql(GT, param, 0);
    }

    // if use ... time > '2010-01-01 01:00:00', query: select count(*) ... time = '2010-01-01 01:00:00'

    public String equalsCountSql(String param) {
        return countSql(EQUALS, param);
    }
    public String equalsQuerySql(String param, int page) {
        int pageStart = page * limit;
        if (pageStart >= bigCountToSql) {
            return querySql(EQUALS, param, pageStart);
        } else {
            return bigPageSql(param, pageStart);
        }
    }

    public int loopCount(int count) {
        int loop = count / limit;
        if (count % limit != 0) {
            loop += 1;
        }
        return loop;
    }

    private String countSql(String operate, String param) {
        String count = "SELECT COUNT(*) FROM ";

        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            // ignore case, dot(.) can match all(include wrap tab)
            sbd.append(sql.trim().replaceFirst("(?is)SELECT (.*?) FROM ", count));
        } else {
            sbd.append(count).append(table);
        }
        appendWhere(operate, param, sbd);
        return sbd.toString();
    }
    private void appendWhere(String operate, String param, StringBuilder sbd) {
        // param split length = increment column size
        if (U.isNotBlank(param)) {
            String where = " WHERE ";

            boolean containsWhere = sbd.toString().toUpperCase().contains(where);
            if (containsWhere) {
                sbd.append(" AND ").append("( ");
            } else {
                sbd.append(where);
            }
            sbd.append(incrementColumn).append(operate);
            if (U.isNumber(param)) {
                sbd.append(param);
            } else {
                sbd.append("'").append(param).append("'");
            }

            if (containsWhere) {
                sbd.append(" )");
            }
        }
    }
    private String querySql(String operate, String param, int pageStart) {
        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            sbd.append(sql.trim());
        } else {
            sbd.append("SELECT * FROM ").append(table);
        }
        appendWhere(operate, param, sbd);
        if (!EQUALS.equals(operate)) {
            sbd.append(" ORDER BY ").append(incrementColumn);
        }
        sbd.append(" LIMIT ");
        if (U.greater0(pageStart)) {
            sbd.append(pageStart).append(", ");
        }
        sbd.append(limit);
        return sbd.toString();
    }
    private String bigPageSql(String param, int pageStart) {
        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            sbd.append(sql.trim());
        } else {
            sbd.append("SELECT * FROM ").append(table);
        }
        sbd.append(" as CUR");
        sbd.append(" INNER JOIN ( SELECT ").append(primaryKey);
        sbd.append(" FROM").append(table);
        sbd.append(" WHERE ").append(incrementColumn).append(" > ");
        if (U.isNumber(param)) {
            sbd.append(param);
        } else {
            sbd.append("'").append(param).append("'");
        }
        sbd.append(" LIMIT ").append(pageStart).append(", ").append(limit);
        sbd.append(" ) TMP on CUR.").append(primaryKey).append(" = TMP.").append(primaryKey);
        return sbd.toString();
    }
}
