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

    /** Database table name */
    private String table;

    /** The field name for the increment in the table */
    private String incrementColumn;

    // Above two properties must be set, the following don't need.

    /** If sql has join, master table's alias */
    private String tableAlias;

    /** es index <==> database table name. it not, will generate by table name(t_some_one ==> someOne) */
    private String index;

    // begin with 6.0, type will be remove, replace with _doc
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
    //** es type */
    // private String type = "_doc";

    /** whether to generate scheme of es on the database table structure */
    private boolean scheme = false;

    /** whether to generate scheme of es on the database table structure */
    private boolean columnLowerCamel = false;

    /** database field used for routing: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-routing-field.html */
    private List<String> routeColumn;

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/optimistic-concurrency-control.html
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning
     */
    private String versionColumn;

    /** If it is a multi-table mapping, whether to stitch the info on the table to the id */
    private boolean patternToId = true;

    /** Operate sql statement. if not, will generate by table name(select * from table_name) */
    private String sql;

    /** Number of each operation. will append in sql(select ... limit 1000) */
    private Integer limit = 1000;

    /** table column -> es field. if not, will generate by column(c_some_type ==> someType) */
    private Map<String, String> mapping;

    /** one to one child mapping */
    private Map<String, ChildMapping> relationMapping;

    /** one to many(nested) child mapping */
    private Map<String, ChildMapping> nestedMapping;

    /** primary key, will generate to id in es, query from db table, if not, can't create index in es */
    private List<String> keyColumn;

    /** If want to ignore some column in SQL */
    private List<String> ignoreColumn;

    /** When use time field to increment, need primary key to generate Page SQL */
    private String primaryKey = "id";

    /**
     * When same time field's count(SELECT COUNT(*) FROM table WHERE time = 'xxx') >= this value, page sql will change
     *
     * old: SELECT a,b FROM table LIMIT 1000000,1000
     * new: SELECT a,b FROM table c INNER JOIN (SELECT id FROM table WHERE time > 'xxx' LIMIT 1000000,1000) t on c.id = t.id
     */
    private Integer bigCountToSql = 2000;

    private String idPrefix;
    private String idSuffix;

    public void check() {
        U.assertNil(table, "must set (db table name)");
        U.assertNil(incrementColumn, "must set (db table increment-column)");

        if (U.isNotBlank(limit)) {
            U.assert0(limit, "limit must greater 0");
        }
        if (U.isNotBlank(sql)) {
            sql = U.replaceBlank(sql);
        }
        if (A.isNotEmpty(relationMapping)) {
            for (Map.Entry<String, ChildMapping> entry : relationMapping.entrySet()) {
                String nested = entry.getKey();
                U.assertNil(nested, "one to one child key can't be null");
                entry.getValue().check(nested);
            }
        }
        if (A.isNotEmpty(nestedMapping)) {
            for (Map.Entry<String, ChildMapping> entry : nestedMapping.entrySet()) {
                String nested = entry.getKey();
                U.assertNil(nested, "nested child key can't be null");
                entry.getValue().check(nested);
            }
        }
    }

    public String useKey() {
        return String.format("table(%s) <=> index(%s)", table, useIndex());
    }

    /** if not set the 「type」, generate from 「table name」 */
    public String useIndex() {
        if (U.isNotBlank(index)) {
            return index;
        } else if (U.isNotBlank(table)) {
            String tmp;
            if (checkMatch()) {
                String match = "%";
                String split = "_";
                tmp = table.replace(match, U.EMPTY).replace(split + split, split);
                if (tmp.startsWith(split)) {
                    tmp = tmp.substring(1);
                }
                if (tmp.endsWith(split)) {
                    tmp = tmp.substring(0, tmp.length() - 1);
                }
            } else {
                tmp = table;
            }
            return U.tableToIndex(tmp);
        } else {
            return U.EMPTY;
        }
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
        return columnLowerCamel ? U.columnToField(column) : column;
    }

    /** generate desc sql */
    public String descSql(String table) {
        return String.format("DESC %s", table);
    }

    /** check table has match */
    public boolean checkMatch() {
        return table.contains("%");
    }
    /** query all tables with match */
    public String matchSql() {
        return String.format("SHOW TABLES LIKE '%s'", table);
    }
    /** get match info with table pattern */
    public String matchInfo(String realTable) {
        if (checkMatch() && patternToId) {
            // t_order_%        : t_order_202001        ==>  202001,   t_order_202002        ==>  202002
            // t_order_%_item_% : t_order_2020_item_01  ==>  2020-01,  t_order_2020_item_02  ==>  2020-02
            String tmp = realTable;
            String match = "%";
            String split = "-";
            for (String str : table.split(match)) {
                tmp = tmp.replaceFirst(str, split);
            }
            if (tmp.startsWith(split)) {
                tmp = tmp.substring(1);
            }
            if (tmp.endsWith(split)) {
                tmp = tmp.substring(0, tmp.length() - 1);
            }
            return tmp;
        } else {
            return U.EMPTY;
        }
    }

    /** select ... from ... where (id > xxx | time > '2010-10-10 00:00:01') order by (id | time) limit 1000 */
    public String querySql(String table, String param) {
        // just limit 1000, not limit 1000000, 1000
        return querySql(GT, table, param, 0);
    }

    /**
     * select ... from ... where time = '2010-10-10 00:00:01' limit 0|1000 , 1000
     * <br><br>or<br><br>
     * select cur.* from ... as cur inner join (select id from ... where time = '2010-01-01 00:00:01' limit 2000,1000) tmp on cur.id = tmp.id
     */
    public String equalsQuerySql(String table, String param, int page) {
        int pageStart = page * limit;
        if (pageStart >= bigCountToSql) {
            return bigPageSql(table, param, pageStart);
        } else {
            return querySql(EQUALS, table, param, pageStart);
        }
    }

    public int loopCount(int count) {
        int loop = count / limit;
        if (count % limit != 0) {
            loop += 1;
        }
        return loop;
    }

    /** select count(*) from ... where time = '2010-10-10 00:00:01' */
    public String equalsCountSql(String table, String param) {
        String count = "SELECT COUNT(*) FROM ";

        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            // ignore case, dot(.) can match all(include wrap tab)
            sbd.append(sql.trim().replaceFirst("(?is)SELECT (.*?) FROM ", count));
        } else {
            sbd.append(count).append("`").append(table).append("`");
        }
        appendWhere(EQUALS, param, sbd);
        return sbd.toString();
    }
    private void appendWhere(String operate, String param, StringBuilder sbd) {
        // param split length = increment column size
        if (U.isBlank(param)) {
            param = "0";
        }

        String where = " WHERE ";
        boolean containsWhere = sbd.toString().toUpperCase().contains(where);
        if (containsWhere) {
            sbd.append(" AND ").append("( ");
        } else {
            sbd.append(where);
        }
        if (U.isNotBlank(tableAlias)) {
            sbd.append(tableAlias).append(".");
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
    private String querySql(String operate, String table, String param, int pageStart) {
        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            sbd.append(sql);
        } else {
            sbd.append("SELECT * FROM `").append(table).append("`");
        }
        appendWhere(operate, param, sbd);
        if (!EQUALS.equals(operate)) {
            sbd.append(" ORDER BY ");
            if (U.isNotBlank(tableAlias)) {
                sbd.append(tableAlias).append(".");
            }
            sbd.append(incrementColumn);
        }
        sbd.append(" LIMIT ");
        if (U.greater0(pageStart)) {
            sbd.append(pageStart).append(", ");
        }
        sbd.append(limit);
        return sbd.toString();
    }
    private String bigPageSql(String table, String param, int pageStart) {
        StringBuilder sbd = new StringBuilder();
        if (U.isNotBlank(sql)) {
            sbd.append("SELECT CUR.* FROM (").append(sql).append(")");
        } else {
            sbd.append("SELECT CUR.* FROM `").append(table).append("`");
        }
        sbd.append(" as CUR");
        sbd.append(" INNER JOIN ( SELECT ").append(primaryKey);
        sbd.append(" FROM `").append(table).append("`");
        sbd.append(" WHERE ");
        if (incrementColumn.contains(".")) {
            sbd.append(incrementColumn.substring(incrementColumn.indexOf(".") + 1));
        } else {
            sbd.append(incrementColumn);
        }
        sbd.append(EQUALS);
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
