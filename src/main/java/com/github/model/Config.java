package com.github.model;

import com.github.util.A;
import com.github.util.Logs;
import com.github.util.U;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;
import java.util.Map;

/**
 * <pre>
 * DB：Databases --> Tables --> Rows      --> Columns
 * ES：Indices   --> Types  --> Documents --> Fields
 * </pre>
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class Config {

    List<String> ipPort = A.lists("127.0.0.1:9300");

    /** ES index <==> database Name */
    String index;

    /**
     * <pre>
     * .---------------- second (0 - 59)         if (0/10) then (0, 10, 20, 30, 40, 50) run
     * |  .---------------- minute (0 - 59)
     * |  |  .------------- hour (0 - 23)
     * |  |  |  .---------- day of month (1 - 31)
     * |  |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec
     * |  |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
     * |  |  |  |  |  |
     * *  *  *  *  *  *
     * </pre>
     *
     * @see org.springframework.scheduling.support.CronSequenceGenerator#parse(java.lang.String)
     */
    String cron = "0 * * * * *"; // every minutes with default

    /**
     * <pre>
     * [
     *   {
     *     "table": "table1 name",
     *     "type": "es type1 name",
     *     "sql": "sql",
     *     "limit": 10,
     *     "incrementColumn": "UPDATE_TIME",
     *     "mapping": {
     *       "table1 column1" : "type1 field1",
     *       "table1 column2" : "type1 field2",
     *       ...
     *     }
     *   },{
     *     "table": "type2 name",
     *     "incrementColumn": "CREATE_TIME",
     *     ...
     *   }
     *   ...
     * ]
     * </pre>
     */
    List<Relation> relation;

    public void check() {
        U.assertNil(index, "must set (es index name) <==> database name");
        U.assertException(relation == null || A.isEmpty(relation), "must set [db es] relation");
        for (Relation relation1 : relation) {
            relation1.check();
        }
    }
    public String ipAndPort() {
        return A.isEmpty(ipPort) ? U.EMPTY : ipPort.iterator().next();
    }


    @Getter
    @Setter
    public static class Relation {
        /** database table name */
        String table;
        /** The field name for the increment in the table */
        List<String> incrementColumn;

        // above two properties must be set, the following don't need.

        /** es type <==> database table name. it not, will generate by table name(t_some_one ==> someOne) */
        String type;
        /** whether to generate scheme of es on the database table structure */
        boolean scheme = true;
        /** operate sql statement. if not, will generate by table name(select * from table_name) */
        String sql;
        /** number of each operation. will append in sql(select ... limit 50) */
        Integer limit = 50;
        /** table column -> es field. if not, will generate by column(c_some_type ==> someType) */
        Map<String, String> mapping;

        /**
         * primary key, will generate to id in es, query from db table, if not, can't create index in es
         *
         * @see org.elasticsearch.client.support.AbstractClient#prepareIndex(java.lang.String, java.lang.String, java.lang.String)
         */
        List<String> keyList;

        private void check() {
            U.assertNil(table, "must set (db table name)");
            U.assertException(A.isEmpty(incrementColumn), "must set (db table increment-column)");
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
            return String.format("desc `%s`", table);
        }
        /** generate query sql */
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
}
