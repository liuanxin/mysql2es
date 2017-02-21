package com.github.mte.model;

import com.github.mte.util.A;
import com.github.mte.util.Logs;
import com.github.mte.util.U;
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
        relation.forEach(Config.Relation::check);
    }
    public String ipAndPort() {
        return A.isEmpty(ipPort) ? U.EMPTY : ipPort.iterator().next();
    }


    @Getter
    @Setter
    public static class Relation {
        String table;
        String type;
        String sql;
        Integer limit = new Integer(50);
        List<String> incrementColumn;
        Map<String, String> mapping;

        /** primary key, query from db table, if not, can't create index in es */
        List<String> keyList;

        private void check() {
            U.assertNil(table, "must set (db table name)");
            U.assertException(A.isEmpty(incrementColumn), "must set (db table increment-column)");
        }

        /** if not set the 「type」, generate from 「table name」 */
        public String useType() {
            if (U.isNotBlank(type)) return type;
            if (U.isNotBlank(table)) return U.tableToType(table);
            return U.EMPTY;
        }
        /** if not config the 「mapping」, generate from 「column name」 */
        public String useField(String column) {
            if (U.isBlank(column)) return U.EMPTY;

            if (A.isNotEmpty(mapping)) {
                String field = mapping.get(column);
                if (U.isNotBlank(field)) return field;
            }
            return U.columnToField(column);
        }
        /** generate query sql */
        public String querySql(String param) {
            check();

            StringBuilder querySql = new StringBuilder();
            querySql.append(U.isNotBlank(sql) ? sql : String.format("SELECT * FROM `%s`", table));

            // param split length = increment column size
            if (U.isNotBlank(param)) {
                String params[] = param.split(U.SPLIT);
                if (incrementColumn.size() != params.length) {
                    if (Logs.ROOT_LOG.isErrorEnabled())
                        Logs.ROOT_LOG.error("increment ({}) != param ({})", A.toStr(incrementColumn), param);
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
