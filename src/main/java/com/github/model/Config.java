package com.github.model;

import com.github.util.A;
import com.github.util.U;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * <pre>
 * DB：Databases --> Tables --> Rows      --> Columns
 * ES：Indices   --> Types  --> Documents --> Fields
 *
 * new es changes: Indices to Tables, 'Types' use default: _doc
 * </pre>
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class Config {

    private List<String> ipPort = Lists.newArrayList("127.0.0.1:9200");

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
     * @see org.springframework.scheduling.support.CronSequenceGenerator#doParse(String[])
     */
    private String cron = "0 * * * * *"; // every minutes with default

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
    private List<Relation> relation;

    public void check() {
        U.assertException(A.isEmpty(relation), "must set [db es] relation");
        for (Relation r : relation) {
            r.check();
        }
    }
    public String ipAndPort() {
        return A.isEmpty(ipPort) ? U.EMPTY : A.first(ipPort);
    }
}
