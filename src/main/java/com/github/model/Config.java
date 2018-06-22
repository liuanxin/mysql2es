package com.github.model;

import com.github.util.A;
import com.github.util.U;
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
        U.assertException(A.isEmpty(relation), "must set [db es] relation");
        for (Relation relation1 : relation) {
            relation1.check();
        }
    }
    public String ipAndPort() {
        return A.isEmpty(ipPort) ? U.EMPTY : ipPort.iterator().next();
    }
}
