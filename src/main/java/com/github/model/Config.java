package com.github.model;

import com.github.util.A;
import com.github.util.U;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * <pre>
 * DB：Databases --> Tables --> Rows      --> Columns
 * ES：Indices   --> Types  --> Documents --> Fields
 *
 * new(>= 6.x version) es changes: Indices to Tables, 'Types' use default: _doc
 * </pre>
 */
@Getter
@Setter
public class Config {

    /** false will disabled to sync mysql data to es */
    private boolean enable = true;

    /** increment value storage, default has temp file: -Djava.io.tmpdir=/path/ */
    private IncrementStorageType incrementType = IncrementStorageType.TEMP_FILE;

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
     * default: every minutes
     *
     * @see org.springframework.scheduling.support.CronExpression#parse(java.lang.String)
     */
    private String cron = "0 * * * * *";

    /** whether to enable data compensation. default: false */
    private boolean enableCompensate = false;
    /** cron expression for data compensation. default: every 2 minutes on the 13th second */
    private String compensateCron = "13 0/2 * * * *";
    /** when sync data time less than this value from now, start compensate data. default: 20 minutes(1200 second) */
    private int beginCompensateSecond = 1200;
    /** start time when compensating data. default: 5 minutes(300 second) */
    private int compensateSecond = 300;

    /** false: no version check is done when writing es */
    private boolean versionCheck = true;

    /**
     * <pre>
     * [
     *   {
     *     "table": "table1 name",
     *     "type": "es type1 name",
     *     "sql": "sql",
     *     "limit": 10,
     *     "incrementColumn": "update_time",
     *     "mapping": {
     *       "table1 column1" : "type1 field1",
     *       "table1 column2" : "type1 field2",
     *       ...
     *     }
     *   },{
     *     "table": "type2 name",
     *     "incrementColumn": "id",
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
}
