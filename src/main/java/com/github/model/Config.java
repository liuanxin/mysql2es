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
     *
     * @see org.springframework.scheduling.support.CronSequenceGenerator#doParse(String[])
     */
    @SuppressWarnings("deprecation")
    private String cron = "0 * * * * *"; // every minutes

    private boolean enableCompensate = false;
    private String compensateCron = "13 0/2 * * * *"; // every 2 minutes on the 13th second
    private int beginCompensateSecond = 1200; // sync data is within 20 minutes
    private int compensateSecond = 300; // Sync data within 5 minutes

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
