package com.github.repository;

import com.github.model.ChildMapping;
import com.github.model.Config;
import com.github.model.IncrementStorageType;
import com.github.model.Relation;
import com.github.util.*;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import lombok.AllArgsConstructor;
import org.elasticsearch.common.UUIDs;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@AllArgsConstructor
@SuppressWarnings({"rawtypes", "DuplicatedCode", "UnusedReturnValue"})
public class DataRepository {

    private static final Map<String, AtomicBoolean> SYNC_RUN = new ConcurrentHashMap<>();
    private static final Map<String, AtomicBoolean> COMPENSATE_RUN = new ConcurrentHashMap<>();

    private static final String EQUALS_SUFFIX = "<-_->";
    private static final String EQUALS_I_SPLIT = "<=_=>";
    private static final String COMPENSATE_SUFFIX = "=compensate";
    private static final Date NIL_DATE_TIME = new Date(0L);

    /**
     * <pre>
     * CREATE TABLE IF NOT EXISTS `t_db_to_es` (
     *   `table_index` VARCHAR(128) NOT NULL COMMENT '表 + es index',
     *   `increment_value` VARCHAR(256) NOT NULL COMMENT '增量数据值',
     *   `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
     *   `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
     *   PRIMARY KEY (`table_index`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'db 到 es 的增量记录';
     * </pre>
     */
    private static final String GENERATE_TABLE =
            "CREATE TABLE IF NOT EXISTS `t_db_to_es` (" +
            "  `table_index` VARCHAR(128) NOT NULL," +
            "  `increment_value` VARCHAR(256) NOT NULL," +
            "  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            "  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
            "  PRIMARY KEY (`table_index`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    /* 「replace into」 will cover create_time and update_time to now() */
    private static final String ADD_INCREMENT = "INSERT INTO `t_db_to_es`(`table_index`, `increment_value`) " +
            "VALUES(?, ?) ON DUPLICATE KEY UPDATE `increment_value` = VALUES(`increment_value`)";
    private static final String GET_INCREMENT = "SELECT `increment_value` FROM `t_db_to_es` WHERE `table_index` = ?";


    private final Config config;
    private final JdbcTemplate jdbcTemplate;
    private final EsRepository esRepository;


    public void generateIncrementTable() {
        jdbcTemplate.execute(GENERATE_TABLE);
    }

    private String getLastValue(IncrementStorageType incrementType, String table, String incrementColumn, String index) {
        String tableColumn = getTableColumn(table, incrementColumn);
        if (incrementType == IncrementStorageType.MYSQL) {
            String name = F.fileNameOrTableKey(tableColumn, index);
            return A.first(jdbcTemplate.queryForList(GET_INCREMENT, String.class, name));
        } else {
            return F.read(tableColumn, index);
        }
    }
    private String getTableColumn(String table, String incrementColumn) {
        return table + "-" + incrementColumn;
    }
    private void saveLastValue(IncrementStorageType incrementType, String table,
                               String incrementColumn, String index, String value) {
        String tableColumn = getTableColumn(table, incrementColumn);
        if (incrementType == IncrementStorageType.MYSQL) {
            String name = F.fileNameOrTableKey(tableColumn, index);
            jdbcTemplate.update(ADD_INCREMENT, name, value);
        } else {
            F.write(tableColumn, index, value);
        }
    }

    /** generate scheme of es on the database table structure */
    public Map<String, Map> dbToEsScheme(Relation relation) {
        String relationTable = relation.getTable();
        String table;
        if (relation.checkMatch()) {
            String sql = relation.matchSql();
            List<String> matchTables = jdbcTemplate.queryForList(sql, String.class);
            table = A.first(matchTables);
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("sql({}) return({}), use `{}` to check basic info",
                        getSql(sql), A.toStr(matchTables), table);
            }
        } else {
            table = relationTable;
        }

        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(relation.descSql(table));
        if (A.isNotEmpty(mapList)) {
            boolean scheme = relation.isScheme();
            List<String> idList = Lists.newArrayList();
            Map<String, Map> propertyMap = Maps.newHashMap();
            Map<String, Boolean> fieldMap = Maps.newHashMap();
            for (Map<String, Object> map : mapList) {
                Object column = map.get("Field");
                Object type = map.get("Type");

                if (U.isNotBlank(column) && U.isNotBlank(type)) {
                    String field = column.toString();
                    fieldMap.put(field, true);

                    Object key = map.get("Key");
                    if (U.isNotBlank(key) && "PRI".equals(key)) {
                        idList.add(field);
                    }
                    if (scheme) {
                        propertyMap.put(relation.useField(field), Searchs.dbToEsType(type.toString()));
                    }
                }
            }

            List<String> idColumn = relation.getIdColumn();
            if (A.isEmpty(idColumn)) {
                if (A.isEmpty(idList)) {
                    U.assertException(String.format("table (%s) no primary key, can't create index in es!", table));
                }
                if (idList.size() > 1) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("table ({}) has multi primary key({})", table, A.toStr(idList));
                    }
                }
                relation.setIdColumn(idList);
            } else {
                for (String id : idColumn) {
                    U.assertNil(fieldMap.get(id), String.format("table(%s) don't have column(%s)", table, id));
                }
            }
            return propertyMap;
        }
        return Collections.emptyMap();
    }

    @Async
    public Future<Void> asyncCompensateData(IncrementStorageType incrementType, Relation relation,
                                            int beginCompensateSecond, int compensateSecond) {
        if (!config.isEnable() || !config.isEnableCompensate()) {
            return new AsyncResult<>(null);
        }

        String index = relation.useIndex();
        AtomicBoolean run = COMPENSATE_RUN.computeIfAbsent(index, key -> new AtomicBoolean(false));
        if (run.compareAndSet(false, true)) {
            try {
                String table = relation.getTable();
                if (U.isNotBlank(table) && U.isNotBlank(index)) {
                    long start = System.currentTimeMillis();
                    List<String> matchTables;
                    if (relation.checkMatch()) {
                        String sql = relation.matchSql();
                        matchTables = jdbcTemplate.queryForList(sql, String.class);
                        long sqlTime = (System.currentTimeMillis() - start);
                        if (Logs.ROOT_LOG.isDebugEnabled()) {
                            Logs.ROOT_LOG.debug("compensate sql({}) time({}ms) return({}), size({})",
                                    getSql(sql), sqlTime, A.toStr(matchTables), matchTables.size());
                        }
                    } else {
                        matchTables = Collections.singletonList(table);
                    }

                    AtomicLong increment = new AtomicLong();
                    for (String matchTable : matchTables) {
                        saveSingleTable(incrementType, relation, index, matchTable, increment, beginCompensateSecond, compensateSecond);
                    }
                    if (Logs.ROOT_LOG.isInfoEnabled()) {
                        long count = increment.get();
                        long end = System.currentTimeMillis();
                        long ms = end - start;
                        String tps = (count > 0) ? String.valueOf(count * 1000 / ms) : "0";
                        Logs.ROOT_LOG.info("compensate async({}) count({}), {} -> {} time({}) tps({})", index, count,
                                Dates.format(new Date(start), Dates.Type.YYYY_MM_DD_HH_MM_SSSSS),
                                Dates.format(new Date(end), Dates.Type.YYYY_MM_DD_HH_MM_SSSSS),
                                Dates.toHuman(ms), tps);
                    }
                }
            } catch (Exception e) {
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error(String.format("compensate async(%s) has exception", index), e);
                }
            } finally {
                run.set(false);
            }
        } else {
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("compensate task ({}) has running", index);
            }
        }
        return new AsyncResult<>(null);
    }

    @Async
    public Future<Void> asyncData(IncrementStorageType incrementType, Relation relation) {
        if (!config.isEnable()) {
            return new AsyncResult<>(null);
        }

        String index = relation.useIndex();
        AtomicBoolean run = SYNC_RUN.computeIfAbsent(index, key -> new AtomicBoolean(false));
        if (run.compareAndSet(false, true)) {
            try {
                String table = relation.getTable();
                if (U.isNotBlank(table) && U.isNotBlank(index)) {
                    long start = System.currentTimeMillis();
                    List<String> matchTables;
                    if (relation.checkMatch()) {
                        String sql = relation.matchSql();
                        matchTables = jdbcTemplate.queryForList(sql, String.class);
                        long sqlTime = (System.currentTimeMillis() - start);
                        if (Logs.ROOT_LOG.isDebugEnabled()) {
                            Logs.ROOT_LOG.debug("sql({}) time({}ms) return({}), size({})",
                                    getSql(sql), sqlTime, A.toStr(matchTables), matchTables.size());
                        }
                    } else {
                        matchTables = Collections.singletonList(table);
                    }

                    AtomicLong increment = new AtomicLong(0L);
                    for (String matchTable : matchTables) {
                        saveSingleTable(incrementType, relation, index, matchTable, increment, 0, 0);
                    }
                    if (Logs.ROOT_LOG.isInfoEnabled()) {
                        long count = increment.get();
                        long end = System.currentTimeMillis();
                        long ms = end - start;
                        String tps = (count > 0) ? String.valueOf(count * 1000 / ms) : "0";
                        // equals data will sync multi times, It will larger than db's count
                        Logs.ROOT_LOG.info("async({}) count({}), {} -> {} time({}) tps({})", index, count,
                                Dates.format(new Date(start), Dates.Type.YYYY_MM_DD_HH_MM_SSSSS),
                                Dates.format(new Date(end), Dates.Type.YYYY_MM_DD_HH_MM_SSSSS),
                                Dates.toHuman(ms), tps);
                    }
                }
            } catch (Exception e) {
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error(String.format("async(%s) has exception", index), e);
                }
            } finally {
                run.set(false);
            }
        } else {
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("async task ({}) has running", index);
            }
        }
        return new AsyncResult<>(null);
    }

    private void saveSingleTable(IncrementStorageType incrementType, Relation relation, String index,
                                 String matchTable, AtomicLong increment, int beginCompensateSecond, int compensateSecond) {
        if (!config.isEnable()) {
            throw new RuntimeException("break sync db to es");
        }

        String lastValue = getLastValue(incrementType, matchTable, relation.getIncrementColumn(), index);
        boolean hasCompensate = (compensateSecond > 0) && U.isNotBlank(lastValue);
        if (hasCompensate) {
            String oldValue = lastValue.split(EQUALS_I_SPLIT)[0];
            Date date = Dates.parse(oldValue);
            if (U.isNotNull(date)) {
                if (beginCompensateSecond > 0) {
                    Date startCompensate = Dates.addSecond(Dates.now(), -beginCompensateSecond);
                    if (startCompensate.getTime() > date.getTime()) {
                        if (Logs.ROOT_LOG.isInfoEnabled()) {
                            Logs.ROOT_LOG.info("index({}) compensation will start from({}), current has ({}), not be operated",
                                    index, Dates.format(startCompensate, Dates.Type.YYYY_MM_DD_HH_MM_SS), oldValue);
                        }
                        return;
                    }
                }
                lastValue = Dates.format(Dates.addSecond(date, -compensateSecond), Dates.Type.YYYY_MM_DD_HH_MM_SS);
                saveLastValue(incrementType, matchTable, relation.getIncrementColumn() + COMPENSATE_SUFFIX, index, lastValue);
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("compensate old({}) => current({})", oldValue, lastValue);
                }
            }
        }
        String matchInId = relation.matchInfo(matchTable);
        for (;;) {
            lastValue = handleGreaterAndEquals(incrementType, relation, matchTable, lastValue, matchInId, increment, hasCompensate);
            if (U.isBlank(lastValue)) {
                return;
            }
        }
    }

    private String handleGreaterAndEquals(IncrementStorageType incrementType, Relation relation, String matchTable,
                                          String lastValue, String matchInId, AtomicLong increment, boolean hasCompensate) {
        if (!config.isEnable()) {
            throw new RuntimeException("break sync db to es");
        }

        if (U.isNotBlank(lastValue) && lastValue.endsWith(EQUALS_SUFFIX)) {
            handleEquals(incrementType, relation, matchTable, lastValue, 0, matchInId, 0, increment, false);
            return lastValue.substring(0, lastValue.length() - EQUALS_SUFFIX.length());
        }

        String sql = relation.querySql(matchTable, lastValue);
        long start = System.currentTimeMillis();
        List<Map<String, Object>> dataList = jdbcTemplate.queryForList(sql);
        if (A.isEmpty(dataList)) {
            // if not data, can break loop
            return null;
        }
        long sqlTime = (System.currentTimeMillis() - start);
        if (Logs.ROOT_LOG.isDebugEnabled()) {
            Logs.ROOT_LOG.debug("{}sql({}) time({}ms) return size({})",
                    (hasCompensate ? "compensate " : ""), getSql(sql), sqlTime, dataList.size());
        }

        Map<String, List<Map<String, Object>>> relationData = childData(relation.getRelationMapping(), dataList);
        Map<String, List<Map<String, Object>>> nestedData = childData(relation.getNestedMapping(), dataList);
        long allSqlTime = (System.currentTimeMillis() - start);

        long esStart = System.currentTimeMillis();
        Map<String, Map<String, Map<String, String>>> esDataMap =
                fixIndexDocument(relation, dataList, matchInId, relationData, nestedData);
        int size = esRepository.saveDataToEs(esDataMap);
        increment.addAndGet(size);
        long end = System.currentTimeMillis();
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("{}greater({}) sql table({}) time({}ms) size({}) batch to es({}) time({}ms) success({}), all time({}ms)",
                    (hasCompensate ? "compensate " : ""), lastValue, matchTable, allSqlTime, dataList.size(),
                    esDataMap.keySet(), (end - esStart), size, (end - start));
        }
        if (size == 0) {
            // if write to es false, can break loop
            return null;
        }

        Map<String, Integer> lastAndCount = getLast(relation, dataList);
        if (A.isEmpty(lastAndCount)) {
            return null;
        }
        lastValue = A.first(lastAndCount.keySet());
        if (U.isBlank(lastValue)) {
            return null;
        }
        if (U.isBlank(lastValue)) {
            // if last data was nil, can break loop
            return null;
        }
        handleEquals(incrementType, relation, matchTable, lastValue, lastAndCount.get(lastValue), matchInId, 0, increment, hasCompensate);
        // write last record
        String incrementColumn = relation.getIncrementColumn() + (hasCompensate ? COMPENSATE_SUFFIX : "");
        saveLastValue(incrementType, matchTable, incrementColumn, relation.useIndex(), lastValue);

        // if sql: limit 1000, query data size 900, can break loop
        if (dataList.size() < relation.getLimit()) {
            return null;
        }
        return lastValue;
    }
    private void handleEquals(IncrementStorageType incrementType, Relation relation, String matchTable,
                              String tempColumnValue, int lastDataCount, String matchInId, int lastEqualsCount,
                              AtomicLong increment, boolean hasCompensate) {
        if (!config.isEnable()) {
            throw new RuntimeException("break sync db to es");
        }

        String eai;
        if (tempColumnValue.endsWith(EQUALS_SUFFIX)) {
            eai = tempColumnValue.substring(0, tempColumnValue.length() - EQUALS_SUFFIX.length());
        } else {
            eai = tempColumnValue;
        }
        String[] equalsArr = eai.split(EQUALS_I_SPLIT);
        String equalsValue = equalsArr[0];
        long nowMs = System.currentTimeMillis();

        // pre: time > '2010-10-10 00:00:01' | 1286640001000, current: time = '2010-10-10 00:00:01' | 1286640001000
        String equalsCountSql = relation.equalsCountSql(matchTable, equalsValue);
        long start = System.currentTimeMillis();
        Integer equalsCount = A.first(jdbcTemplate.queryForList(equalsCountSql, Integer.class));
        if (Logs.ROOT_LOG.isDebugEnabled()) {
            Logs.ROOT_LOG.debug("{}equals count sql({}) time({}ms) return({})",
                    (hasCompensate ? "compensate " : ""), getSql(equalsCountSql), (System.currentTimeMillis() - start), equalsCount);
        }
        if (U.less0(equalsCount)) {
            currentSecondHandle(equalsValue, nowMs, incrementType, relation,
                    matchTable, 0, matchInId, 0, increment, hasCompensate);
            return;
        }
        if (U.greater0(lastDataCount) && equalsCount == lastDataCount) {
            currentSecondHandle(equalsValue, nowMs, incrementType, relation,
                    matchTable, 0, matchInId, 0, increment, hasCompensate);
            return;
        }
        if (lastEqualsCount > 0 && lastEqualsCount == equalsCount) {
            currentSecondHandle(equalsValue, nowMs, incrementType, relation,
                    matchTable, 0, matchInId, equalsCount, increment, hasCompensate);
            return;
        }

        int equalsLoopCount = relation.loopCount(equalsCount);
        int i = 0;
        if (equalsArr.length == 2) {
            i = U.toInt(equalsArr[1]);
            // if count = 1000, limit = 10, save in file has 101, can be return right now!
            if (i * relation.getLimit() > equalsCount) {
                currentSecondHandle(equalsValue, nowMs, incrementType, relation,
                        matchTable, 0, matchInId, equalsCount, increment, hasCompensate);
                return;
            }
            if (i < 0) {
                i = 0;
            }
        }
        for (; i < equalsLoopCount; i++) {
            String equalsSql = relation.equalsQuerySql(matchTable, equalsValue, i);
            long sqlStart = System.currentTimeMillis();
            List<Map<String, Object>> equalsDataList = jdbcTemplate.queryForList(equalsSql);
            // if not data, can break equals handle
            if (A.isEmpty(equalsDataList)) {
                currentSecondHandle(equalsValue, nowMs, incrementType, relation,
                        matchTable, i, matchInId, equalsCount, increment, hasCompensate);
                return;
            }
            long sqlTime = (System.currentTimeMillis() - sqlStart);
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("{}equals sql({}) time({}ms) return size({})",
                        (hasCompensate ? "compensate " : ""), getSql(equalsSql), sqlTime, equalsDataList.size());
            }

            Map<String, List<Map<String, Object>>> relationData = childData(relation.getRelationMapping(), equalsDataList);
            Map<String, List<Map<String, Object>>> nestedData = childData(relation.getNestedMapping(), equalsDataList);
            long allSqlTime = (System.currentTimeMillis() - sqlStart);

            long esStart = System.currentTimeMillis();
            Map<String, Map<String, Map<String, String>>> esDataMap =
                    fixIndexDocument(relation, equalsDataList, matchInId, relationData, nestedData);
            int size = esRepository.saveDataToEs(esDataMap);
            increment.addAndGet(size);
            long end = System.currentTimeMillis();
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("{}equals({} : {} -> {}) sql table({}) time({}ms) size({}) batch to es({}) time({}ms) success({}), all time({}ms)",
                        (hasCompensate ? "compensate " : ""), equalsValue, i, equalsLoopCount, matchTable,
                        allSqlTime, equalsDataList.size(), esDataMap.keySet(), (end - esStart), size, (end - sqlStart));
            }

            // if success was 0, can break equals handle
            // if sql: limit 1000, 1000, query data size 900, can break equals handle
            if ((size == 0) || (equalsDataList.size() < relation.getLimit())) {
                currentSecondHandle(equalsValue, nowMs, incrementType, relation,
                        matchTable, i, matchInId, equalsCount, increment, hasCompensate);
                return;
            } else {
                // write current equals record
                String valueToSave = equalsValue + EQUALS_I_SPLIT + i + EQUALS_SUFFIX;
                String incrementColumn = relation.getIncrementColumn() + (hasCompensate ? COMPENSATE_SUFFIX : "");
                saveLastValue(incrementType, matchTable, incrementColumn, relation.useIndex(), valueToSave);
            }
        }
    }

    /**
     * If the incremental data is the current second, sleep to the next second and perform the above = operation again.
     * This is to avoid writing when there is no processing in the current second during synchronization (such as
     * the millisecond before the current second) and then jump Elapsed, but when it comes to the milliseconds
     * after the current second, the database (especially mysql will automatically round off when
     * processing milliseconds) has manipulated the data again.
     *
     * If this is not done, the records of database operations will not be synchronized
     * when the number of milliseconds after the current second
     */
    private void currentSecondHandle(String equalsValue, long nowMs, IncrementStorageType incrementType,
                                     Relation relation, String matchTable, int i, String matchInId,
                                     int currentEqualsCount, AtomicLong increment, boolean hasCompensate) {
        Date equalsDate = Dates.parse(equalsValue);
        if (U.isNotNull(equalsDate)) {
            long equalsMs = equalsDate.getTime();
            boolean needSleepToNextSecond = (nowMs > equalsMs) && (equalsMs / 1000 == nowMs / 1000);
            if (needSleepToNextSecond) {
                try {
                    long nextSecondMs = ((nowMs / 1000) + 1) * 1000;
                    Thread.sleep(nextSecondMs - nowMs);
                    handleEquals(incrementType, relation, matchTable, equalsValue + EQUALS_I_SPLIT + i, 0,
                            matchInId, currentEqualsCount, increment, hasCompensate);
                } catch (InterruptedException e) {
                    if (Logs.ROOT_LOG.isErrorEnabled()) {
                        Logs.ROOT_LOG.error("increment value has current ms, sleep to next second exception", e);
                    }
                }
            }
        }
    }

    private Map<String, List<Map<String, Object>>> childData(Map<String, ChildMapping> childMapping, List<Map<String, Object>> dataList) {
        if (A.isEmpty(dataList)) {
            return Collections.emptyMap();
        }

        Map<String, List<Map<String, Object>>> returnMap = Maps.newHashMap();
        if (A.isNotEmpty(childMapping)) {
            for (Map.Entry<String, ChildMapping> entry : childMapping.entrySet()) {
                String key = entry.getKey();
                ChildMapping nested = entry.getValue();

                List<Object> relations = Lists.newArrayList();
                for (Map<String, Object> data : dataList) {
                    relations.add(data.get(nested.getMainField()));
                }
                if (A.isNotEmpty(relations)) {
                    String sql = nested.nestedQuerySql(relations);
                    if (U.isNotBlank(sql)) {
                        long start = System.currentTimeMillis();
                        List<Map<String, Object>> nestedDataList = jdbcTemplate.queryForList(sql);
                        if (Logs.ROOT_LOG.isDebugEnabled()) {
                            Logs.ROOT_LOG.debug("child({}) sql({}) time({}ms) return size({})",
                                    key, getSql(sql), (System.currentTimeMillis() - start), nestedDataList.size());
                        }
                        returnMap.put(key, nestedDataList);
                    }
                }
            }
        }
        return returnMap;
    }

    private static String getSql(String sql) {
        if (U.isBlank(sql)) {
            return U.EMPTY;
        }
        int max = 200;
        if (sql.length() <= max) {
            return sql;
        }
        String fromSql = sql.substring(sql.toUpperCase().indexOf(" FROM ") + 1);
        if (fromSql.length() <= max) {
            return fromSql;
        }
        String[] valueArr = fromSql.split(",");
        int len = valueArr.length;
        if (len <= 10) {
            return fromSql;
        }
        return Joiner.on(", ").join(Arrays.asList(valueArr[0], (". " + (valueArr.length - 2) + " ."), valueArr[len - 1]));
    }
    /** write last record */
    private Map<String, Integer> getLast(Relation relation, List<Map<String, Object>> dataList) {
        String column = relation.getIncrementColumn();

        String lastIncrementData = getIncrementData(A.last(dataList), column);
        if (U.isBlank(lastIncrementData)) {
            return Collections.emptyMap();
        }

        Map<String, Integer> dataCountMap = Maps.newHashMap();
        String firstIncrementData = getIncrementData(A.first(dataList), column);
        if (U.isNotNull(firstIncrementData) && firstIncrementData.equals(lastIncrementData)) {
            dataCountMap.put(lastIncrementData, dataList.size());
            return dataCountMap;
        }

        dataCountMap.put(lastIncrementData, 1);
        // if has [1,2,3,4,5], just handle [2,3,4], ignore head and tail
        for (int i = dataList.size() - 2; i >= 1; i--) {
            String incrementData = getIncrementData(dataList.get(i), column);
            if (lastIncrementData.equals(incrementData)) {
                dataCountMap.put(lastIncrementData, dataCountMap.get(lastIncrementData) + 1);
            } else {
                return dataCountMap;
            }
        }
        return dataCountMap;
    }
    private String getIncrementData(Map<String, Object> data, String column) {
        if (A.isEmpty(data)) {
            return null;
        }
        Object obj = data.get(column.contains(".") ? column.substring(column.indexOf(".") + 1) : column);
        if (U.isBlank(obj)) {
            return null;
        }

        if (obj instanceof Date) {
            return Dates.format((Date) obj, Dates.Type.YYYY_MM_DD_HH_MM_SS);
        } else {
            return obj.toString();
        }
    }

    private String handleIndex(String index, Relation relation, Map<String, Object> data) {
        String templateColumn = relation.getTemplateColumn();
        if (U.isNotBlank(templateColumn)) {
            String template = U.toStr(data.get(templateColumn));
            Date datetime = Dates.parse(template);
            if (U.isNotBlank(datetime)) {
                return index + Dates.format(datetime, relation.getTemplatePattern());
            } else if (U.isNumber(template)) {
                return index + template;
            } else {
                throw new RuntimeException(String.format(
                        "templateColumn(%s) in data(%s: %s) is not a Date type and not a Number type",
                        templateColumn, index, data));
            }
        }
        return index;
    }

    /**
     * organize db result to es Document
     *
     * {
     *   index1 : { id1 : data1, id2 : data2 },
     *   index2 : { id3 : data3, id4 : data4 }
     * }
     *
     * data1: { id: id1, name: xxx ... }
     * data2: { id: id2, name: yyy ... }
     * data3: { id: id3, name: zzz ... }
     * data4: { id: id4, name: abc ... }
     */
    private Map<String, Map<String, Map<String, String>>> fixIndexDocument(
            Relation relation,
            List<Map<String, Object>> dataList,
            String matchInId,
            Map<String, List<Map<String, Object>>> relationData,
            Map<String, List<Map<String, Object>>> nestedData
    ) {
        Map<String, ChildMapping> relationMapping = relation.getRelationMapping();
        Map<String, Map<String, Map<String, Object>>> relationMap = handleRelation(relationData, relationMapping);

        Map<String, ChildMapping> nestedMapping = relation.getNestedMapping();
        Map<String, Multimap<String, Map<String, Object>>> nestedMap = handleNested(nestedData, nestedMapping);

        Map<String, Map<String, Map<String, String>>> documentMap = Maps.newHashMap();
        int saveSize = 0;
        for (Map<String, Object> data : dataList) {
            fillRelation(relationMapping, relationMap, data);
            fillNested(nestedMapping, nestedMap, data);

            Map<String, Object> dataMap = handleData(relation, data);
            // Document no data, don't need to save? or update to nil?
            if (A.isNotEmpty(dataMap)) {
                Map<String, String> source = Maps.newHashMap();
                source.put("data", Jsons.toJson(dataMap));

                fillRouteOnSingle(relation, data, source);
                fillVersionOnSingle(relation, data, source);

                String realIndex = handleIndex(relation.getIndex(), relation, data);
                Map<String, Map<String, String>> idDataMap = documentMap.get(realIndex);
                if (A.isEmpty(idDataMap)) {
                    idDataMap = Maps.newHashMap();
                }
                String id = handleId(relation, matchInId, data);
                idDataMap.put(id, source);
                documentMap.put(realIndex, idDataMap);
                saveSize++;
            }
        }
        if (saveSize < dataList.size()) {
            if (Logs.ROOT_LOG.isWarnEnabled()) {
                Logs.ROOT_LOG.warn("db size({}) <--> es size({})", dataList.size(), saveSize);
            }
        }
        return documentMap;
    }

    private String handleId(Relation relation, String matchInId, Map<String, Object> data) {
        StringBuilder idBuild = new StringBuilder();
        String idPrefix = relation.getIdPrefix();
        if (U.isNotBlank(idPrefix)) {
            idBuild.append(idPrefix.trim());
        }
        if (U.isNotBlank(matchInId)) {
            if (idBuild.length() > 0) {
                idBuild.append("-");
            }
            idBuild.append(matchInId.trim());
        }
        for (String idColumn : relation.getIdColumn()) {
            if (idBuild.length() > 0) {
                idBuild.append("-");
            }
            String str = U.toStr(data.get(idColumn));
            if (U.isNotBlank(str)) {
                idBuild.append(str.trim());
            }
        }
        String idSuffix = relation.getIdSuffix();
        if (U.isNotBlank(idSuffix)) {
            if (idBuild.length() > 0) {
                idBuild.append("-");
            }
            idBuild.append(idSuffix.trim());
        }
        // if not has id, use es generator
        return idBuild.length() == 0 ? UUIDs.base64UUID() : idBuild.toString();
    }

    private Map<String, Map<String, Map<String, Object>>> handleRelation(Map<String, List<Map<String, Object>>> relationData,
                                                                         Map<String, ChildMapping> relationMapping) {
        Map<String, Map<String, Map<String, Object>>> relationMap = Maps.newHashMap();
        if (A.isNotEmpty(relationMapping)) {
            for (Map.Entry<String, ChildMapping> entry : relationMapping.entrySet()) {
                String key = entry.getKey();
                ChildMapping child = entry.getValue();

                List<Map<String, Object>> list = relationData.get(key);
                if (A.isNotEmpty(list)) {
                    String tableField = child.getChildField();
                    Map<String, Map<String, Object>> dataMap = Maps.newHashMap();
                    for (Map<String, Object> data : list) {
                        String fieldData = U.toStr(data.get(tableField));
                        if (U.isNotBlank(fieldData)) {
                            data.remove(tableField);
                            dataMap.put(fieldData, data);
                        }
                    }
                    relationMap.put(key, dataMap);
                }
            }
        }
        return relationMap;
    }

    private Map<String, Multimap<String, Map<String, Object>>> handleNested(Map<String, List<Map<String, Object>>> nestedData,
                                                                            Map<String, ChildMapping> nestedMapping) {
        Map<String, Multimap<String, Map<String, Object>>> nestedMap = Maps.newHashMap();
        if (A.isNotEmpty(nestedMapping)) {
            for (Map.Entry<String, ChildMapping> entry : nestedMapping.entrySet()) {
                String key = entry.getKey();
                ChildMapping nested = entry.getValue();

                List<Map<String, Object>> list = nestedData.get(key);
                if (A.isNotEmpty(list)) {
                    String tableField = nested.getChildField();
                    Multimap<String, Map<String, Object>> multiMap = LinkedHashMultimap.create();
                    for (Map<String, Object> data : list) {
                        String fieldData = U.toStr(data.get(tableField));
                        if (U.isNotBlank(fieldData)) {
                            data.remove(tableField);
                            multiMap.put(fieldData, data);
                        }
                    }
                    nestedMap.put(key, multiMap);
                }
            }
        }
        return nestedMap;
    }

    private void fillRelation(Map<String, ChildMapping> relationMapping,
                              Map<String, Map<String, Map<String, Object>>> relationMap,
                              Map<String, Object> data) {
        if (A.isNotEmpty(relationMapping)) {
            for (Map.Entry<String, ChildMapping> entry : relationMapping.entrySet()) {
                String nestedKey = entry.getKey();
                if (data.containsKey(nestedKey)) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("one to one mapping({}) has already alias in primary sql, ignore put)", nestedKey);
                    }
                } else {
                    ChildMapping nestedValue = entry.getValue();
                    Map<String, Map<String, Object>> dataMap = relationMap.get(nestedKey);
                    if (A.isNotEmpty(dataMap)) {
                        Map<String, Object> map = dataMap.get(U.toStr(data.get(nestedValue.getMainField())));
                        if (A.isNotEmpty(map)) {
                            // data.putAll(map); // cover all fields
                            for (Map.Entry<String, Object> me : map.entrySet()) {
                                if (data.containsKey(me.getKey())) {
                                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                                        Logs.ROOT_LOG.warn("one to one mapping({}) field({}) has already alias in primary sql, ignore put)",
                                                nestedKey, me.getKey());
                                    }
                                } else {
                                    data.put(me.getKey(), me.getValue());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void fillNested(Map<String, ChildMapping> nestedMapping,
                            Map<String, Multimap<String, Map<String, Object>>> nestedMap,
                            Map<String, Object> data) {
        if (A.isNotEmpty(nestedMapping)) {
            for (Map.Entry<String, ChildMapping> entry : nestedMapping.entrySet()) {
                String nestedKey = entry.getKey();
                if (data.containsKey(nestedKey)) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("nested mapping({}) has already alias in primary sql, ignore put)", nestedKey);
                    }
                } else {
                    ChildMapping nestedValue = entry.getValue();
                    Multimap<String, Map<String, Object>> multimap = nestedMap.get(nestedKey);
                    if (U.isNotBlank(multimap) && multimap.size() > 0) {
                        Collection<Map<String, Object>> list = multimap.get(U.toStr(data.get(nestedValue.getMainField())));
                        if (A.isNotEmpty(list)) {
                            data.put(nestedKey, list);
                        }
                    }
                }
            }
        }
    }

    private Map<String, Object> handleData(Relation relation, Map<String, Object> data) {
        Map<String, Object> dataMap = Maps.newHashMap();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = relation.useField(entry.getKey());
            if (U.isNotBlank(key)) {
                Object value = entry.getValue();
                if (U.isNotBlank(value)) {
                    if (Sets.newHashSet("0000-00-00", "00:00:00", "0000-00-00 00:00:00").contains(value.toString())) {
                        dataMap.put(key, NIL_DATE_TIME);
                    } else {
                        dataMap.put(key, value);
                    }
                } else {
                    // field has suggest and null, can't be write => https://elasticsearch.cn/question/4051
                    // use    IFNULL(xxx, ' ')    in SQL
                    // dataMap.put(key, U.isBlank(value) ? "" : value);
                    dataMap.put(key, "");
                }
            }
        }
        return dataMap;
    }

    private void fillVersionOnSingle(Relation relation, Map<String, Object> data, Map<String, String> sourceMap) {
        String versionColumn = relation.getVersionColumn();
        if (U.isNotBlank(versionColumn)) {
            String version = U.toStr(data.get(versionColumn));
            if (U.isNumber(version) && U.greater0(Double.parseDouble(version))) {
                sourceMap.put("version", version);
            } else {
                Date datetime = Dates.parse(version);
                if (U.isNotNull(datetime)) {
                    sourceMap.put("version", U.toStr(datetime.getTime()));
                } else {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("versionColumn on the data is not a Number type, and not a Date type");
                    }
                }
            }
        }
    }

    private void fillRouteOnSingle(Relation relation, Map<String, Object> data, Map<String, String> sourceMap) {
        List<String> routeColumnList = relation.getRouteColumn();
        if (A.isNotEmpty(routeColumnList)) {
            List<String> routes = Lists.newArrayList();
            for (String route : routeColumnList) {
                Object obj = data.get(route);
                if (U.isNotBlank(obj)) {
                    routes.add(U.toStr(obj).trim());
                }
            }
            if (A.isEmpty(routes)) {
                if (Logs.ROOT_LOG.isWarnEnabled()) {
                    Logs.ROOT_LOG.warn("routeColumn has not on the data");
                }
            } else {
                sourceMap.put("routing", A.toStr(routes));
            }
        }
    }
}
