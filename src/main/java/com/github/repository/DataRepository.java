package com.github.repository;

import com.github.model.ChildMapping;
import com.github.model.IncrementStorageType;
import com.github.model.Relation;
import com.github.util.*;
import com.google.common.collect.*;
import lombok.AllArgsConstructor;
import org.elasticsearch.common.UUIDs;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

@Component
@AllArgsConstructor
@SuppressWarnings({ "rawtypes", "DuplicatedCode" })
public class DataRepository {

    private static final String EQUALS_SUFFIX = "<-_->";
    private static final String EQUALS_I_SPLIT = "<=_=>";
    private static final Date NIL_DATE_TIME = new Date(0L);

    /**
     * <pre>
     * CREATE TABLE IF NOT EXISTS `t_db_to_es` (
     *   `table_index` VARCHAR(64) NOT NULL COMMENT '表 + es index',
     *   `increment_value` VARCHAR(256) NOT NULL COMMENT '增量数据值',
     *   `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
     *   `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
     *   PRIMARY KEY (`table_index`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'db 到 es 的增量记录';
     * </pre>
     */
    private static final String GENERATE_TABLE =
            "CREATE TABLE IF NOT EXISTS `t_db_to_es` (" +
            "  `table_index` VARCHAR(64) NOT NULL," +
            "  `increment_value` VARCHAR(256) NOT NULL," +
            "  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            "  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
            "  PRIMARY KEY (`table_index`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    private static final String SELECT_COUNT = "SELECT COUNT(*) FROM `t_db_to_es` WHERE `table_index` = ?";
    /* 「replace into」 will cover create_time and update_time to now() */
    private static final String ADD_INCREMENT = "INSERT INTO `t_db_to_es`(`table_index`, `increment_value`) VALUES(?, ?)";
    private static final String UPDATE_INCREMENT = "UPDATE `t_db_to_es` SET `increment_value` = ? WHERE `table_index` = ?";
    private static final String GET_INCREMENT = "SELECT `increment_value` FROM `t_db_to_es` WHERE `table_index` = ?";


    private final JdbcTemplate jdbcTemplate;
    private final EsRepository esRepository;


    public void generateIncrementTable() {
        jdbcTemplate.execute(GENERATE_TABLE);
    }

    private String getLastValue(IncrementStorageType incrementType, String table, String incrementColumn, String index) {
        String tableColumn = getTableColumn(table, incrementColumn);
        if (U.isBlank(incrementType) || incrementType == IncrementStorageType.TEMP_FILE) {
            return F.read(tableColumn, index);
        } else if (incrementType == IncrementStorageType.MYSQL) {
            String name = F.fileNameOrTableKey(tableColumn, index);
            return A.first(jdbcTemplate.queryForList(GET_INCREMENT, String.class, name));
        } else {
            return null;
        }
    }
    private String getTableColumn(String table, String incrementColumn) {
        return table + "-" + incrementColumn;
    }
    private void saveLastValue(IncrementStorageType incrementType, String table, String incrementColumn, String index, String value) {
        String tableColumn = getTableColumn(table, incrementColumn);
        if (U.isBlank(incrementType) || incrementType == IncrementStorageType.TEMP_FILE) {
            F.write(tableColumn, index, value);
        } else if (incrementType == IncrementStorageType.MYSQL) {
            String name = F.fileNameOrTableKey(tableColumn, index);
            Integer count = A.first(jdbcTemplate.queryForList(SELECT_COUNT, Integer.class, name));
            if (U.greater0(count)) {
                jdbcTemplate.update(UPDATE_INCREMENT, value, name);
            } else {
                jdbcTemplate.update(ADD_INCREMENT, name, value);
            }
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
                for (String key : idColumn) {
                    U.assertNil(fieldMap.get(key), String.format("table (%s) don't have column (%s)", table, key));
                }
            }
            return propertyMap;
        }
        return Collections.emptyMap();
    }

    /** async data to es */
    @Async
    public Future<Long> asyncData(IncrementStorageType incrementType, Relation relation) {
        if (A.isEmpty(relation.getIdColumn())) {
            dbToEsScheme(relation);
        }

        long count = 0;
        String table = relation.getTable();
        String index = relation.useIndex();
        if (U.isNotBlank(table) && U.isNotBlank(index)) {
            List<String> matchTables;
            if (relation.checkMatch()) {
                long start = System.currentTimeMillis();
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

            AtomicLong increment = new AtomicLong();
            for (String matchTable : matchTables) {
                saveSingleTable(incrementType, relation, index, matchTable, increment);
            }
            count = increment.get();
        }
        return new AsyncResult<>(count);
    }

    private void saveSingleTable(IncrementStorageType incrementType, Relation relation,
                                 String index, String matchTable, AtomicLong increment) {
        String lastValue = getLastValue(incrementType, matchTable, relation.getIncrementColumn(), index);
        String matchInId = relation.matchInfo(matchTable);
        for (;;) {
            lastValue = handleGreaterAndEquals(incrementType, relation, matchTable, lastValue, matchInId, increment);
            if (U.isBlank(lastValue)) {
                return;
            }
        }
    }

    private String handleGreaterAndEquals(IncrementStorageType incrementType, Relation relation, String matchTable,
                                          String lastValue, String matchInId, AtomicLong increment) {
        if (U.isNotBlank(lastValue) && lastValue.endsWith(EQUALS_SUFFIX)) {
            lastValue = lastValue.substring(0, lastValue.length() - EQUALS_SUFFIX.length());
            handleEquals(incrementType, relation, matchTable, lastValue, matchInId, 0, increment);
            return lastValue;
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
            Logs.ROOT_LOG.debug("sql({}) time({}ms) return size({})", getSql(sql), sqlTime, dataList.size());
        }

        Map<String, List<Map<String, Object>>> relationData = childData(relation.getRelationMapping(), dataList);
        Map<String, List<Map<String, Object>>> nestedData = childData(relation.getNestedMapping(), dataList);
        long allSqlTime = (System.currentTimeMillis() - start);

        long esStart = System.currentTimeMillis();
        String index = relation.useIndex();
        int size = esRepository.saveDataToEs(index, fixDocument(relation, dataList, matchInId, relationData, nestedData));
        increment.addAndGet(size);
        long end = System.currentTimeMillis();
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("greater({}) sql time({}ms) size({}) batch to({}) time({}ms) success({}), all time({}ms)",
                    lastValue, allSqlTime, dataList.size(), index, (end - esStart), size, (end - start));
        }
        if (size == 0) {
            // if write to es false, can break loop
            return null;
        }

        lastValue = getLast(relation, dataList);
        if (U.isBlank(lastValue)) {
            // if last data was nil, can break loop
            return null;
        }
        handleEquals(incrementType, relation, matchTable, lastValue, matchInId, 0, increment);
        // write last record
        saveLastValue(incrementType, matchTable, relation.getIncrementColumn(), index, lastValue);

        // if sql: limit 1000, query data size 900, can break loop
        if (dataList.size() < relation.getLimit()) {
            return null;
        }
        return lastValue;
    }
    private void handleEquals(IncrementStorageType incrementType, Relation relation, String matchTable,
                              String tempColumnValue, String matchInId, int lastEqualsCount, AtomicLong increment) {
        String[] equalsValueArr = tempColumnValue.split(EQUALS_I_SPLIT);
        String equalsValue = equalsValueArr[0];
        long nowMs = System.currentTimeMillis();

        // pre: time > '2010-10-10 00:00:01' | 1286640001000, current: time = '2010-10-10 00:00:01' | 1286640001000
        String equalsCountSql = relation.equalsCountSql(matchTable, equalsValue);
        long start = System.currentTimeMillis();
        Integer equalsCount = A.first(jdbcTemplate.queryForList(equalsCountSql, Integer.class));
        if (Logs.ROOT_LOG.isDebugEnabled()) {
            Logs.ROOT_LOG.debug("equals count sql({}) time({}ms) return({})",
                    getSql(equalsCountSql), (System.currentTimeMillis() - start), equalsCount);
        }
        if (U.less0(equalsCount)) {
            currentSecondHandle(equalsValue, nowMs, incrementType, relation, matchTable, 0, matchInId, 0, increment);
            return;
        }
        if (lastEqualsCount > 0 && lastEqualsCount == equalsCount) {
            currentSecondHandle(equalsValue, nowMs, incrementType, relation, matchTable, 0, matchInId, equalsCount, increment);
            return;
        }

        String index = relation.useIndex();
        int equalsLoopCount = relation.loopCount(equalsCount);
        int i = 0;
        if (equalsValueArr.length == 2) {
            i = U.toInt(equalsValueArr[1]);
            // if count = 1000, limit = 10, save has 101
            if (i * relation.getLimit() > equalsCount) {
                currentSecondHandle(equalsValue, nowMs, incrementType, relation, matchTable, 0, matchInId, equalsCount, increment);
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
                currentSecondHandle(equalsValue, nowMs, incrementType, relation, matchTable, i, matchInId, equalsCount, increment);
                return;
            }
            long sqlTime = (System.currentTimeMillis() - sqlStart);
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("equals sql({}) time({}ms) return size({})",
                        getSql(equalsSql), sqlTime, equalsDataList.size());
            }

            Map<String, List<Map<String, Object>>> relationData = childData(relation.getRelationMapping(), equalsDataList);
            Map<String, List<Map<String, Object>>> nestedData = childData(relation.getNestedMapping(), equalsDataList);
            long allSqlTime = (System.currentTimeMillis() - sqlStart);

            long esStart = System.currentTimeMillis();
            int size = esRepository.saveDataToEs(index, fixDocument(relation, equalsDataList, matchInId, relationData, nestedData));
            increment.addAndGet(size);
            long end = System.currentTimeMillis();
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("equals({}-{}: {}) sql time({}ms) size({}) batch to({}) time({}ms) success({}), all time({}ms)",
                        equalsValue, i, equalsLoopCount, allSqlTime, equalsDataList.size(), index, (end - esStart), size, (end - sqlStart));
            }

            // if success was 0, can break equals handle
            // if sql: limit 1000, 1000, query data size 900, can break equals handle
            if ((size == 0) || (equalsDataList.size() < relation.getLimit())) {
                currentSecondHandle(equalsValue, nowMs, incrementType, relation, matchTable, i, matchInId, equalsCount, increment);
                return;
            } else {
                // write current equals record
                String valueToSave = equalsValue + EQUALS_I_SPLIT + i + EQUALS_SUFFIX;
                saveLastValue(incrementType, matchTable, relation.getIncrementColumn(), index, valueToSave);
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
                                     Relation relation, String matchTable, int i,
                                     String matchInId, int currentEqualsCount, AtomicLong increment) {
        Date equalsDate = Dates.parse(equalsValue);
        if (U.isNotBlank(equalsDate)) {
            long equalsMs = equalsDate.getTime();
            boolean needSleepToNextSecond = (nowMs > equalsMs) && (equalsMs / 1000 == nowMs / 1000);
            if (needSleepToNextSecond) {
                try {
                    long nextSecondMs = ((nowMs / 1000) + 1) * 1000;
                    Thread.sleep(nextSecondMs - nowMs);
                    handleEquals(incrementType, relation, matchTable, equalsValue + EQUALS_I_SPLIT + i, matchInId, currentEqualsCount, increment);
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

    private String getSql(String sql) {
        return U.isBlank(sql) ? U.EMPTY : U.toStr(sql, 200, 30);
    }
    /** write last record in temp file */
    private String getLast(Relation relation, List<Map<String, Object>> dataList) {
        Map<String, Object> last = A.last(dataList);
        if (A.isNotEmpty(last)) {
            String column = relation.getIncrementColumn();
            Object obj = last.get(column.contains(".") ? column.substring(column.indexOf(".") + 1) : column);
            if (U.isNotBlank(obj)) {
                // if was Date return 'yyyy-MM-dd HH:mm:ss', else return toStr
                String lastData;
                if (obj instanceof Date) {
                    // lastData = String.valueOf(((Date) obj).getTime());
                    lastData = Dates.format((Date) obj, Dates.Type.YYYY_MM_DD_HH_MM_SS);
                } else {
                    lastData = obj.toString();
                }
                return lastData;
            }
        }
        return null;
    }
    /** traverse the Database Result and organize into es Document */
    private Map<String, Map<String, String>> fixDocument(Relation relation, List<Map<String, Object>> dataList, String matchInId,
                                                         Map<String, List<Map<String, Object>>> relationData,
                                                         Map<String, List<Map<String, Object>>> nestedData) {
        Map<String, Map<String, Map<String, Object>>> relationMap = Maps.newHashMap();
        if (A.isNotEmpty(relation.getRelationMapping())) {
            for (Map.Entry<String, ChildMapping> entry : relation.getRelationMapping().entrySet()) {
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
        Map<String, Multimap<String, Map<String, Object>>> nestedMap = Maps.newHashMap();
        if (A.isNotEmpty(relation.getNestedMapping())) {
            for (Map.Entry<String, ChildMapping> entry : relation.getNestedMapping().entrySet()) {
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

        Map<String, Map<String, String>> documents = Maps.newHashMap();
        for (Map<String, Object> data : dataList) {
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
            for (String column : relation.getIdColumn()) {
                if (idBuild.length() > 0) {
                    idBuild.append("-");
                }
                String str = U.toStr(data.get(column));
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
            String id = idBuild.length() == 0 ? UUIDs.base64UUID() : idBuild.toString();

            if (A.isNotEmpty(relation.getRelationMapping())) {
                for (Map.Entry<String, ChildMapping> entry : relation.getRelationMapping().entrySet()) {
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

            if (A.isNotEmpty(relation.getNestedMapping())) {
                for (Map.Entry<String, ChildMapping> entry : relation.getNestedMapping().entrySet()) {
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

            // Document no data, don't need to save? or update to nil?
            if (A.isNotEmpty(dataMap)) {
                Map<String, String> sourceMap = Maps.newHashMap();
                sourceMap.put("data", Jsons.toJson(dataMap));

                if (A.isNotEmpty(relation.getRouteColumn())) {
                    List<String> routes = Lists.newArrayList();
                    for (String route : relation.getRouteColumn()) {
                        Object obj = data.get(route);
                        if (U.isNotBlank(obj)) {
                            routes.add(U.toStr(obj).trim());
                        }
                    }
                    if (A.isNotEmpty(routes)) {
                        sourceMap.put("routing", A.toStr(routes));
                    }
                }
                if (U.isNotBlank(relation.getVersionColumn())) {
                    String version = U.toStr(data.get(relation.getVersionColumn()));
                    if (U.isNumber(version) && U.greater0(Double.parseDouble(version))) {
                        sourceMap.put("version", version);
                    } else {
                        Date datetime = Dates.parse(version);
                        if (U.isNotBlank(datetime)) {
                            sourceMap.put("version", U.toStr(datetime.getTime()));
                        }
                    }
                }
                documents.put(id, sourceMap);
            }
        }
        if (documents.size() < dataList.size()) {
            if (Logs.ROOT_LOG.isWarnEnabled()) {
                Logs.ROOT_LOG.warn("data size({}) <--> es size({}), may be has duplicate id", dataList.size(), documents.size());
            }
        }
        return documents;
    }
}
