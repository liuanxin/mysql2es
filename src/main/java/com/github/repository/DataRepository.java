package com.github.repository;

import com.github.model.NestedMapping;
import com.github.model.Relation;
import com.github.util.*;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Future;

@Component
@SuppressWarnings({ "rawtypes", "DuplicatedCode" })
public class DataRepository {

    private final JdbcTemplate jdbcTemplate;
    private final EsRepository esRepository;
    public DataRepository(JdbcTemplate jdbcTemplate, EsRepository esRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.esRepository = esRepository;
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
                Logs.ROOT_LOG.info("sql({}) return({}), use `{}` to check basic info", sql, A.toStr(matchTables), table);
            }
        } else {
            table = relationTable;
        }

        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(relation.descSql(table));
        if (A.isNotEmpty(mapList)) {
            boolean scheme = relation.isScheme();
            List<String> keyList = Lists.newArrayList();
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
                        keyList.add(field);
                    }
                    if (scheme) {
                        propertyMap.put(relation.useField(field), Searchs.dbToEsType(type.toString()));
                    }
                }
            }

            List<String> keyColumn = relation.getKeyColumn();
            if (A.isEmpty(keyColumn)) {
                if (A.isEmpty(keyList)) {
                    U.assertException(String.format("table (%s) no primary key, can't create index in es!", table));
                }
                if (keyList.size() > 1) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("table ({}) has multi primary key({})", table, A.toStr(keyList));
                    }
                }
                relation.setKeyColumn(keyList);
            } else {
                for (String key : keyColumn) {
                    U.assertNil(fieldMap.get(key), String.format("table (%s) don't have column (%s)", table, key));
                }
            }
            return propertyMap;
        }
        return Collections.emptyMap();
    }

    /** async data to es */
    @Async
    public Future<Boolean> asyncData(Relation relation) {
        if (A.isEmpty(relation.getKeyColumn())) {
            dbToEsScheme(relation);
        }
        saveData(relation);
        return new AsyncResult<>(true);
    }
    private void saveData(Relation relation) {
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
                            sql, sqlTime, A.toStr(matchTables), matchTables.size());
                }
            } else {
                matchTables = Collections.singletonList(table);
            }
            for (String matchTable : matchTables) {
                saveSingleTable(relation, index, matchTable);
            }
        }
    }

    private void saveSingleTable(Relation relation, String index, String matchTable) {
        String lastValue = F.read(matchTable, index);
        String matchInId = relation.matchInfo(matchTable);
        for (;;) {
            lastValue = handleGreaterAndEquals(relation, matchTable, lastValue, matchInId);
            if (U.isBlank(lastValue)) {
                return;
            }
        }
    }

    private String handleGreaterAndEquals(Relation relation, String matchTable, String lastValue, String matchInId) {
        String sql = relation.querySql(matchTable, lastValue);
        long sqlStart = System.currentTimeMillis();
        List<Map<String, Object>> dataList = jdbcTemplate.queryForList(sql);
        if (A.isEmpty(dataList)) {
            // if not data, can break loop
            return null;
        }
        long sqlTime = (System.currentTimeMillis() - sqlStart);
        if (Logs.ROOT_LOG.isDebugEnabled()) {
            Logs.ROOT_LOG.debug("sql({}) time({}ms) return size({})", sql, sqlTime, dataList.size());
        }

        Map<String, List<Map<String, Object>>> nestedData = nestedData(relation, dataList);
        long allSqlTime = (System.currentTimeMillis() - sqlStart);

        long esStart = System.currentTimeMillis();
        String index = relation.useIndex();
        int size = esRepository.saveDataToEs(index, fixDocument(relation, dataList, matchInId, nestedData));
        long esTime = (System.currentTimeMillis() - esStart);
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("sql time({}ms) size({}) batch to({}) time({}ms) success({})",
                    allSqlTime, dataList.size(), index, esTime, size);
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
        // write last record in temp file
        F.write(matchTable, index, lastValue);
        handleEquals(relation, matchTable, lastValue, matchInId);

        // if sql: limit 1000, query data size 900, can break loop
        if (dataList.size() < relation.getLimit()) {
            return null;
        }
        return lastValue;
    }
    private void handleEquals(Relation relation, String matchTable, String tempColumnValue, String matchInId) {
        // if was number: id > 123, don't need to id = 123
        // not number: time > '2010-10-10 00:00:01', this: time = '2010-10-10 00:00:01'
        if (U.isNotNumber(tempColumnValue)) {
            String equalsCountSql = relation.equalsCountSql(matchTable, tempColumnValue);
            long start = System.currentTimeMillis();
            Integer equalsCount = A.first(jdbcTemplate.queryForList(equalsCountSql, Integer.class));
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("equals count sql({}) time({}ms) return({})",
                        equalsCountSql, (System.currentTimeMillis() - start), equalsCount);
            }

            if (U.greater0(equalsCount)) {
                String index = relation.useIndex();
                int equalsLoopCount = relation.loopCount(equalsCount);
                for (int i = 0; i < equalsLoopCount; i++) {
                    String equalsSql = relation.equalsQuerySql(matchTable, tempColumnValue, i);
                    long sqlStart = System.currentTimeMillis();
                    List<Map<String, Object>> equalsDataList = jdbcTemplate.queryForList(equalsSql);
                    if (A.isEmpty(equalsDataList)) {
                        // if not data, can break equals handle
                        return;
                    }
                    long sqlTime = (System.currentTimeMillis() - sqlStart);
                    if (Logs.ROOT_LOG.isDebugEnabled()) {
                        Logs.ROOT_LOG.debug("equals sql({}) time({}ms) return size({})",
                                equalsSql, sqlTime, equalsDataList.size());
                    }

                    Map<String, List<Map<String, Object>>> nestedData = nestedData(relation, equalsDataList);
                    long allSqlTime = (System.currentTimeMillis() - sqlStart);

                    long esStart = System.currentTimeMillis();
                    int size = esRepository.saveDataToEs(index, fixDocument(relation, equalsDataList, matchInId, nestedData));
                    long esTime = (System.currentTimeMillis() - esStart);
                    if (Logs.ROOT_LOG.isInfoEnabled()) {
                        Logs.ROOT_LOG.info("equals sql time({}ms) size({}) batch to({}) time({}ms) success({})",
                                allSqlTime, equalsDataList.size(), index, esTime, size);
                    }

                    if (size == 0) {
                        // if success was 0, can break equals handle
                        return;
                    }

                    // if sql: limit 1000, 1000, query data size 900, can break equals handle
                    if (equalsDataList.size() < relation.getLimit()) {
                        return;
                    }
                }
            }
        }
    }

    private Map<String, List<Map<String, Object>>> nestedData(Relation relation, List<Map<String, Object>> dataList) {
        if (A.isEmpty(dataList)) {
            return Collections.emptyMap();
        }

        Map<String, List<Map<String, Object>>> returnMap = Maps.newHashMap();
        if (A.isNotEmpty(relation.getNestedMapping())) {
            for (Map.Entry<String, NestedMapping> entry : relation.getNestedMapping().entrySet()) {
                String key = entry.getKey();
                NestedMapping nested = entry.getValue();

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
                            Logs.ROOT_LOG.debug("nested({}) sql({}) time({}ms) return size({})",
                                    key, sql, (System.currentTimeMillis() - start), nestedDataList.size());
                        }
                        returnMap.put(key, nestedDataList);
                    }
                }
            }
        }
        return returnMap;
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
    private Map<String, Map<String, String>> fixDocument(Relation relation, List<Map<String, Object>> dataList,
                                                         String matchInId, Map<String, List<Map<String, Object>>> nestedData) {
        Map<String, Multimap<String, Map<String, Object>>> nestedMap = Maps.newHashMap();
        if (A.isNotEmpty(relation.getNestedMapping())) {
            for (Map.Entry<String, NestedMapping> entry : relation.getNestedMapping().entrySet()) {
                String key = entry.getKey();
                NestedMapping nested = entry.getValue();

                List<Map<String, Object>> list = nestedData.get(key);
                if (A.isNotEmpty(list)) {
                    String tableField = nested.getNestedField();
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
                idBuild.append(idPrefix);
            }
            if (relation.isPatternToId() && U.isNotBlank(matchInId)) {
                if (idBuild.length() > 0) {
                    idBuild.append("-");
                }
                idBuild.append(matchInId);
            }
            for (String column : relation.getKeyColumn()) {
                if (idBuild.length() > 0) {
                    idBuild.append("-");
                }
                idBuild.append(data.get(column));
            }
            String idSuffix = relation.getIdSuffix();
            if (U.isNotBlank(idSuffix)) {
                if (idBuild.length() > 0) {
                    idBuild.append("-");
                }
                idBuild.append(idSuffix);
            }
            String id = idBuild.toString();

            // Document no id, can't be save
            if (U.isNotBlank(id)) {
                if (A.isNotEmpty(relation.getNestedMapping())) {
                    for (Map.Entry<String, NestedMapping> entry : relation.getNestedMapping().entrySet()) {
                        String nestedKey = entry.getKey();
                        if (data.containsKey(nestedKey)) {
                            if (Logs.ROOT_LOG.isWarnEnabled()) {
                                Logs.ROOT_LOG.warn("nested({}) has already alias in primary sql, ignore put)", nestedKey);
                            }
                        } else {
                            NestedMapping nestedValue = entry.getValue();
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
                        // field has suggest and null, can't be write => https://elasticsearch.cn/question/4051
                        dataMap.put(key, U.isBlank(value) ? " " : value);
                        // dataMap.put(key, value);
                    }
                }

                Map<String, String> sourceMap = Maps.newHashMap();
                // Document no data, don't need to save? or update to nil?
                if (A.isNotEmpty(dataMap)) {
                    sourceMap.put("data", Jsons.toJson(dataMap));

                    List<String> routes = Lists.newArrayList();
                    for (String route : relation.getRouteKey()) {
                        Object obj = data.get(route);
                        if (U.isNotBlank(obj)) {
                            routes.add(U.toStr(obj).trim());
                        }
                    }
                    if (A.isNotEmpty(routes)) {
                        sourceMap.put("routing", A.toStr(routes));
                    }
                }
                documents.put(id, sourceMap);
            }
        }
        return documents;
    }
}
