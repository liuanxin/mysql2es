package com.github.repository;

import com.github.model.Config;
import com.github.model.Relation;
import com.github.model.Scheme;
import com.github.util.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@Component
public class DataRepository {

    private final Config config;
    private final JdbcTemplate jdbcTemplate;
    private final EsRepository esRepository;

    @Autowired
    public DataRepository(Config config, JdbcTemplate jdbcTemplate, EsRepository esRepository) {
        this.config = config;
        this.jdbcTemplate = jdbcTemplate;
        this.esRepository = esRepository;
    }

    /** generate scheme of es on the database table structure */
    public List<Scheme> dbToEsScheme() {
        List<Scheme> schemeList = Lists.newArrayList();
        for (Relation relation : config.getRelation()) {
            List<Map<String, Object>> mapList = jdbcTemplate.queryForList(relation.descSql());
            if (A.isNotEmpty(mapList)) {
                boolean scheme = relation.isScheme();

                List<String> keyList = Lists.newArrayList();
                Map<String, Map> propertyMap = A.maps();
                Map<String, Boolean> fieldMap = A.maps();
                for (Map<String, Object> map : mapList) {
                    Object column = map.get("Field");
                    Object type = map.get("Type");

                    if (U.isNotBlank(column) && U.isNotBlank(type)) {
                        String field = column.toString();
                        fieldMap.put(field, true);
                        if (scheme) {
                            propertyMap.put(relation.useField(field), Searchs.dbToEsType(type.toString()));
                        }

                        Object key = map.get("Key");
                        if (U.isNotBlank(key) && "PRI".equals(key)) {
                            keyList.add(field);
                        }
                    }
                }

                List<String> keyColumn = relation.getKeyColumn();
                String table = relation.getTable();
                if (A.isEmpty(keyColumn)) {
                    U.assertException(A.isEmpty(keyList),
                            String.format("table (%s) no primary key, can't create index in es!", table));

                    if (keyList.size() > 1) {
                        if (Logs.ROOT_LOG.isWarnEnabled()) {
                            Logs.ROOT_LOG.warn("table ({}) has multi primary key, " +
                                    "increment data may be query for duplicate data!", table);
                        }
                    }
                    relation.setKeyColumn(keyList);
                } else {
                    for (String key : keyColumn) {
                        U.assertNil(fieldMap.get(key), String.format("table (%s) don't have column (%s)", table, key));
                    }
                }
                if (scheme) {
                    schemeList.add(new Scheme().setIndex(relation.useIndex()).setProperties(propertyMap));
                }
            }
        }
        return schemeList;
    }

    /** async data to es */
    @Async
    public Future<Boolean> asyncData(Relation relation) {
        if (A.isEmpty(relation.getKeyColumn())) {
            dbToEsScheme();
        }
        saveData(relation);
        return new AsyncResult<>(true);
    }
    private void saveData(Relation relation) {
        String index = relation.useIndex();
        String type = relation.getType();

        String tmpColumnValue = Files.read(index, type);
        // select count(*) from ... where increment > xxx
        String countSql = relation.countSql(tmpColumnValue);
        long start = System.currentTimeMillis();
        Integer count = A.first(jdbcTemplate.queryForList(countSql, Integer.class));
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("count sql({}) execute({}), return({})",
                    countSql, (System.currentTimeMillis() - start + "ms"), count);
        }
        if (U.less0(count)) {
            return;
        }

        for (;;) {
            // select ... from ... where increment > xxx order by increment limit 1000
            String sql = relation.querySql(tmpColumnValue);
            start = System.currentTimeMillis();
            List<Map<String, Object>> dataList = jdbcTemplate.queryForList(sql);
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("sql({}) execute({})", sql, (System.currentTimeMillis() - start + "ms"));
            }
            esRepository.saveDataToEs(index, type, fixDocument(relation, dataList));
            tmpColumnValue = getLast(relation, dataList);
            if (U.isBlank(tmpColumnValue)) {
                return;
            }

            // handle increment = xxx, If the time field is synchronized, and the same data in the same second is a lot of time
            // select count(*) from ... where increment = xxx limit 1000
            String equalsCountSql = relation.equalsCountSql(tmpColumnValue);
            start = System.currentTimeMillis();
            Integer equalsCount = A.first(jdbcTemplate.queryForList(equalsCountSql, Integer.class));
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("count equals sql({}) execute({}), return({})",
                        equalsCountSql, (System.currentTimeMillis() - start + "ms"), count);
            }

            if (U.greater0(equalsCount)) {
                int equalsLoopCount = relation.loopCount(equalsCount);
                for (int i = 0; i < equalsLoopCount; i++) {
                    // select ... from ... where increment = xxx limit   0,1000|1000,1000|2000,1000| ...
                    String equalsSql = relation.equalsQuerySql(tmpColumnValue, i);
                    start = System.currentTimeMillis();
                    List<Map<String, Object>> equalsColumnDataList = jdbcTemplate.queryForList(equalsSql);
                    if (Logs.ROOT_LOG.isInfoEnabled()) {
                        Logs.ROOT_LOG.info("equals sql({}) execute({})",
                                equalsSql, (System.currentTimeMillis() - start + "ms"));
                    }
                    esRepository.saveDataToEs(index, type, fixDocument(relation, equalsColumnDataList));
                }
            }
            // write last record in temp file
            Files.write(relation.getIndex(), relation.getType(), tmpColumnValue);

            // if sql: limit 1000, query data size 900, Can return
            if (dataList.size() < relation.getLimit()) {
                return;
            }
        }
    }
    /** write last record in temp file */
    private String getLast(Relation relation, List<Map<String, Object>> dataList) {
        Map<String, Object> last = A.last(dataList);
        if (A.isNotEmpty(last)) {
            Object obj = last.get(relation.getIncrementColumnAlias());
            if (U.isNotBlank(obj)) {
                // if was Date return 'yyyy-MM-dd HH:mm:ss', else return toStr
                String lastData;
                if (obj instanceof Date) {
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
    private Map<String, String> fixDocument(Relation relation, List<Map<String, Object>> dataList) {
        Map<String, String> documents = Maps.newHashMap();
        for (Map<String, Object> obj : dataList) {
            StringBuilder sbd = new StringBuilder();
            String idPrefix = relation.getIdPrefix();
            if (U.isNotBlank(idPrefix)) {
                sbd.append(idPrefix);
            }
            for (String primary : relation.getKeyColumn()) {
                sbd.append(obj.get(primary)).append("-");
            }
            if (sbd.toString().endsWith("-")) {
                sbd.delete(sbd.length() - 1, sbd.length());
            }
            String idSuffix = relation.getIdSuffix();
            if (U.isNotBlank(idSuffix)) {
                sbd.append(idSuffix);
            }
            String id = sbd.toString();
            // Document no id, can't be save
            if (U.isNotBlank(id)) {
                Map<String, Object> dataMap = Maps.newHashMap();
                for (Map.Entry<String, Object> entry : obj.entrySet()) {
                    String key = relation.useField(entry.getKey());
                    if (U.isNotBlank(key)) {
                        Object value = entry.getValue();
                        // field has suggest and null, can't be write => https://elasticsearch.cn/question/4051
                        dataMap.put(key, U.isBlank(value) ? " " : value);
                        // dataMap.put(key, value);
                    }
                }
                // Document no data, don't need to save? or update to nil?
                if (A.isNotEmpty(dataMap)) {
                    documents.put(id, Jsons.toJson(dataMap));
                }
            }
        }
        return documents;
    }

    public void deleteTempFile() {
        for (Relation relation : config.getRelation()) {
            Files.delete(relation.useIndex(), relation.getType());
        }
    }
}
