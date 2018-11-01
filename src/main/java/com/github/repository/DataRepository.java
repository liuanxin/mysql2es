package com.github.repository;

import com.github.model.Config;
import com.github.model.Document;
import com.github.model.Relation;
import com.github.model.Scheme;
import com.github.util.*;
import org.apache.commons.lang3.math.NumberUtils;
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

    @Autowired
    private Config config;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EsRepository esRepository;

    /** generate scheme of es on the database table structure */
    public List<Scheme> dbToEsScheme() {
        List<Scheme> schemeList = A.lists();
        for (Relation relation : config.getRelation()) {
            List<Map<String, Object>> mapList = jdbcTemplate.queryForList(relation.descSql());
            if (A.isNotEmpty(mapList)) {
                boolean scheme = relation.isScheme();

                List<String> keyList = A.lists();
                Map<String, Map> propertyMap = A.maps();
                for (Map<String, Object> map : mapList) {
                    Object column = map.get("Field");
                    Object type = map.get("Type");

                    if (U.isNotBlank(column) && U.isNotBlank(type)) {
                        if (scheme) {
                            propertyMap.put(relation.useField(column.toString()), Searchs.dbToEsType(type.toString()));
                        }

                        Object key = map.get("Key");
                        if (U.isNotBlank(key) && "PRI".equals(key)) {
                            keyList.add(column.toString());
                        }
                    }
                }

                U.assertException(A.isEmpty(keyList), String.format("table (%s) no primary key, " +
                        "can't create index in es!", relation.getTable()));
                if (keyList.size() > 1) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("table ({}) has multi primary key, " +
                                "increment data may be query for duplicate data, but will not miss !",
                                relation.getTable());
                    }
                }

                relation.setKeyList(keyList);
                if (scheme) {
                    schemeList.add(new Scheme().setIndex(relation.useType()).setProperties(propertyMap));
                }
            }
        }
        return schemeList;
    }

    /** async data to es */
    @Async
    public Future<Boolean> asyncData(Relation relation) {
        List<String> keyList = relation.getKeyList();
        if (A.isEmpty(keyList)) {
            dbToEsScheme();
            keyList = relation.getKeyList();
        }
        // must have primary key
        if (A.isNotEmpty(keyList)) {
            // read last id from temp file
            String index = relation.useType();
            String tmpColumnValue = Files.read(index);
            String countSql = relation.countSql(tmpColumnValue);
            Integer count = A.first(jdbcTemplate.queryForList(countSql, Integer.class));
            if (U.greater0(count)) {
                List<Document> documents = A.lists();
                List<Map<String, Object>> dataList = null;

                int loopCount = relation.loopCount(count);
                for (int i = 0; i < loopCount; i++) {
                    String pageSql = relation.querySql(i, tmpColumnValue);
                    dataList = jdbcTemplate.queryForList(pageSql);

                    documents.addAll(fixDocument(relation, keyList, dataList));
                    // batch insert
                    if (documents.size() >= config.getCount()) {
                        esRepository.saveDataToEs(documents);
                        documents.clear();

                        // write last id to temp file
                        String last = getLast(keyList, relation.getIncrementColumn(), dataList);
                        if (U.isNotBlank(last)) {
                            Files.write(index, last);
                        }
                    }
                }

                // save last data
                if (A.isNotEmpty(documents)) {
                    esRepository.saveDataToEs(documents);

                    if (A.isNotEmpty(documents)) {
                        // write last id to temp file
                        String last = getLast(keyList, relation.getIncrementColumn(), dataList);
                        if (U.isNotBlank(last)) {
                            Files.write(index, last);
                        }
                    }
                }
            }
        }
        return new AsyncResult<>(true);
    }
    /** return last record */
    private String getLast(List<String> keyList, List<String> columnList, List<Map<String, Object>> dataList) {
        Map<String, Object> last = A.last(dataList);
        if (A.isNotEmpty(last)) {
            List<String> lastList = A.lists();
            for (String column : columnList) {
                Object obj = last.get(column);
                if (U.isNotBlank(obj)) {
                    // if was Date return 'yyyy-MM-dd HH:mm:ss', else return toStr
                    lastList.add(dataToStr(obj));
                }
            }
            String lastValue = A.toStr(lastList, U.FIRST_SPLIT);

            if (keyList.size() == 1 && columnList.size() == 1) {
                String increment = columnList.get(0);
                String key = keyList.get(0);

                List<Object> columnValueList = A.lists();
                for (Map<String, Object> data : dataList) {
                    String column = dataToStr(data.get(increment));
                    // append (last data)'s (increment data) equals's data to tmp file
                    if (lastValue.equals(column)) {
                        String tmp = dataToStr(data.get(key));
                        if (U.isNotBlank(tmp)) {
                            columnValueList.add(tmp);
                        }
                    }
                }
                if (A.isNotEmpty(columnValueList)) {
                    StringBuilder sbd = new StringBuilder(U.SECOND_SPLIT + "AND " + key);
                    // multi use NOT IN, single use !=
                    if (columnValueList.size() > 1) {
                        sbd.append(" NOT IN (");
                        for (Object obj : columnValueList) {
                            sbd.append(sqlData(obj)).append(",");
                        }
                        if (sbd.toString().endsWith(",")) {
                            sbd.delete(sbd.length() - 1, sbd.length());
                        }
                        sbd.append(")");
                    } else {
                        sbd.append(" != ").append(sqlData(columnValueList.get(0)));
                    }
                    lastValue += sbd.toString();
                }
            }
            return lastValue;
        }
        return U.EMPTY;
    }
    private String sqlData(Object obj) {
        return (NumberUtils.isCreatable(obj.toString())) ? obj.toString() : ("'" + obj + "'");
    }
    private String dataToStr(Object obj) {
        if (obj instanceof Date) {
            return Dates.format((Date) obj, Dates.Type.YYYY_MM_DD_HH_MM_SS);
        }
        return obj.toString();
    }
    /** traverse the Database Result and organize into es Document */
    private List<Document> fixDocument(Relation relation, List<String> keyList,
                                       List<Map<String, Object>> dataList) {
        List<Document> documents = A.lists();
        for (Map<String, Object> objMap : dataList) {
            StringBuilder sbd = new StringBuilder();
            for (String primary : keyList) {
                sbd.append(objMap.get(primary)).append("-");
            }
            if (sbd.toString().endsWith("-")) {
                sbd.delete(sbd.length() - 1, sbd.length());
            }
            String id = sbd.toString();
            // Document no id, can't be save
            if (U.isNotBlank(id)) {
                Map<String, Object> dataMap = A.newHashMap();
                for (Map.Entry<String, Object> entry : objMap.entrySet()) {
                    String key = relation.useField(entry.getKey());
                    if (U.isNotBlank(key)) {
                        dataMap.put(key, entry.getValue());
                    }
                }
                // Document no data, don't need to save? or update to nil?
                // if (A.isNotEmpty(dataMap)) {
                documents.add(new Document()
                        .setIndex(relation.useType())
                        .setId(id).setData(dataMap));
                // }
            }
        }
        return documents;
    }

    public void deleteTempFile() {
        for (Relation relation : config.getRelation()) {
            Files.delete(relation.useType());
        }
    }
}
