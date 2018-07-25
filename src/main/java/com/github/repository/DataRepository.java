package com.github.repository;

import com.github.model.Config;
import com.github.model.Document;
import com.github.model.Relation;
import com.github.model.Scheme;
import com.github.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Component
public class DataRepository {

    @Autowired
    private Config config;

    @Autowired
    private JdbcTemplate jdbcTemplate;

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
                            propertyMap.put(relation.useField(column.toString()), dbToEsType(type.toString()));
                        }

                        Object key = map.get("Key");
                        if (U.isNotBlank(key) && "PRI".equals(key)) {
                            keyList.add(column.toString());
                        }
                    }
                }
                if (A.isEmpty(keyList)) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("table ({}) no primary key, can't create index in es!", relation.getTable());
                    }
                } else {
                    relation.setKeyList(keyList);
                    if (scheme) {
                        schemeList.add(new Scheme().setIndex(relation.useType()).setProperties(propertyMap));
                    }
                }
            }
        }
        return schemeList;
    }
    private static Map<String, Map> dbToEsType(String fieldType) {
        fieldType = fieldType.toLowerCase();

        // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
        if ("tinyint(1)".equals(fieldType)) {
            return A.maps("type", "boolean");
        }
        else if (fieldType.contains("bigint")) {
            return A.maps("type", "long");
        }
        else if (fieldType.contains("int")) {
            return A.maps("type", "integer");
        }
        else if (fieldType.contains("date") || fieldType.contains("time")) {
            return A.maps("type", "date");
        }
        /*
        else if () {
            return A.maps("type", "nested");
        }
        else if () {
            return A.maps("type", "object");
        }
        */
        else {
            // if use ik, global set like next line, scheme mapping set with text, ik config don't need
            /*
            curl -XPOST http://ip:port/index/fulltext/_mapping -H 'Content-Type:application/json' -d '
                {
                  "properties": {
                    "content": {
                      "type": "text",
                      "analyzer": "ik_max_word",
                      "search_analyzer": "ik_max_word"
                    }
                  }
                }'
            */
            // return A.maps("type", "text", "analyzer", "ik_max_word", "search_analyzer", "ik_max_word");
            return A.maps("type", "text");
        }
    }

    /** increment data: read temp file -> query data -> write last record in temp file */
    public List<Document> incrementData() {
        List<Document> documents = A.lists();
        for (Relation relation : config.getRelation()) {
            List<String> keyList = relation.getKeyList();
            if (A.isEmpty(keyList)) {
                dbToEsScheme();
                keyList = relation.getKeyList();
            }
            // must have primary key
            if (A.isNotEmpty(keyList)) {
                // read last id from temp file
                String tmpColumnValue = Files.read(config.getIndex(), relation.useType());
                String countSql = relation.countSql(tmpColumnValue);
                Integer count = A.first(jdbcTemplate.queryForList(countSql, Integer.class));
                if (U.greater0(count)) {
                    int loopCount = relation.loopCount(count);
                    List<Map<String, Object>> dataList = new ArrayList<>();
                    for (int i = 0; i < loopCount; i++) {
                        // Total number of single operations can't exceed set value
                        if (dataList.size() < config.getCount()) {
                            String pageSql = relation.querySql(i, tmpColumnValue);
                            dataList.addAll(jdbcTemplate.queryForList(pageSql));
                        }
                    }
                    if (A.isNotEmpty(dataList)) {
                        // write last id to temp file
                        String last = getLast(relation.getIncrementColumn(), dataList);
                        if (U.isNotBlank(last)) {
                            Files.write(config.getIndex(), relation.useType(), last);
                        }
                        documents.addAll(fixDocument(relation, keyList, dataList));
                    }
                }
            }
        }
        return documents;
    }
    /** return last record */
    private String getLast(List<String> incrementColumnList, List<Map<String, Object>> dataList) {
        Map<String, Object> last = A.last(dataList);
        if (A.isNotEmpty(last)) {
            List<String> lastList = A.lists();
            for (String column : incrementColumnList) {
                Object obj = last.get(column);
                if (U.isNotBlank(obj)) {
                    // if was Date return timeMillis, else return toStr
                    lastList.add((obj instanceof Date) ? Dates.format((Date) obj, Dates.Type.YYYY_MM_DD_HH_MM_SS) : obj.toString());
                }
            }
            return A.toStr(lastList);
        }
        return U.EMPTY;
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
                        // index use old type, type use default: _doc
                        // .setIndex(config.getIndex()).setType(relation.useType())
                        .setId(id).setData(dataMap));
                // }
            }
        }
        return documents;
    }

    public void deleteTempFile() {
        for (Relation relation : config.getRelation()) {
            Files.delete(config.getIndex(), relation.useType());
        }
    }
}
