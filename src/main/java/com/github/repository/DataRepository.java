package com.github.repository;

import com.github.model.Document;
import com.github.model.Config;
import com.github.model.Relation;
import com.github.model.Scheme;
import com.github.util.A;
import com.github.util.Files;
import com.github.util.Logs;
import com.github.util.U;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

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
                        schemeList.add(new Scheme(config.getIndex(), relation.useType(), propertyMap));
                    }
                }
            }
        }
        return schemeList;
    }
    private static Map<String, Map> dbToEsType(String fieldType) {
        fieldType = fieldType.toLowerCase();

        if ("tinyint(1)".equals(fieldType)) {
            return A.maps("type", "boolean");
        } else if (fieldType.contains("int") || fieldType.contains("date") || fieldType.contains("time")) {
            return A.maps("type", "long");
        } else {
            // if use ik, global set like next line, scheme mapping set with text, ik config don't need
            /*
            curl -XPOST http://ip:port/index/fulltext/_mapping -d '
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
            return A.maps("type", "text");//, "analyzer", "ik_max_word", "search_analyzer", "ik_max_word");
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
                String sql = relation.querySql(Files.read(config.getIndex(), relation.useType()));
                List<Map<String, Object>> dataList = jdbcTemplate.queryForList(sql);
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
                    lastList.add((obj instanceof Date) ? String.valueOf(((Date) obj).getTime()) : obj.toString());
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
            StringBuilder id = new StringBuilder();
            for (String primary : keyList) {
                id.append(objMap.get(primary));
            }
            // Document no id, can't be save
            if (U.isNotBlank(id.toString())) {
                Map<String, Object> dataMap = A.newHashMap();
                for (Map.Entry<String, Object> entry : objMap.entrySet()) {
                    String key = relation.useField(entry.getKey());
                    if (U.isNotBlank(key)) {
                        dataMap.put(key, entry.getValue());
                    }
                }
                // Document no data, don't need to save? or update to nil?
                // if (A.isNotEmpty(dataMap)) {
                documents.add(new Document().setIndex(config.getIndex())
                        .setType(relation.useType()).setId(id.toString()).setData(dataMap));
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
