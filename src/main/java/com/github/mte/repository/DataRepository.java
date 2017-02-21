package com.github.mte.repository;

import com.github.mte.model.Config;
import com.github.mte.model.Document;
import com.github.mte.model.Scheme;
import com.github.mte.util.A;
import com.github.mte.util.Files;
import com.github.mte.util.Logs;
import com.github.mte.util.U;
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

    @Autowired
    private EsRepository esRepository;

    private void check() {
        U.assertNil(config, "no config with mysql and es");
        config.check();
    }

    public List<Scheme> dbToEsScheme() {
        check();

        List<Scheme> schemeList = A.lists();
        for (Config.Relation relation : config.getRelation()) {
            String table = relation.getTable();

            List<String> keyList = A.lists();
            Map<String, Map> propertyMap = A.maps();
            for (Map<String, Object> map : jdbcTemplate.queryForList("desc `" + table + "`")) {
                Object column = map.get("Field");
                Object type = map.get("Type");

                if (U.isNotBlank(column) && U.isNotBlank(type)) {
                    propertyMap.put(relation.useField(column.toString()), dbToEsType(type.toString().toLowerCase()));

                    Object key = map.get("Key");
                    if (U.isNotBlank(key) && "PRI".equals(key)) {
                        keyList.add(column.toString());
                    }
                }
            }
            relation.setKeyList(keyList);
            if (A.isEmpty(keyList)) {
                if (Logs.ROOT_LOG.isWarnEnabled())
                    Logs.ROOT_LOG.warn("table ({}) no primary key, can't create index in es!", table);
            } else {
                schemeList.add(new Scheme().setIndex(config.getIndex())
                        .setType(relation.useType()).setProperties(propertyMap));
            }
        }
        return schemeList;
    }
    private static Map<String, Map> dbToEsType(String fieldType) {
        if ("tinyint(1)".equals(fieldType)) {
            return A.maps("type", "boolean");
        } else if (fieldType.contains("int") || fieldType.contains("time")) {
            return A.maps("type", "long");
        } else {
            return A.maps("type", "text");//, "analyzer", "ik_max_word", "search_analyzer", "ik_max_word");
        }
    }

    public void syncData(String last) {
        check();
        for (Config.Relation relation : config.getRelation()) {
            List<String> keyList = relation.getKeyList();
            if (A.isEmpty(keyList)) {
                // 把 mysql 的表结构同步到 es
                dbToEsScheme();
                keyList = relation.getKeyList();
            }
            // must have primary key
            if (A.isNotEmpty(keyList)) {
                syncDbToEs(last, relation, keyList);
            }
        }
    }
    public List<Document> incrementData() {
        check();

        List<Document> documents = A.lists();
        for (Config.Relation relation : config.getRelation()) {
            List<String> keyList = relation.getKeyList();
            if (A.isEmpty(keyList)) {
                dbToEsScheme();
                keyList = relation.getKeyList();
            }
            // must have primary key
            if (A.isNotEmpty(keyList)) {
                syncDbToEsByTmpFile(documents, relation, keyList);
            }
        }
        return documents;
    }
    /** 把数据库数据同步到 es. 递归调用 */
    private void syncDbToEs(String last, Config.Relation relation, List<String> keyList) {
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(relation.querySql(last));
        if (A.isNotEmpty(mapList)) {
            List<Document> documents = A.lists();
            last = getDocuments(documents, relation, keyList, mapList);
            esRepository.saveDataToEs(documents);
            syncDbToEs(last, relation, keyList);
        }
    }
    private String getDocuments(List<Document> documents, Config.Relation relation,
                                List<String> keyList, List<Map<String, Object>> dataList) {
        String last = U.EMPTY;
        for (int i = 0; i < dataList.size(); i++) {
            Map<String, Object> objMap = dataList.get(i);

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
            if (i + 1 == dataList.size()) {
                List<String> lastList = A.lists();
                for (String column : relation.getIncrementColumn()) {
                    lastList.add(getIncrement(objMap.get(column)));
                }
                last = A.toStr(lastList);
            }
        }
        return last;
    }
    private void syncDbToEsByTmpFile(List<Document> documents, Config.Relation relation, List<String> keyList) {
        // read last id to temp file
        String sql = relation.querySql(Files.read(config.getIndex(), relation.useType()));

        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql);
        if (A.isNotEmpty(mapList)) {
            String last = getDocuments(documents, relation, keyList, mapList);
            // write last id in temp file
            if (U.isNotBlank(last)) {
                Files.write(config.getIndex(), relation.useType(), last);
            }
        }
    }


    /** if was time, return currentTimeMillis. else return string value */
    private static String getIncrement(Object obj) {
        if (U.isBlank(obj)) {
            return U.EMPTY;
        } else if (obj instanceof Date) {
            return String.valueOf(((Date) obj).getTime());
        } else {
            return obj.toString();
        }
    }

    public void deleteTempFile() {
        for (Config.Relation relation : config.getRelation()) {
            Files.delete(config.getIndex(), relation.useType());
        }
    }
}
