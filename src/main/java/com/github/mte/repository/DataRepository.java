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

    /** 将相关表的结构同步至 es */
    public List<Scheme> dbToEsScheme() {
        List<Scheme> schemeList = A.lists();
        for (Config.Relation relation : config.getRelation()) {
            List<Map<String, Object>> mapList = jdbcTemplate.queryForList(relation.descSql());
            if (A.isNotEmpty(mapList)) {
                List<String> keyList = A.lists();
                Map<String, Map> propertyMap = A.maps();
                for (Map<String, Object> map : mapList) {
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
                if (A.isEmpty(keyList)) {
                    if (Logs.ROOT_LOG.isWarnEnabled())
                        Logs.ROOT_LOG.warn("table ({}) no primary key, can't create index in es!", relation.getTable());
                } else {
                    relation.setKeyList(keyList);
                    schemeList.add(new Scheme(config.getIndex(), relation.useType(), propertyMap));
                }
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

    /** 增量数据, 从临时文件中取值并查询固定数量的数据, 将最后一条记录的值写回临时文件 */
    public List<Document> incrementData() {
        List<Document> documents = A.lists();
        for (Config.Relation relation : config.getRelation()) {
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
    /** 返回最后一条记录 */
    private String getLast(List<String> incrementColumnList, List<Map<String, Object>> dataList) {
        Map<String, Object> last = A.last(dataList);
        if (A.isNotEmpty(last)) {
            List<String> lastList = A.lists();
            for (String column : incrementColumnList) {
                Object obj = last.get(column);
                if (U.isNotBlank(obj)) {
                    // 如果是时间类型则返回时间戳, 否则返回其 toString
                    lastList.add((obj instanceof Date) ? String.valueOf(((Date) obj).getTime()) : obj.toString());
                }
            }
            return A.toStr(lastList);
        }
        return U.EMPTY;
    }
    /** 遍历结果集并整理成 es 相关的数据集 */
    private List<Document> fixDocument(Config.Relation relation, List<String> keyList,
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
        for (Config.Relation relation : config.getRelation()) {
            Files.delete(config.getIndex(), relation.useType());
        }
    }
}
