package com.github.util;

import com.github.annotation.NeedAnalysis;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/** Useful when generating certain search objects */
public final class Searchs {

    private static final String STRING_TYPE = "text";
    private static final String DATE_TYPE = "date";
    
    /** https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html */
    private static final Map<Class<?>, String> TYPE_MAPPING = A.maps(
            boolean.class, "boolean",
            Boolean.class, "boolean",
            boolean[].class, "boolean",
            Boolean[].class, "boolean",

            byte.class, "byte",
            Byte.class, "byte",
            byte[].class, "byte",
            Byte[].class, "byte",

            short.class, "short",
            Short.class, "short",
            short[].class, "short",
            Short[].class, "short",

            int.class, "integer",
            Integer.class, "integer",
            int[].class, "integer",
            Integer[].class, "integer",

            long.class, "long",
            Long.class, "long",
            long[].class, "long",
            Long[].class, "long",
            BigInteger.class, "long",
            BigInteger[].class, "long",

            float.class, "float",
            Float.class, "float",
            float[].class, "float",
            Float[].class, "float",

            double.class, "double",
            Double.class, "double",
            double[].class, "double",
            Double[].class, "double",
            BigDecimal.class, "double",
            BigDecimal[].class, "double",

            String.class, STRING_TYPE,
            String[].class, STRING_TYPE,

            Date.class, DATE_TYPE,
            Date[].class, DATE_TYPE
    );
    private static Map<String, Object> mappingType(String type, NeedAnalysis needAnalyzer) {
        Map<String, Object> map = A.maps("type", type);
        if (STRING_TYPE.equals(type) && U.isNotBlank(needAnalyzer)) {
            if (needAnalyzer.keyword()) {
                map.put("type", "keyword");
            }
            if (needAnalyzer.chinese()) {
                map.put("analyzer", "ik_synonym");
                map.put("search_analyzer", "ik_synonym_smart");
            }

            Map<Object, Object> fieldMap = A.maps();
            if (needAnalyzer.pinyin()) {
                fieldMap.put("pinyin", A.maps("type", STRING_TYPE, "analyzer", "simple_pinyin"));
                fieldMap.put("full_pinyin", A.maps("type", STRING_TYPE, "analyzer", "full_pinyin"));
            }
            if (needAnalyzer.suggest()) {
                fieldMap.put("suggest", A.maps(
                        "type", "completion",
                        "analyzer", "ik_synonym",
                        "search_analyzer", "ik_synonym_smart"
                ));
            }
            if (needAnalyzer.suggest() && needAnalyzer.pinyin()) {
                fieldMap.put("suggest_pinyin", A.maps("type", "completion", "analyzer", "simple_pinyin"));
                fieldMap.put("suggest_full_pinyin", A.maps("type", "completion", "analyzer", "full_pinyin"));
            }
            map.put("fields", fieldMap);
        }
        else if (DATE_TYPE.equals(type)) {
            map.put("format", "epoch_millis||yyyy-MM-dd HH:mm:ss SSS||yyyy-MM-dd||yyyy-MM-dd HH:mm:ss");
        }
        return map;
    }

    public static Map<String, Map> dbToEsType(String fieldType) {
        fieldType = fieldType.toLowerCase();

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
            return A.maps(
                    "type", "date",
                    "format", "epoch_millis||yyyy-MM-dd HH:mm:ss SSS||yyyy-MM-dd||yyyy-MM-dd HH:mm:ss"
            );
        }
        else if (fieldType.contains("float")) {
            return A.maps("type", "float");
        }
        else if (fieldType.contains("decimal") || fieldType.contains("double")) {
            return A.maps("type", "double");
        }
        else {
            // if use ik or pinyin or synonym etc... please customize the configuration, don't configure automatically
            /*
            Map fieldMap = A.maps(
                "pinyin", A.maps("type", "text", "analyzer", "pinyin_analysis"),
                "suggest", A.maps("type", "completion", "analyzer", "ik_synonym", "search_analyzer", "ik_synonym_smart"),
                "suggest_pinyin", A.maps("type", "completion", "analyzer", "pinyin_analysis")
            );
            return A.maps(
                "type", "text",
                "analyzer", "ik_synonym",
                "search_analyzer", "ik_synonym_smart",
                "fields", fieldMap
            );
            */
            return A.maps("type", "text");
        }
    }

    /**
     * install ik and pinyin, use synonym
     *
     * elasticsearch-plugin install https://github.com/medcl/elasticsearch-analysis-ik/releases/download/vXXX/elasticsearch-analysis-ik-XXX.zip
     * elasticsearch-plugin install https://github.com/medcl/elasticsearch-analysis-pinyin/releases/download/vXXX/elasticsearch-analysis-pinyin-XXX.zip
     *
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-tokenfilter.html
     */
    public static String getSettings() {
        return "{\n" +
                "  \"index\" : {\n" +
                "    \"analysis\" : {\n" +
                "      \"analyzer\" : {\n" +
                "        \"ik_synonym\" : {\n" +
                "          \"type\" : \"custom\",\n" +
                "          \"tokenizer\" : \"ik_max_word\",\n" +
                "          \"filter\" : [\"synonym_filter\"]\n" +
                "        }\n" +
                "        , \"ik_synonym_smart\" : {\n" +
                "          \"type\" : \"custom\",\n" +
                "          \"tokenizer\" : \"ik_smart\",\n" +
                "          \"filter\" : [\"synonym_filter\"]\n" +
                "        }\n" +
                "        , \"simple_pinyin\": {\n" +
                "          \"tokenizer\" : \"whitespace\",\n" +
                "          \"filter\" : [\"lowercase\", \"simple_pinyin_filter\"]\n" +
                "        }" +
                "        , \"full_pinyin\": {\n" +
                "          \"tokenizer\" : \"whitespace\",\n" +
                "          \"filter\" : [\"lowercase\", \"full_pinyin_filter\"]\n" +
                "        }" +
                "      }\n" +
                "      , \"filter\" : {\n" +
                "        \"synonym_filter\" : {\n" +
                "          \"type\" : \"synonym\",\n" +
                "          \"synonyms_path\" : \"analysis/synonym.txt\"\n" +
                "        }\n" +
                // see https://github.com/medcl/elasticsearch-analysis-pinyin
                "        , \"simple_pinyin_filter\": {\n" +
                "          \"type\": \"pinyin\",\n" +
                "          \"keep_first_letter\": true,\n" +
                "          \"keep_full_pinyin\": false\n" +
                "        },\n" +
                "        \"full_pinyin_filter\": {\n" +
                "          \"type\": \"pinyin\",\n" +
                "          \"keep_first_letter\": false,\n" +
                "          \"keep_full_pinyin\": true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }

    /**
     * <pre>
     * createScheme(index, type, clazz) {
     *   if (!exists(indices, index)) {
     *     if (createIndex(indices, index)) {
     *       createScheme(indices, index, type, clazz);
     *     }
     *   }
     * }
     *   
     * private boolean exists(IndicesClient indices, String index) {
     *   try {
     *     return indices.exists(new GetIndexRequest().indices(index));
     *   } catch (IOException e) {
     *     if (LOG.isErrorEnabled()) {
     *       LOG.error(String.format("query index(%s) exists exception", index), e);
     *     }
     *     return true;
     *   }
     * }
     * private boolean createIndex(IndicesClient indices, String index) {
     *   CreateIndexRequest request = new CreateIndexRequest(index);
     *   String settings = Searchs.getSettings();
     *   request.settings(settings, XContentType.JSON);
     *   try {
     *     return indices.create(request).isAcknowledged();
     *   } catch (IOException e) {
     *     if (LOG.isErrorEnabled()) {
     *       LOG.error(String.format("create index(%s) exception", index), e);
     *     }
     *     return false;
     *   }
     * }
     * private void createScheme(IndicesClient indices, String index, String type, Class clazz) {
     *   PutMappingRequest request = new PutMappingRequest(index);
     *   String mapping = Searchs.getMapping(clazz);
     *   request.type(type).source(mapping, XContentType.JSON);
     *   try {
     *     PutMappingResponse response = indices.putMapping(request);
     *     if (LOG.isDebugEnabled()) {
     *       LOG.debug("put ({}/{}) mapping: {}", index, type, response.isAcknowledged());
     *     }
     *   } catch (IOException e) {
     *     if (LOG.isErrorEnabled()) {
     *       LOG.error(String.format("put (%s/%s) mapping exception", index, type), e);
     *     }
     *   }
     * }
     * </pre>
     */
    public static String getMapping(Class clazz) {
        return Jsons.toJson(A.maps("properties", collectScheme(clazz)));
    }

    private static Map<String, Map> collectScheme(Class clazz) {
        Map<String, Map> propertyMap = A.maps();
        for (Field field : clazz.getDeclaredFields()) {
            int mod = field.getModifiers();
            if (!Modifier.isStatic(mod) && !Modifier.isFinal(mod)) {
                Class<?> fieldType = field.getType();
                String fieldName = field.getName();
                NeedAnalysis needAnalyzer = field.getAnnotation(NeedAnalysis.class);

                String mapping = TYPE_MAPPING.get(fieldType);
                if (U.isNotBlank(mapping)) {
                    propertyMap.put(fieldName, mappingType(mapping, needAnalyzer));
                } else {
                    if (Map.class.isAssignableFrom(fieldType)) {
                        U.assertException(fieldName + " ==> please use Object or List to replace Map");
                    } else if (Collection.class.isAssignableFrom(fieldType)) {
                        Type type = field.getGenericType();
                        if (type instanceof ParameterizedType) {
                            Type args = ((ParameterizedType) type).getActualTypeArguments()[0];
                            if (args instanceof Class) {
                                mapping = TYPE_MAPPING.get(args);
                                if (U.isNotBlank(mapping)) {
                                    propertyMap.put(fieldName, mappingType(mapping, needAnalyzer));
                                } else {
                                    Map map = A.maps("type", "nested", "properties", collectScheme((Class) args));
                                    propertyMap.put(fieldName, map);
                                }
                            } else {
                                U.assertException("please set Generic in List<xxx>, don't use Generic like: List<T>");
                            }
                        }
                    } else if (fieldType.isArray()) {
                        String generic = field.toGenericString();
                        generic = generic.substring(0, generic.indexOf("[]"));
                        if (generic.contains(" ")) {
                            generic = generic.substring(generic.indexOf(" "), generic.length()).trim();
                        }
                        try {
                            Class<?> arrayType = Class.forName(generic);
                            Map map = A.maps("type", "nested", "properties", collectScheme(arrayType));
                            propertyMap.put(fieldName, map);
                        } catch (ClassNotFoundException e) {
                            U.assertException(String.format("class(%s) not found", generic));
                        }
                    } else {
                        Map map = A.maps("type", "nested", "properties", collectScheme(fieldType));
                        propertyMap.put(fieldName, map);
                    }
                }
            }
        }
        return propertyMap;
    }
}
