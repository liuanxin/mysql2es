package com.github.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

/** util with Json */
public class Jsons {

	private static final ObjectMapper BASIC = new BasicObjectMapper();
	private static class BasicObjectMapper extends ObjectMapper {
		private BasicObjectMapper() {
			super();
			configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
			configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		}
	}

	public static String toJson(Object obj) {
        try {
            return BASIC.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("object(" + obj + ") to json exception.", e);
        }
	}

	public static <T> T toObject(String json, Class<T> clazz) {
		try {
			return BASIC.readValue(json, clazz);
		} catch (Exception e) {
			throw new RuntimeException("json (" + json + ") to object(" + clazz.getName() + ") exception", e);
		}
	}
    public static <T> T toObjectNil(String json, Class<T> clazz) {
        try {
            return BASIC.readValue(json, clazz);
        } catch (Exception e) {
            return null;
        }
    }

    public static <T> List<T> toList(String json, Class<T> clazz) {
        try {
            return BASIC.readValue(json, BASIC.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            throw new RuntimeException("json(" + json + ") to list(" + clazz.getName() + ") exception.", e);
        }
    }
    public static <T> List<T> toListNil(String json, Class<T> clazz) {
        try {
            return BASIC.readValue(json, BASIC.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            return null;
        }
    }
}
