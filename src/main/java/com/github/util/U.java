package com.github.util;

import com.google.common.base.CaseFormat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/** util */
public final class U {

    public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

    public static final String EMPTY = "";
    public static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final Pattern BLANK_REGEX = Pattern.compile("\\s{2,}");


    public static String replaceBlank(String str) {
        return BLANK_REGEX.matcher(str).replaceAll(" ");
    }


    public static String columnToField(String column) {
        return CaseFormat.UPPER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL)
                .convert(column.toUpperCase().startsWith("C_") ? column.toUpperCase().substring(2) : column);
    }

    // Invalid index name [abcXyz], must be lowercase ==> convert to : abc-xyz

    public static String tableToIndex(String table) {
        return CaseFormat.UPPER_UNDERSCORE.converterTo(CaseFormat.LOWER_HYPHEN)
                .convert(table.toUpperCase().startsWith("T_") ? table.toUpperCase().substring(2) : table);
    }

    public static String addSuffix(String src) {
        if (isBlank(src)) {
            return "/";
        }
        if (src.endsWith("/")) {
            return src;
        }
        return src + "/";
    }

    public static boolean isNumber(Object obj) {
        if (isBlank(obj)) {
            return false;
        }
        try {
            Double.parseDouble(obj.toString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    public static boolean greater0(Number obj) {
        return obj != null && obj.doubleValue() > 0;
    }
    public static boolean less0(Number obj) {
        return obj == null || obj.doubleValue() <= 0;
    }

    public static int toInt(Object obj) {
        if (isBlank(obj)) {
            return 0;
        }
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        }
        try {
            return Integer.parseInt(obj.toString().trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    public static Long toLong(Object obj) {
        if (isBlank(obj)) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static boolean isNull(Object obj) {
        return obj == null;
    }
    public static boolean isNotNull(Object obj) {
        return !isNull(obj);
    }
    public static boolean isBlank(Object obj) {
        return isNull(obj) || obj.toString().trim().length() == 0 || "null".equalsIgnoreCase(obj.toString().trim());
    }
    public static boolean isNotBlank(Object obj) {
        return !isBlank(obj);
    }
    public static String toStr(Object obj) {
        return obj == null ? null : obj.toString();
    }

    public static void assertNil(Object obj, String msg) {
        assertException(isBlank(obj), msg);
    }
    public static void assert0(Number obj, String msg) {
        assertException(less0(obj), msg);
    }
    public static void assertException(Boolean flag, String msg) {
        if (flag != null && flag) {
            assertException(msg);
        }
    }
    public static void assertException(String msg) {
        throw new RuntimeException(msg);
    }
}
