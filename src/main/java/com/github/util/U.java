package com.github.util;

import com.google.common.base.CaseFormat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.regex.Pattern;

/** util */
public final class U {

    public static final String EMPTY = "";
    public static final String SPLIT = "!~!";
    public static final Random RANDOM = new Random();
    public static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final Pattern BLANK_REGEX = Pattern.compile("\\s{2,}");


    public static String replaceBlank(String str) {
        return BLANK_REGEX.matcher(str).replaceAll(" ");
    }


    public static String random(int length) {
        if (length <= 0) {
            return EMPTY;
        }

        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sbd.append(RANDOM.nextInt(10));
        }
        return sbd.toString();
    }

    public static String columnToField(String column) {
        return CaseFormat.UPPER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL)
                .convert(column.toUpperCase().startsWith("C_") ? column.toUpperCase().substring(2) : column);
    }

    // Invalid index name [abcXyz], must be lowercase ==> convert to : abc-xyz

    public static String tableToType(String table) {
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
    public static int toInt(Object obj) {
        if (isBlank(obj)) {
            return 0;
        }
        try {
            return Integer.parseInt(obj.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    public static long toLong(Object obj) {
        if (isBlank(obj)) {
            return 0L;
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
    public static boolean greater0(Number obj) {
        return obj != null && obj.doubleValue() > 0;
    }
    public static boolean less0(Number obj) {
        return obj == null || obj.doubleValue() <= 0;
    }


    public static boolean isBlank(Object obj) {
        return obj == null || obj.toString().trim().length() == 0 || "null".equalsIgnoreCase(obj.toString().trim());
    }
    public static boolean isNotBlank(Object obj) {
        return !isBlank(obj);
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
