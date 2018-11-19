package com.github.util;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/** util with Date */
public class Dates {

    public enum Type {
        /** yyyy-MM-ddTHH:mm:ss.SSSZ */
        TSZ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        /** yyyy-MM-ddTHH:mm:ss.SSS */
        TS("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        /** yyyy-MM-ddTHH:mm:ssZ */
        TZ("yyyy-MM-dd'T'HH:mm:ss'Z'"),
        /** yyyy-MM-ddTHH:mm:ss */
        T("yyyy-MM-dd'T'HH:mm:ss"),

        /** yyyy-MM-dd HH:mm:ss SSS */
        YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss SSS"),
        /** yyyy-MM-dd HH:mm:ss */
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        /** yyyy-MM-dd HH:mm */
        YYYY_MM_DD_HH_MM("yyyy-MM-dd HH:mm"),
        /** yyyy-MM-dd */
        YYYY_MM_DD("yyyy-MM-dd"),
        /** yyyy-MM */
        YYYY_MM("yyyy-MM"),

        /** yyyyMMddHHmmssSSS */
        YYYYMMDDHHMMSSSSS("yyyyMMddHHmmssSSS"),
        /** yyyyMMddHHmmss */
        YYYYMMDDHHMMSS("yyyyMMddHHmmss"),
        /** yyyyMMddHHmm */
        YYYYMMDDHHMM("yyyyMMddHHmm"),
        /** yyyyMMdd */
        YYYYMMDD("yyyyMMdd"),
        /** yyMMdd */
        YYMMDD("yyMMdd"),
        /** yyyyMM */
        YYYYMM("yyyyMM"),

        /** HH:mm:ss */
        HH_MM_SS("HH:mm:ss"),
        /** HH:mm */
        HH_MM("HH:mm"),

        /** yyyy/MM/dd */
        USA_YYYY_MM_DD("yyyy/MM/dd"),
        /** MM/dd/yyyy HH:mm:ss */
        USA_MM_DD_YYYY_HH_MM_SS("MM/dd/yyyy HH:mm:ss"),

        CST("EEE MMM dd HH:mm:ss zzz yyyy");

        private String value;
        Type(String value) {
            this.value = value;
        }
        public String getValue() {
            return value;
        }

        public boolean isCst() {
            return this == CST;
        }
    }

    public static Date now() {
        return new Date();
    }

    /** format date to string {@link Type} */
    public static String format(Date date, Type type) {
        if (U.isBlank(date) || U.isBlank(type)) {
            return U.EMPTY;
        } else {
            return getDateFormat(type).print(date.getTime());
        }
    }

    public static String format(Date date, String type) {
        if (U.isBlank(date) || U.isBlank(type)) {
            return U.EMPTY;
        } else {
            return DateTimeFormat.forPattern(type).print(date.getTime());
        }
    }

    private static DateTimeFormatter getDateFormat(Type type) {
        return DateTimeFormat.forPattern(type.getValue());
    }

    /** format string to date, type with every one in {@link Type} */
    public static Date parse(String source) {
        if (U.isBlank(source)) {
            return null;
        }

        source = source.trim();
        for (Type type : Type.values()) {
            if (type.isCst()) {
                try {
                    // cst 单独处理
                    return new SimpleDateFormat(type.getValue(), Locale.ENGLISH).parse(source);
                } catch (ParseException | IllegalArgumentException e) {
                    // ignore
                }
            } else {
                Date date = getDateFormat(type).parseDateTime(source).toDate();
                if (date != null) {
                    return date;
                }
            }
        }
        return null;
    }
}
