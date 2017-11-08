package com.github.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

/** util with Date */
public class Dates {

    public enum Type {
        /** yyyy-MM-ddTHH:mm:ss.SSSZ */
        TZ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),

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

        /** HH:mm:ss */
        HH_MM_SS("HH:mm:ss"),
        /** HH:mm */
        HH_MM("HH:mm"),

        /** yyyyMMddHHmmssSSS */
        YYYYMMDDHHMMSSSSS("yyyyMMddHHmmssSSS"),
        /** yyyyMMddHHmmss */
        YYYYMMDDHHMMSS("yyyyMMddHHmmss"),
        /** yyyyMMddHHmm */
        YYYYMMDDHHMM("yyyyMMddHHmm"),
        /** yyyyMMdd */
        YYYYMMDD("yyyyMMdd"),
        /** yyyyMM */
        YYYYMM("yyyyMM");

        private String value;
        private Type(String value) {
            this.value = value;
        }
        public String getValue() {
            return value;
        }
    }


    public static Date now() {
        return new Date();
    }

    /** format date to string {@link Type} */
    public static String format(Date date, Type type) {
        if (U.isBlank(date) || U.isBlank(type)) {
            return U.EMPTY;
        }

        return getDateFormat(type).print(date.getTime());
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
            Date date = getDateFormat(type).parseDateTime(source).toDate();
            if (date != null) {
                return date;
            }
        }
        return null;
    }

    /** 2016-12-31 xx:yy:zz --> 2016-12-31 23:59:59 999 */
    public static Date startInDay(Date date) {
        if (U.isBlank(date)) {
            return null;
        }

        return new DateTime(date).hourOfDay().withMaximumValue()
                .minuteOfHour().withMaximumValue()
                .secondOfMinute().withMaximumValue()
                .millisOfSecond().withMaximumValue().toDate();
    }
    /** 2016-12-31 xx:yy:zz --> 2016-12-31 00:00:00 000 */
    public static Date endInDay(Date date) {
        if (U.isBlank(date)) {
            return null;
        }

        return new DateTime(date).hourOfDay().withMinimumValue()
                .minuteOfHour().withMinimumValue()
                .secondOfMinute().withMinimumValue()
                .millisOfSecond().withMinimumValue().toDate();
    }
}
