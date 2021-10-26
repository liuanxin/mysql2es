package com.github.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/** util with Date */
@SuppressWarnings("unused")
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

        /** yyyy-MM-dd HH:mm:ss.SSS */
        YYYY_MM_DD_HH_MM_SSSSS("yyyy-MM-dd HH:mm:ss.SSS"),
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

        private final String value;
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

    private static final long SECOND = 1000L;
    private static final long MINUTE = 60 * SECOND;
    private static final long HOUR = 60 * MINUTE;
    private static final long DAY = 24 * HOUR;
    private static final long YEAR = 365 * DAY;

    public static Date now() {
        return new Date();
    }

    /** format date to string {@link Type} */
    public static String format(Date date, Type type) {
        if (U.isNull(date) || U.isNull(type)) {
            return U.EMPTY;
        } else {
            return getDateFormat(type).print(date.getTime());
        }
    }

    public static String format(Date date, String type) {
        if (U.isNull(date) || U.isNull(type)) {
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
                    // cst
                    return new SimpleDateFormat(type.getValue(), Locale.ENGLISH).parse(source);
                } catch (ParseException | IllegalArgumentException ignore) {
                }
            } else {
                try {
                    Date date = getDateFormat(type).parseDateTime(source).toDate();
                    if (date != null) {
                        return date;
                    }
                } catch (Exception ignore) {
                }
            }
        }
        return null;
    }

    public static Date addSecond(Date date, int minute) {
        return new DateTime(date).plusSeconds(minute).toDate();
    }

    public static String toHuman(long intervalMs) {
        if (intervalMs == 0) {
            return "0";
        }

        boolean flag = (intervalMs < 0);
        long ms = flag ? -intervalMs : intervalMs;

        long year = ms / YEAR;
        long y = ms % YEAR;

        long day = y / DAY;
        long d = y % DAY;

        long hour = d / HOUR;
        long h = d % HOUR;

        long minute = h / MINUTE;
        long mi = h % MINUTE;

        long second = mi / SECOND;
        long m = mi % SECOND;

        StringBuilder sbd = new StringBuilder();
        if (flag) {
            sbd.append("-");
        }
        if (year != 0) {
            sbd.append(year).append(" year ");
        }
        if (day != 0) {
            sbd.append(day).append(" day ");
        }
        if (hour != 0) {
            sbd.append(hour).append(" hour ");
        }
        if (minute != 0) {
            sbd.append(minute).append(" min ");
        }
        if (second != 0) {
            sbd.append(second).append(" second ");
        }
        if (m != 0) {
            sbd.append(m).append(" ms ");
        }
        return sbd.toString().trim();
    }
}
