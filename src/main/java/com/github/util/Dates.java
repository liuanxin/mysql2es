package com.github.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/** util with Date */
public class Dates {

    public enum Type {
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
        /** yyyy-MM-dd am/pm --> am/pm 会根据时区自动完成, 也就是如果当前时区是北京的话, 会显示成 上午/下午 */
        YYYY_MM_DD_AP("yyyy-MM-dd a"),

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

        /** 到毫秒: yyyy-MM-ddTHH:mm:ss.SSSZ */
        TSZ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        /** 到毫秒: yyyy-MM-ddTHH:mm:ss.SSS */
        TS("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        /** 到秒: yyyy-MM-ddTHH:mm:ssZ */
        TZ("yyyy-MM-dd'T'HH:mm:ss'Z'"),
        /** 到秒: yyyy-MM-ddTHH:mm:ss */
        T("yyyy-MM-dd'T'HH:mm:ss"),

        /** yyyy/MM/dd */
        USA_YYYY_MM_DD("yyyy/MM/dd"),
        /** MM/dd/yyyy HH:mm:ss */
        USA_MM_DD_YYYY_HH_MM_SS("MM/dd/yyyy HH:mm:ss"),
        /** yyyy年MM月dd日 HH时mm分ss秒 */
        CN_YYYY_MM_DD_HH_MM_SS("yyyy年MM月dd日 HH时mm分ss秒"),
        /** yyyy年MM月dd日 HH点 */
        CN_YYYY_MM_DD_HH("yyyy年MM月dd日 HH点"),
        /** yyyy年MM月dd日 HH点 */
        CN_YYYY_MM_DD_HH_MM("yyyy年MM月dd日 HH点mm分"),
        /** yyyy年MM月dd日 */
        CN_YYYY_MM_DD("yyyy年MM月dd日"),

        /** 直接打印 new Date() 时的样式 */
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

    /** 2016-12-31 xx:yy:zz --> 2016-12-31 00:00:00 000 */
    public static Date startInDay(Date date) {
        if (U.isBlank(date)) {
            return null;
        }
        return new DateTime(date).hourOfDay().withMinimumValue()
                .minuteOfHour().withMinimumValue()
                .secondOfMinute().withMinimumValue()
                .millisOfSecond().withMinimumValue().toDate();
    }
    /** 2016-12-31 xx:yy:zz --> 2016-12-31 23:59:59 999 */
    public static Date endInDay(Date date) {
        if (U.isBlank(date)) {
            return null;
        }
        return new DateTime(date).hourOfDay().withMaximumValue()
                .minuteOfHour().withMaximumValue()
                .secondOfMinute().withMaximumValue()
                .millisOfSecond().withMaximumValue().toDate();
    }
}
