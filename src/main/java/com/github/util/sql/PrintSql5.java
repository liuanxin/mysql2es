//package com.github.util.sql;
//
//import com.github.util.Dates;
//import com.github.util.Logs;
//import com.github.util.U;
//import com.google.common.cache.Cache;
//import com.google.common.cache.CacheBuilder;
//import com.mysql.jdbc.Connection;
//import com.mysql.jdbc.ResultSetInternalMethods;
//import com.mysql.jdbc.Statement;
//import com.mysql.jdbc.StatementInterceptor;
//
//import java.sql.SQLException;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
///**
// * mysql 5 的连接参数是: &statementInterceptors=com.github.util.sql.PrintSql5
// * mysql 8 的连接参数是: &queryInterceptors=com.github.util.sql.PrintSql8
// */
//public class PrintSql5 implements StatementInterceptor {
//
//    private static final Cache<Thread, Long> TIME_CACHE = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).build();
//
//    @Override
//    public void init(Connection connection, Properties properties) throws SQLException {}
//
//    @Override
//    public ResultSetInternalMethods preProcess(String sql, Statement statement,
//                                               Connection connection) throws SQLException {
//        TIME_CACHE.put(Thread.currentThread(), System.currentTimeMillis());
//        return null;
//    }
//
//    @Override
//    public ResultSetInternalMethods postProcess(String sql, Statement statement, ResultSetInternalMethods resultSet,
//                                                Connection connection) throws SQLException {
//        Thread thread = Thread.currentThread();
//        try {
//            if (U.isBlank(sql) && U.isNotBlank(statement)) {
//                sql = statement.toString();
//                if (U.isNotBlank(sql)) {
//                    int i = sql.indexOf(':');
//                    if (i > 0 ) {
//                        sql = sql.substring(i + 1).trim();
//                    }
//                }
//            }
//            if (U.isNotBlank(sql)) {
//                if (Logs.SQL_LOG.isDebugEnabled()) {
//
//                    StringBuilder sbd = new StringBuilder();
//                    Long start = TIME_CACHE.getIfPresent(thread);
//                    if (U.greater0(start)) {
//                        sbd.append("time: ").append(Dates.toHuman(System.currentTimeMillis() - start)).append(" ms, ");
//                    }
//
//                    int size;
//                    if (resultSet != null && resultSet.reallyResult() && resultSet.last()) {
//                        size = resultSet.getRow();
//                        resultSet.beforeFirst();
//                    } else {
//                        size = 0;
//                    }
//                    sbd.append("size: ").append(size).append(", ");
//
//                    // druid -> SQLUtils.formatMySql
//                    sbd.append("sql:\n").append(SqlFormat.format(sql).replaceFirst("^\\s*?\n", ""));
//                    Logs.SQL_LOG.debug(sbd.toString());
//                }
//            }
//        } finally {
//            TIME_CACHE.invalidate(thread);
//        }
//        return null;
//    }
//
//    @Override
//    public boolean executeTopLevelOnly() { return false; }
//
//    @Override
//    public void destroy() {
//        TIME_CACHE.invalidateAll();
//    }
//}
