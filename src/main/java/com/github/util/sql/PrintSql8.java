package com.github.util.sql;

import com.github.util.Dates;
import com.github.util.Logs;
import com.github.util.U;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.ServerPreparedQuery;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * mysql 5 的连接参数是: &statementInterceptors=com.github.util.sql.PrintSql5
 * mysql 8 的连接参数是: &queryInterceptors=com.github.util.sql.PrintSql8
 */
public class PrintSql8 implements QueryInterceptor {

    private static final Cache<Thread, Long> TIME_CACHE = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).build();

    @Override
    public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
        return this;
    }

    @Override
    public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
        TIME_CACHE.put(Thread.currentThread(), System.currentTimeMillis());
        return null;
    }

    @Override
    public <T extends Resultset> T postProcess(Supplier<String> sql, Query query, T rs, ServerSession serverSession) {
        Thread thread = Thread.currentThread();
        try {
            String realSql = U.isNotNull(sql) ? sql.get() : "";
            if (realSql.contains("?") && query instanceof ServerPreparedQuery && rs instanceof ResultSetImpl) {
                // 如果设置了 useServerPrepStmts 为 true 的话, query 将是 ServerPreparedQuery,
                // 此时通过下面方式获取的 sql 语句中不会有单引号('), 比如应该是 name = '张三' 的将会输出成 name = 张三
                // 且 insert 语句只能输出带 ? 的语句
                String tmp = ((ResultSetImpl) rs).getOwningQuery().toString();

                String colon = ":";
                if (U.isNotBlank(tmp) && tmp.contains(colon)) {
                    realSql = tmp.substring(tmp.indexOf(colon) + colon.length()).trim();
                }
            }

            if (U.isNotBlank(realSql)) {
                if (Logs.SQL_LOG.isDebugEnabled()) {
                    StringBuilder sbd = new StringBuilder();

                    Long start = TIME_CACHE.getIfPresent(thread);
                    if (U.greater0(start)) {
                        sbd.append("time: ").append(Dates.toHuman(System.currentTimeMillis() - start)).append(" ms, ");
                    }

                    int size;
                    if (U.isNotNull(rs) && rs.hasRows()) {
                        size = rs.getRows().size();
                    } else {
                        size = 0;
                    }
                    sbd.append("size: ").append(size).append(", ");

                    sbd.append("sql:\n").append(SqlFormat.format(realSql).replaceFirst("^\\s*?\n", ""));
                    Logs.SQL_LOG.debug(sbd.toString());
                }
            }
        } finally {
            TIME_CACHE.invalidate(thread);
        }
        return null;
    }

    @Override
    public boolean executeTopLevelOnly() { return false; }

    @Override
    public void destroy() {
        TIME_CACHE.invalidateAll();
    }
}
