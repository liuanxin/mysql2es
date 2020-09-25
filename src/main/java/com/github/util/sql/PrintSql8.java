package com.github.util.sql;

import com.github.util.Logs;
import com.github.util.U;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class PrintSql8 implements QueryInterceptor {

    private static final Cache<Thread, Long> TIME = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    @Override
    public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
        return this;
    }

    @Override
    public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
        TIME.put(Thread.currentThread(), System.currentTimeMillis());
        return null;
    }

    @Override
    public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery,
                                               T originalResultSet, ServerSession serverSession) {
        Thread self = Thread.currentThread();
        if (U.isNotBlank(sql)) {
            if (Logs.SQL_LOG.isDebugEnabled()) {
                String formatSql = SqlFormat.format(sql.get());
                Long start = TIME.getIfPresent(self);
                if (start != null) {
                    Logs.SQL_LOG.debug("time: {} ms, sql:\n{}", (System.currentTimeMillis() - start), formatSql);
                } else {
                    Logs.SQL_LOG.debug("sql:\n{}", formatSql);
                }
            }
        }
        TIME.invalidate(self);
        return null;
    }

    @Override
    public boolean executeTopLevelOnly() { return false; }

    @Override
    public void destroy() {
        TIME.invalidateAll();
    }
}
