package com.github.mte.util;

import com.alibaba.druid.sql.SQLUtils;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementInterceptor;

import java.sql.SQLException;
import java.util.Properties;

/** print MariaDB sql */
public class PrintSql implements StatementInterceptor {

    public void init(Connection connection, Properties properties) throws SQLException {}

    public ResultSetInternalMethods preProcess(String sql, Statement statement,
                                               Connection connection) throws SQLException {
        if (U.isBlank(sql) && statement != null) {
            sql = statement.toString();
            if (U.isNotBlank(sql) && sql.indexOf(':') > 0) {
                sql = SQLUtils.formatMySql(sql.substring(sql.indexOf(':') + 1).trim());
            }
        }
        if (U.isNotBlank(sql)) {
            if (Logs.SQL_LOG.isDebugEnabled())
                Logs.SQL_LOG.debug("{}", sql);
                //LogUtil.SQL_LOG.debug("/* sql begin */\n{}\n/* sql end.. */", sql);
        }
        return null;
    }

    public ResultSetInternalMethods postProcess(String s, Statement statement,
                 ResultSetInternalMethods resultSetInternalMethods, Connection connection) throws SQLException {
        return null;
    }

    public boolean executeTopLevelOnly() { return false; }
    public void destroy() {}
}
