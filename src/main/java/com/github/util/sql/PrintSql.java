package com.github.util.sql;

import com.github.util.Logs;
import com.github.util.U;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementInterceptor;

import java.sql.SQLException;
import java.util.Properties;

public class PrintSql implements StatementInterceptor {

    @Override
    public void init(Connection connection, Properties properties) throws SQLException {}

    @Override
    public ResultSetInternalMethods preProcess(String sql, Statement statement,
                                               Connection connection) throws SQLException {
        if (U.isBlank(sql) && statement != null) {
            sql = statement.toString();
            if (U.isNotBlank(sql)) {
                int indexOf = sql.indexOf(':');
                if (indexOf > 0) {
                    sql = SqlFormat.format(sql.substring(indexOf + 1).trim());
                }
            }
        }
        if (U.isNotBlank(sql)) {
            if (Logs.SQL_LOG.isDebugEnabled() && !"SELECT 1".equalsIgnoreCase(sql)) {
                Logs.SQL_LOG.debug("{}", sql);
            }
        }
        return null;
    }

    @Override
    public ResultSetInternalMethods postProcess(String s, Statement statement,
                                                ResultSetInternalMethods resultSetInternalMethods,
                                                Connection connection) throws SQLException {
        return null;
    }

    @Override
    public boolean executeTopLevelOnly() { return false; }

    @Override
    public void destroy() {}
}
