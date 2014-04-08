/* ****************************************************************************
 * MEEPOTECH CONFIDENTIAL
 * ----------------------
 * [2013] - [2014] MeePo Technology Incorporated
 * All Rights Reserved.
 *
 * IMPORTANT NOTICE:
 * All information contained herein is, and remains the property of MeePo
 * Technology Incorporated and its suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to MeePo Technology
 * Incorporated and its suppliers and may be covered by Chinese and Foreign
 * Patents, patents in process, and are protected by trade secret or copyright
 * law. Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained from
 * MeePo Technology Incorporated.
 * ****************************************************************************
 */

package com.banmayun.server.migration.from.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractDAO {

    protected Connection conn = null;
    protected QueryRunner runner = new QueryRunner();
    protected MapHandler handler = new MapHandler();
    protected MapListHandler listHandler = new MapListHandler();

    public AbstractDAO(Connection conn) {
        this.conn = conn;
    }

    public Connection getConnection() {
        return this.conn;
    }

    protected Map<String, Object> uniqueResult(String sql) throws SQLException {
        return this.runner.query(this.conn, sql, this.handler);
    }

    protected Map<String, Object> uniqueResult(String sql, Object... params) throws SQLException {
        return this.runner.query(this.conn, sql, this.handler, params);
    }

    protected List<Map<String, Object>> list(String sql) throws SQLException {
        return this.runner.query(this.conn, sql, this.listHandler);
    }

    protected List<Map<String, Object>> list(String sql, Object... params) throws SQLException {
        return this.runner.query(this.conn, sql, this.listHandler, params);
    }

    protected int update(String sql) throws SQLException {
        return this.runner.update(this.conn, sql);
    }

    protected int update(String sql, Object... params) throws SQLException {
        return this.runner.update(this.conn, sql, params);
    }

    protected static String[] getColumnAliases(String tableAlias, String[] columnNames) {
        String[] ret = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            ret[i] = tableAlias + "_" + columnNames[i];
        }
        return ret;
    }

    protected static String getColumnsInsert(String tableAlias, String[] columnNames) {
        return StringUtils.join(columnNames, ", ");
    }

    protected static String getColumnsReturn(String tableAlias, String[] columnNames) {
        return StringUtils.remove(getColumnsSelect(tableAlias, columnNames), tableAlias + ".");
    }

    protected static String getColumnsSelect(String tableAlias, String[] columnNames) {
        String[] tmp = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            tmp[i] = tableAlias + "." + columnNames[i] + " AS " + tableAlias + "_" + columnNames[i];
        }
        return StringUtils.join(tmp, ", ");
    }

    protected static String getColumnsUpdate(String tableAlias, String[] columnNames) {
        String[] tmp = new String[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            tmp[i] = columnNames[i] + "=?";
        }
        return StringUtils.join(tmp, ", ");
    }
}
