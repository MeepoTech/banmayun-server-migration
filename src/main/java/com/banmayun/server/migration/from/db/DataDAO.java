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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.banmayun.server.migration.from.core.Data;
import com.google.common.base.Optional;

public class DataDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "md5", "bytes", "refs", "location" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "d";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Data parseData(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Data ret = new Data();
        ret.setMD5((String) arg.get(COLUMN_ALIASES[0]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRefs((Integer) arg.get(COLUMN_ALIASES[2]));
        ret.setLocation((String) arg.get(COLUMN_ALIASES[3]));
        return ret;
    }

    public static List<Data> parseDatas(List<Map<String, Object>> arg) {
        List<Data> ret = new ArrayList<Data>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseData(map));
        }
        return ret;
    }

    public DataDAO(Connection conn) {
        super(conn);
    }

    public Data create(Data data) throws SQLException {
        String sql = "INSERT INTO tbl_data (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?) " + "RETURNING "
                + COLUMNS_RETURN;
        return parseData(super.uniqueResult(sql, data.getMD5(), data.getBytes(), data.getRefs(), data.getLocation()));
    }

    public Optional<Data> get(String md5, long bytes) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_data d " + "WHERE d.md5=? AND d.bytes=?";
        return Optional.fromNullable(parseData(super.uniqueResult(sql, md5, bytes)));
    }

    public List<Data> list(int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_data d " + "ORDER BY d.md5, d.bytes OFFSET ? LIMIT ?";
        return parseDatas(super.list(sql, offset, limit));
    }

    public List<Data> listByZeroRefs(int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_data d " + "WHERE d.refs<=0 LIMIT ?";
        return parseDatas(super.list(sql, limit));
    }

    public Optional<Data> incrementRefs(String md5, long bytes) throws SQLException {
        String sql = "UPDATE tbl_data d SET refs=d.refs+1 " + "WHERE d.md5=? AND d.bytes=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseData(super.uniqueResult(sql, md5, bytes)));
    }

    public Optional<Data> decrementRefs(String md5, long bytes) throws SQLException {
        String sql = "UPDATE tbl_data d SET refs=d.refs-1 " + "WHERE d.md5=? AND d.bytes=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseData(super.uniqueResult(sql, md5, bytes)));
    }

    public Optional<Data> delete(String md5, long bytes) throws SQLException {
        String sql = "DELETE FROM tbl_data d " + "WHERE d.md5=? AND d.bytes=? AND d.refs<=0 " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseData(super.uniqueResult(sql, md5, bytes)));
    }
}
