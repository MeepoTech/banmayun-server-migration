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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.from.core.Cursor;
import com.google.common.base.Optional;

public class CursorDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "id", "group_id", "root_id", "version", "pos",
            "next_version", "prev", "created" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "c";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Cursor parseCursor(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Cursor ret = new Cursor();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setPos((Integer) arg.get(COLUMN_ALIASES[4]));
        ret.setNextVersion((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setPrev((Long) arg.get(COLUMN_ALIASES[6]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[7]));
        return ret;
    }

    public static List<Cursor> parseCursors(List<Map<String, Object>> arg) {
        List<Cursor> ret = new ArrayList<Cursor>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseCursor(map));
        }
        return ret;
    }

    public CursorDAO(Connection conn) {
        super(conn);
    }

    public Cursor create(Cursor cursor) throws SQLException {
        String sql = "INSERT INTO tbl_cursor (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "RETURNING " + COLUMNS_RETURN;
        return parseCursor(super.uniqueResult(sql, cursor.getGroupId(), cursor.getRootId(), cursor.getVersion(),
                cursor.getPos(), cursor.getNextVersion(), cursor.getPrev(), cursor.getCreated()));
    }

    public List<Cursor> listByGroupId(long groupId, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_cursor c "
                + "WHERE c.group_id=? ORDER BY c.created DESC OFFSET ? LIMIT ?";
        return parseCursors(super.list(sql, groupId, offset, limit));
    }

    public Optional<Cursor> get(long id) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_cursor c " + "WHERE c.id=?";
        return Optional.fromNullable(parseCursor(super.uniqueResult(sql, id)));
    }

    public Optional<Cursor> delete(long id) throws SQLException {
        String sql = "DELETE FROM tbl_cursor c " + "WHERE c.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseCursor(super.uniqueResult(sql, id)));
    }

    public void deleteByVersionBelow(long groupId, long rootId, long version) throws SQLException {
        String sql = "DELETE FROM tbl_cursor c " + "WHERE c.group_id=? AND c.root_id=? AND c.version<?";
        super.update(sql, groupId, rootId, version);
    }
}
