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

import com.banmayun.server.migration.from.core.Meta;
import com.banmayun.server.migration.from.core.Trash;
import com.google.common.base.Optional;
import com.yammer.dropwizard.util.Duration;

public class TrashDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "file_id", "group_id", "root_id", "created",
            "created_by", "is_deleted" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "t";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Trash parseTrash(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Trash ret = new Trash();
        ret.setFileId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[3]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setIsDeleted((Boolean) arg.get(COLUMN_ALIASES[5]));
        return ret;
    }

    public static List<Trash> parseTrashes(List<Map<String, Object>> arg) {
        List<Trash> ret = new ArrayList<Trash>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseTrash(map));
        }
        return ret;
    }

    public TrashDAO(Connection conn) {
        super(conn);
    }

    public Trash create(Trash trash) throws SQLException {
        String sql = "INSERT INTO tbl_trash (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?, ?) " + "RETURNING "
                + COLUMNS_RETURN;
        return parseTrash(super.uniqueResult(sql, trash.getFileId(), trash.getGroupId(), trash.getRootId(),
                trash.getCreated(), trash.getCreatedBy(), trash.getIsDeleted()));
    }

    public Optional<Trash> getByFileId(long fileId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_trash t " + "WHERE t.file_id=?";
        return Optional.fromNullable(parseTrash(super.uniqueResult(sql, fileId)));
    }

    public List<Meta> listByRoot(long groupId, long rootId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + MetaDAO.COLUMNS_SELECT + " FROM tbl_trash t "
                + "INNER JOIN tbl_meta m ON m.file_id=t.file_id " + "WHERE t.group_id=? AND t.root_id=?"
                + "ORDER BY t.created DESC ";
        return MetaDAO.parseMetasWithTrash(super.list(sql, groupId, rootId));
    }

    public List<Trash> listByGroupId(long groupId, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_trash t "
                + "WHERE t.group_id=? ORDER BY t.created DESC OFFSET ? LIMIT ?";
        return parseTrashes(super.list(sql, groupId, offset, limit));
    }

    public List<Meta> listByRoot(long groupId, long rootId, boolean isDeleted) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + MetaDAO.COLUMNS_SELECT + " FROM tbl_trash t "
                + "INNER JOIN tbl_meta m ON m.file_id=t.file_id "
                + "WHERE t.group_id=? AND t.root_id=? AND t.is_deleted=? " + "ORDER BY t.created DESC ";
        return MetaDAO.parseMetasWithTrash(super.list(sql, groupId, rootId, isDeleted));
    }

    public int deleteByGroup(long groupId, long rootId) throws SQLException {
        String sql = "DELETE FROM tbl_trash t " + "WHERE t.group_id=? AND t.root_id=? ";
        return super.update(sql, groupId, rootId);
    }

    public Optional<Trash> deleteByFileId(long fileId) throws SQLException {
        String sql = "DELETE FROM tbl_trash t " + "WHERE t.file_id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseTrash(super.uniqueResult(sql, fileId)));
    }

    public Optional<Trash> disableByFileId(long fileId) throws SQLException {
        String sql = "UPDATE tbl_trash t set is_deleted = true " + "WHERE t.file_id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseTrash(super.uniqueResult(sql, fileId)));
    }

    public int disableByGroupId(long groupId, long rootId) throws SQLException {
        String sql = "UPDATE tbl_trash t set is_deleted = true " + "WHERE t.group_id=? AND t.root_id=? ";
        return super.update(sql, groupId, rootId);
    }

    public List<Trash> deleteOverdue(long groupId, long rootId) throws SQLException {
        String sql = "DELETE FROM tbl_trash t " + "WHERE t.group_id=? AND t.root_id=? AND t.created<=? " + "RETURNING "
                + COLUMNS_RETURN;
        return parseTrashes(super.list(sql, groupId, rootId,
                new Timestamp(System.currentTimeMillis() - Duration.days(30).toMilliseconds())));
    }
}
