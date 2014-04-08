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

import com.banmayun.server.migration.from.core.Revision;
import com.google.common.base.Optional;

public class RevisionDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "file_id", "version", "group_id", "root_id", "md5",
            "bytes", "modified", "modified_by", "client_modified" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "r";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Revision parseRevision(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Revision ret = new Revision();
        ret.setFileId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setMD5((String) arg.get(COLUMN_ALIASES[4]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setModified((Timestamp) arg.get(COLUMN_ALIASES[6]));
        ret.setModifiedBy((Long) arg.get(COLUMN_ALIASES[7]));
        ret.setClientModified((Timestamp) arg.get(COLUMN_ALIASES[8]));
        return ret;
    }

    public static List<Revision> parseRevisions(List<Map<String, Object>> arg) {
        List<Revision> ret = new ArrayList<Revision>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseRevision(map));
        }
        return ret;
    }

    public RevisionDAO(Connection conn) {
        super(conn);
    }

    public Revision create(Revision revision) throws SQLException {
        String sql = "INSERT INTO tbl_revision (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "RETURNING " + COLUMNS_RETURN;
        return parseRevision(super.uniqueResult(sql, revision.getFileId(), revision.getVersion(),
                revision.getGroupId(), revision.getRootId(), revision.getMD5(), revision.getBytes(),
                revision.getModified(), revision.getModifiedBy(), revision.getClientModified()));
    }

    public Optional<Revision> get(long fileId, long version) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_revision r " + "WHERE r.file_id=? AND r.version=?";
        return Optional.fromNullable(parseRevision(super.uniqueResult(sql, fileId, version)));
    }

    public List<Revision> listByGroupId(long groupId, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_revision r "
                + "WHERE r.group_id=? ORDER BY r.version DESC OFFSET ? LIMIT ?";
        return parseRevisions(super.list(sql, groupId, offset, limit));
    }

    public List<Revision> listByFileId(long fileId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_revision r " + "WHERE r.file_id=? ORDER BY r.version DESC";
        return parseRevisions(super.list(sql, fileId));
    }

    public Optional<Revision> delete(long fileId, long version) throws SQLException {
        String sql = "DELETE FROM tbl_revision r " + "WHERE r.file_id=? AND r.version=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseRevision(super.uniqueResult(sql, fileId, version)));
    }

    public void deleteByVersionBelowOrEqual(long groupId, long rootId, long version) throws SQLException {
        // decrement refs
        String sql = "WITH s AS " + "(SELECT r.md5, r.bytes, COUNT(r) AS count FROM tbl_revision r "
                + "WHERE r.group_id=? AND r.root_id=? AND r.version<=? " + "GROUP BY r.md5, r.bytes) "
                + "UPDATE tbl_data d SET refs=d.refs-s.count FROM s " + "WHERE d.md5=s.md5 AND d.bytes=s.bytes";
        super.update(sql, groupId, rootId, version);

        sql = "DELETE FROM tbl_revision r " + "WHERE r.group_id=? AND r.root_id=? AND r.version<=?";
        super.update(sql, groupId, rootId, version);
    }

    public List<Map<String, Object>> checkDataRefs() throws SQLException {
        String sql = "WITH s AS " + "(SELECT r.md5, r.bytes, COUNT(r) AS count FROM tbl_revision r "
                + "GROUP BY r.md5, r.bytes) " + "SELECT d.md5 AS md5, d.bytes AS bytes, d.refs AS refs, count "
                + "FROM tbl_data d, s " + "WHERE (NOT s.md5 is NULL) "
                + "AND ((d.md5=s.md5 AND d.bytes=s.bytes AND d.refs<count) " + "  OR NOT EXISTS "
                + "(SELECT 1 FROM tbl_data d WHERE d.md5=s.md5 AND d.bytes=s.bytes))";
        return super.list(sql);
    }
}
