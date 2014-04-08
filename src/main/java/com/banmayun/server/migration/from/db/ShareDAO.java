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
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.from.core.Share;
import com.google.common.base.Optional;

public class ShareDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "id", "group_id", "root_id", "path", "password",
            "expires", "created", "created_by" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "s";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Share parseShare(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Share ret = new Share();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setPath((String) arg.get(COLUMN_ALIASES[3]));
        ret.setPassword((String) arg.get(COLUMN_ALIASES[4]));
        ret.setExpires((Timestamp) arg.get(COLUMN_ALIASES[5]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[6]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[7]));
        return ret;
    }

    public static List<Share> parseShares(List<Map<String, Object>> arg) {
        List<Share> ret = new ArrayList<Share>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseShare(map));
        }
        return ret;
    }

    public ShareDAO(Connection conn) {
        super(conn);
    }

    public Share create(Share share) throws SQLException {
        String sql = "INSERT INTO tbl_share (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?, ?, ?) " + "RETURNING "
                + COLUMNS_RETURN;
        return parseShare(super.uniqueResult(sql, share.getGroupId(), share.getRootId(), share.getPath(),
                share.getPassword(), share.getExpires(), share.getCreated(), share.getCreatedBy()));
    }

    public Optional<Share> get(long id) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_share s " + "WHERE s.id=?";
        return Optional.fromNullable(parseShare(super.uniqueResult(sql, id)));
    }

    public Pair<Integer, List<Share>> listAndCountByRoot(long groupId, long rootId, int offset, int limit)
            throws SQLException {
        String sql = "SELECT count(*) as count" + " FROM tbl_share s " + "WHERE s.group_id=? AND s.root_id=? ";
        Integer count = ((Long) super.uniqueResult(sql, groupId, rootId).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_share s " + "WHERE s.group_id=? AND s.root_id=? "
                + "ORDER BY s.created DESC OFFSET ? LIMIT ?";
        List<Share> shares = parseShares(super.list(sql, groupId, rootId, offset, limit));
        return Pair.of(count, shares);
    }

    public List<Share> listByRoot(long groupId, long rootId, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_share s " + "WHERE s.group_id=? AND s.root_id=? "
                + "ORDER BY s.created DESC OFFSET ? LIMIT ?";
        return parseShares(super.list(sql, groupId, rootId, offset, limit));
    }

    public List<Share> listByPath(long groupId, long rootId, String path, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_share s "
                + "WHERE s.group_id=? AND s.root_id=? AND lower(s.path)=lower(?) "
                + "ORDER BY s.created DESC OFFSET ? LIMIT ?";
        return parseShares(super.list(sql, groupId, rootId, path, offset, limit));
    }

    public Pair<Integer, List<Share>> listAndCountByPath(long groupId, long rootId, String path, int offset, int limit)
            throws SQLException {
        String sql = "SELECT count(*) as count" + " FROM tbl_share s "
                + "WHERE s.group_id=? AND s.root_id=? AND lower(s.path)=lower(?) ";
        Integer count = ((Long) super.uniqueResult(sql, groupId, rootId, path).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_share s "
                + "WHERE s.group_id=? AND s.root_id=? AND lower(s.path)=lower(?) "
                + "ORDER BY s.created DESC OFFSET ? LIMIT ?";
        List<Share> shares = parseShares(super.list(sql, groupId, rootId, path, offset, limit));
        return Pair.of(count, shares);
    }

    public Optional<Share> delete(long id) throws SQLException {
        String sql = "DELETE FROM tbl_share s WHERE s.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseShare(super.uniqueResult(sql, id)));
    }

    public int deleteByRoot(long groupId, long rootId) throws SQLException {
        String sql = "DELETE FROM tbl_share s WHERE s.group_id=? AND s.root_id=? ";
        return super.update(sql, groupId, rootId);
    }

    public Optional<Share> update(long id, Share update) throws SQLException {
        Share share = this.get(id).orNull();
        if (share == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_share s SET " + COLUMNS_UPDATE + " WHERE s.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseShare(super.uniqueResult(sql,
                update.getId() == null ? share.getId() : update.getId(),
                update.getGroupId() == null ? share.getGroupId() : update.getGroupId(),
                update.getRootId() == null ? share.getRootId() : update.getRootId(),
                update.getPath() == null ? share.getPath() : update.getPath(),
                update.getPassword() == null ? share.getPassword() : update.getPassword(),
                update.getExpires() == null ? share.getExpires() : update.getExpires(),
                update.getCreated() == null ? share.getCreated() : update.getCreated(),
                update.getCreatedBy() == null ? share.getCreatedBy() : update.getCreatedBy(), id)));
    }

    public int deleteExpired() throws SQLException {
        String sql = "DELETE FROM tbl_share s " + "WHERE s.expires<=?";
        return super.update(sql, new Timestamp(System.currentTimeMillis()));
    }
}
