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

import com.banmayun.server.migration.from.core.Comment;
import com.google.common.base.Optional;

public class CommentDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "id", "file_id", "contents", "created", "created_by" };
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

    public static Comment parseComment(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Comment ret = new Comment();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setFileId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setContents((String) arg.get(COLUMN_ALIASES[2]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[3]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[4]));
        return ret;
    }

    public static List<Comment> parseComments(List<Map<String, Object>> arg) {
        List<Comment> ret = new ArrayList<Comment>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseComment(map));
        }
        return ret;
    }

    public CommentDAO(Connection conn) {
        super(conn);
    }

    public Comment create(Comment comment) throws SQLException {
        String sql = "INSERT INTO tbl_comment (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?) " + "RETURNING "
                + COLUMNS_RETURN;
        return parseComment(super.uniqueResult(sql, comment.getFileId(), comment.getContents(), comment.getCreated(),
                comment.getCreatedBy()));
    }

    public Optional<Comment> get(long id) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_comment c" + "WHERE c.id=?";
        return Optional.fromNullable(parseComment(super.uniqueResult(sql, id)));
    }

    public List<Comment> listByFileId(long fileId, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_comment c "
                + "WHERE c.file_id=? ORDER BY c.created DESC OFFSET ? LIMIT ?";
        return parseComments(super.list(sql, fileId, offset, limit));
    }

    public Pair<Integer, List<Comment>> listAndCountByFileId(long fileId, int offset, int limit) throws SQLException {
        String sql = "SELECT count(*) as count FROM tbl_comment c WHERE c.file_id=?";
        Integer count = ((Long) super.uniqueResult(sql, fileId).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_comment c "
                + "WHERE c.file_id=? ORDER BY c.created DESC OFFSET ? LIMIT ?";
        List<Comment> comments = parseComments(super.list(sql, fileId, offset, limit));
        return Pair.of(count, comments);
    }

    public Optional<Comment> delete(long id) throws SQLException {
        String sql = "DELETE FROM tbl_comment c WHERE c.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseComment(super.uniqueResult(sql, id)));
    }
}
