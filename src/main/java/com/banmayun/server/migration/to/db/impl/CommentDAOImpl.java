package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.to.core.Comment;
import com.banmayun.server.migration.to.db.CommentDAO;
import com.banmayun.server.migration.to.db.DAOException;
import com.google.common.base.Optional;

public class CommentDAOImpl extends AbstractDAO implements CommentDAO {

    protected static final String TABLE_NAME = "comments";
    protected static final String TABLE_ALIAS = "_comment_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "root_id", "meta_id", "contents", "created",
            "created_by" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_UPDATE;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_UPDATE = DAOUtils.getColumnsUpdate(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Comment parseComment(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Comment ret = new Comment();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setMetaId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setContents((String) arg.get(COLUMN_ALIASES[3]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[4]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[5]));
        return ret;
    }

    public static List<Comment> parseComments(List<Map<String, Object>> arg) {
        List<Comment> ret = new ArrayList<Comment>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseComment(map));
        }
        return ret;
    }

    public CommentDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Comment createComment(Comment comment) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?) RETURNING %2$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        Comment createdComment = parseComment(super.uniqueResult(sql, comment.getRootId(), comment.getMetaId(),
                comment.getContents(), comment.getCreatedAt(), comment.getCreatedBy()));

        // increment comment_count for meta
        sql = String.format("UPDATE %1$s %2$s SET comment_count=%2$s.comment_count+1 WHERE %2$s.root_id=? "
                + "AND %2$s.meta_id=?", MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, createdComment.getRootId(), createdComment.getMetaId());

        return createdComment;
    }

    @Override
    public Optional<Comment> getComment(long rootId, long metaId, long commentId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? AND %2$s.id=?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseComment(super.uniqueResult(sql, rootId, metaId, commentId)));
    }

    @Override
    public int countComments() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countCommentsForRoot(long rootId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=?", TABLE_NAME,
                TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countCommentsForMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=?",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId, metaId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Comment> listComments(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.created_at DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseComments(super.list(sql, offset, limit));
    }

    @Override
    public List<Comment> listCommentsForRoot(long rootId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? "
                + "ORDER BY %2$s.created_at DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseComments(super.list(sql, rootId, offset, limit));
    }

    @Override
    public List<Comment> listCommentsForMeta(long rootId, long metaId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? "
                + "ORDER BY %2$s.created_at DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseComments(super.list(sql, rootId, metaId, offset, limit));
    }

    @Override
    public void deleteComments() throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        super.update(sql);

        sql = String.format("UPDATE %1$s %2$s SET comment_count=0", MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql);
    }

    @Override
    public void deleteCommentsForRoot(long rootId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_RETURN);
        super.update(sql, rootId);

        sql = String.format("UPDATE %1$s %2$s SET comment_count=0 WHERE %2$s.root_id=?", MetaDAOImpl.TABLE_NAME,
                MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId);
    }

    @Override
    public void deleteCommentsForMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_RETURN);
        super.update(sql, rootId, metaId);

        sql = String.format("UPDATE %1$s %2$s SET comment_count=0 WHERE %2$s.root_id=? AND %2$s.meta_id=?",
                MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId, metaId);
    }

    @Override
    public Optional<Comment> deleteComment(long rootId, long metaId, long commentId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? AND %2$s.id=? "
                + "RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        Comment deletedComment = parseComment(super.uniqueResult(sql, rootId, metaId, commentId));
        if (deletedComment == null) {
            return Optional.absent();
        }

        // decrement comment_count for meta
        sql = String.format("UPDATE %1$s %2$s SET comment_count=%2$s.comment_count-1 WHERE %2$s.root_id=? "
                + "AND %2$s.id=?", MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId, metaId);

        return Optional.of(deletedComment);
    }

    @Override
    public void updateMetaCommentCount() throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.root_id, %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.root_id=%4$s.root_id AND %2$s.meta_id=%4$s.id "
                + "GROUP BY %4$s.root_id, %4$s.id) UPDATE %3$s %4$s SET comment_count=tmp.count FROM tmp "
                + "WHERE %4$s.root_id=tmp.root_id AND %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS, MetaDAOImpl.TABLE_NAME,
                MetaDAOImpl.TABLE_ALIAS);
        super.update(sql);
    }

    @Override
    public void updateMetaCommentCountForRoot(long rootId) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.root_id, %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.root_id=%4$s.root_id AND %2$s.meta_id=%4$s.id "
                + "WHERE %4$s.root_id=? GROUP BY %4$s.root_id, %4$s.id) "
                + "UPDATE %3$s %4$s SET comment_count=tmp.count FROM tmp "
                + "WHERE %4$s.root_id=tmp.root_id AND %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS, MetaDAOImpl.TABLE_NAME,
                MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId);
    }

    @Override
    public void updateMetaCommentCountForMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.root_id, %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.root_id=%4$s.root_id AND %2$s.meta_id=%4$s.id "
                + "WHERE %4$s.root_id=? AND %4$s.id=? GROUP BY %4$s.root_id, %4$s.id) "
                + "UPDATE %3$s %4$s SET comment_count=tmp.count FROM tmp "
                + "WHERE %4$s.root_id=tmp.root_id AND %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS, MetaDAOImpl.TABLE_NAME,
                MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId, metaId);
    }

}
