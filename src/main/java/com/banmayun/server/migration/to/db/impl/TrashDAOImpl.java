package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Meta;
import com.banmayun.server.migration.to.core.Trash;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.TrashDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class TrashDAOImpl extends AbstractDAO implements TrashDAO {

    protected static final String TABLE_NAME = "trashes";
    protected static final String TABLE_ALIAS = "_trash_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "root_id", "meta_id", "is_deleted",
            "created_at", "created_by" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Trash parseTrash(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Trash ret = new Trash();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setMetaId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setIsDeleted((Boolean) arg.get(COLUMN_ALIASES[3]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[4]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[5]));
        return ret;
    }

    public static List<Trash> parseTrashes(List<Map<String, Object>> arg) {
        List<Trash> ret = new ArrayList<Trash>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseTrash(map));
        }
        return ret;
    }

    public static Pair<Trash, Meta> parseTrashMeta(Map<String, Object> arg) {
        return Pair.of(parseTrash(arg), MetaDAOImpl.parseMeta(arg));
    }

    public static List<Pair<Trash, Meta>> parseTrashMetaList(List<Map<String, Object>> arg) {
        List<Pair<Trash, Meta>> ret = new ArrayList<Pair<Trash, Meta>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseTrashMeta(map));
        }
        return ret;
    }

    public TrashDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Trash createTrash(Trash trash) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?) RETURNING %3$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        return parseTrash(super.uniqueResult(sql, trash.getRootId(), trash.getMetaId(), trash.getIsDeleted(),
                trash.getCreatedAt(), trash.getCreatedBy()));
    }

    @Override
    public Pair<Trash, Meta> getTrash(long rootId, long id) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.meta_id "
                + "WHERE %2$s.root_id=? AND %2$s.id=? AND NOT %2$s.is_deleted", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT, MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS, MetaDAOImpl.COLUMNS_SELECT);
        return parseTrashMeta(super.uniqueResult(sql, rootId, id));
    }

    @Override
    public int countTrashesForRoot(long rootId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=? "
                + "AND NOT %2$s.is_deleted", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Pair<Trash, Meta>> listTrashesForRoot(long rootId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.meta_id "
                + "WHERE %2$s.root_id=? AND NOT %2$s.is_deleted ORDER By %2$s.created_at DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS,
                MetaDAOImpl.COLUMNS_SELECT);
        return parseTrashMetaList(super.list(sql, rootId, offset, limit));
    }

    @Override
    public Optional<Trash> deleteTrash(long rootId, long id) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET is_deleted=? WHERE %2$s.root_id=? AND %2$s.id=? "
                + "AND NOT %2$s.is_deleted RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseTrash(super.uniqueResult(sql, true, rootId, id)));
    }

    @Override
    public void deleteTrashesForRoot(long rootId) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET is_deleted=? WHERE %2$s.root_id=? AND NOT %2$s.is_deleted",
                TABLE_NAME, TABLE_ALIAS);
        super.update(sql, true, rootId);
    }

    @Override
    public void undeleteTrashesForRoot(long rootId) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET is_deleted=? WHERE %2$s.root_id=? AND %2$s.is_deleted",
                TABLE_NAME, TABLE_ALIAS);
        super.update(sql, false, rootId);
    }

    @Override
    public void deleteTrashesCreatedBefore(Timestamp time) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.created_at<?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, time);
    }
}
