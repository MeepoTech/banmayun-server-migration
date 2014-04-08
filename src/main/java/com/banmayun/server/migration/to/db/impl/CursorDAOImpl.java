package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.to.core.Cursor;
import com.banmayun.server.migration.to.db.CursorDAO;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class CursorDAOImpl extends AbstractDAO implements CursorDAO {

    protected static final String TABLE_NAME = "cursors";
    protected static final String TABLE_ALIAS = "_cursor_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "root_id", "version", "pos", "next_version",
            "prev", "created_at" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 0, COLUMN_NAMES.length));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public CursorDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    public static Cursor parseCursor(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Cursor ret = new Cursor();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setPos((Integer) arg.get(COLUMN_ALIASES[3]));
        ret.setNextVersion((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setPrev((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[6]));
        return ret;
    }

    @Override
    public Cursor createCursor(Cursor cursor) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING %3$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        return parseCursor(super.uniqueResult(sql, cursor.getId(), cursor.getRootId(), cursor.getVersion(),
                cursor.getPos(), cursor.getNextVersion(), cursor.getPrev(), cursor.getCreatedAt()));
    }

    @Override
    public Optional<Cursor> getCursor(long rootId, long id) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.id=?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseCursor(super.uniqueResult(sql, rootId, id)));
    }

    @Override
    public Optional<Cursor> deleteCursor(long rootId, long id) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.id=? RETURNING %3$s",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseCursor(super.uniqueResult(sql, rootId, id)));
    }

    @Override
    public void deleteCursorsCreatedBefore(Timestamp time) throws DAOException {
        // delete
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.created_at<?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, time);
    }

    @Override
    public void setIdInitValue() throws DAOException {
        // TODO Auto-generated method stub
        String sql = String.format("SELECT setval('cursors_id_seq', MAX(id)) FROM %1$s", TABLE_NAME);
        super.uniqueResult(sql);
    }
}
