package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;

import com.banmayun.server.migration.to.core.Cursor;
import com.google.common.base.Optional;

public interface CursorDAO {

    public Cursor createCursor(Cursor cursor) throws UniqueViolationException, DAOException;

    public Optional<Cursor> getCursor(long rootId, long id) throws DAOException;

    public Optional<Cursor> deleteCursor(long rootId, long cursorId) throws DAOException;

    public void deleteCursorsCreatedBefore(Timestamp time) throws DAOException;

    public void setIdInitValue() throws DAOException;
}
