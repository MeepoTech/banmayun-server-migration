package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Meta;
import com.banmayun.server.migration.to.core.Trash;
import com.google.common.base.Optional;

public interface TrashDAO {

    public Trash createTrash(Trash trash) throws UniqueViolationException, DAOException;

    public Pair<Trash, Meta> getTrash(long rootId, long trashId) throws DAOException;

    public int countTrashesForRoot(long rootId) throws DAOException;

    public List<Pair<Trash, Meta>> listTrashesForRoot(long rootId, int offset, int limit) throws DAOException;

    public Optional<Trash> deleteTrash(long rootId, long trashId) throws DAOException;

    public void deleteTrashesForRoot(long rootId) throws DAOException;

    public void undeleteTrashesForRoot(long rootId) throws DAOException;

    public void deleteTrashesCreatedBefore(Timestamp time) throws DAOException;
}
