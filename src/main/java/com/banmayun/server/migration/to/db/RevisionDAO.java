package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;
import java.util.List;

import com.banmayun.server.migration.to.core.Revision;
import com.google.common.base.Optional;

public interface RevisionDAO {

    public Revision createRevision(Revision revision) throws UniqueViolationException, DAOException;

    public Optional<Revision> getRevision(long rootId, long metaId, long version) throws DAOException;

    public int countRevisionsForMeta(long rootId, long metaId) throws DAOException;

    public List<Revision> listRevisionsForMeta(long rootId, long metaId, int offset, int limit) throws DAOException;

    public Optional<Revision> deleteRevision(long rootId, long metaId, long version) throws DAOException;

    public void deleteRevisionsModifiedBefore(Timestamp time) throws DAOException;
}
