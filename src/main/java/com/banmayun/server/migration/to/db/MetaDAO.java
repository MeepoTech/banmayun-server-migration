package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;
import java.util.List;

import com.banmayun.server.migration.to.core.Data;
import com.banmayun.server.migration.to.core.Meta;
import com.google.common.base.Optional;

public interface MetaDAO {

    public Meta createMeta(Meta meta) throws UniqueViolationException, DAOException;

    public Optional<Meta> findMetaByPath(long rootId, String path) throws DAOException;

    public Optional<Meta> getMeta(long rootId, long metaId) throws DAOException;

    public int countMetas() throws DAOException;

    public int countMetasForRoot(long rootId) throws DAOException;

    public int countFiles() throws DAOException;

    public int countFilesForRoot(long rootId) throws DAOException;

    public int countFolders() throws DAOException;

    public int countFoldersForRoot(long rootId) throws DAOException;

    public long countBytes() throws DAOException;

    public long countBytesForRoot(long rootId) throws DAOException;

    public List<Meta> listMetas(int offset, int limit) throws DAOException;

    public List<Meta> listMetasByPath(long rootId, String path) throws DAOException;

    public List<Meta> topMetasByCommentCount(int offset, int limit) throws DAOException;

    public List<Meta> topMetasByCommentCountForRoot(long rootId, int offset, int limit) throws DAOException;

    public List<Meta> topFilesByShareCount(int offset, int limit) throws DAOException;

    public List<Meta> topFilesByShareCountForRoot(long rootId, int offset, int limit) throws DAOException;

    public Optional<Meta> setMetaPermission(long rootId, long metaId, String permission) throws DAOException;

    public Optional<Meta> commitFileModification(long rootId, long metaId, long userId, Timestamp clientModifiedAt,
            Data data) throws DAOException;

    public Optional<Meta> utimeFolder(long rootId, long metaId, Timestamp clientModifiedAt) throws DAOException;

    public Optional<Meta> copyMetasRecursivelyByPath(long rootId, String path, String toPath, long userId)
            throws UniqueViolationException, DAOException;

    public Optional<Meta> moveMetasRecursivelyByPath(long rootId, String path, String toPath, long userId)
            throws UniqueViolationException, DAOException;

    public Optional<Meta> trashMetasRecursivelyByPath(long rootId, String path, long userId) throws DAOException;

    public Optional<Meta> restoreMetasRecursivelyByPath(long rootId, String path, long nonce, long userId)
            throws DAOException;

    public Optional<Meta> restoreMetasRecursivelyByPath(long rootId, String path, long nonce, String toPath, long userId)
            throws DAOException;

    public void deleteMetasTrashedBefore(Timestamp time) throws DAOException;

    public void updateRootFileCountAndByteCount() throws DAOException;

    public void updateRootFileCountAndByteCountForRoot(long rootId) throws DAOException;

    public List<Meta> deltaBetweenVersion(long rootId, long versionLow, long versionHigh, int offset, int limit)
            throws DAOException;

    public List<Meta> deltaAfterVersion(long rootId, long version, int offset, int limit) throws DAOException;

    public void setVersionInitValue() throws DAOException;
}
