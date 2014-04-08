package com.banmayun.server.migration.to.db;

import com.banmayun.server.migration.to.core.ChunkedUpload;
import com.google.common.base.Optional;

public interface ChunkedUploadDAO {

    public ChunkedUpload createChunkedUpload(ChunkedUpload chunkedUpload) throws DAOException;

    public Optional<ChunkedUpload> getChunkedUpload(long chunkedUploadId) throws DAOException;

    public Optional<ChunkedUpload> updateChunkedUpload(long chunkedUploadId, ChunkedUpload chunkedUpload)
            throws DAOException;

    public Optional<ChunkedUpload> deleteChunkedUpload(long chunkedUploadId) throws DAOException;
}
