package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Meta;
import com.banmayun.server.migration.to.core.Share;
import com.google.common.base.Optional;

public interface ShareDAO {

    public Share createShare(Share share) throws DAOException;

    public Optional<Share> getShare(long rootId, long metaId, long shareId) throws DAOException;

    public Optional<Share> getShareById(long shareId) throws DAOException;

    public int countShares() throws DAOException;

    public int countSharesForRoot(long rootId) throws DAOException;

    public int countSharesForMeta(long rootId, long metaId) throws DAOException;

    public List<Pair<Share, Meta>> listShares(int offset, int limit) throws DAOException;

    public List<Pair<Share, Meta>> listSharesForRoot(long rootId, int offset, int limit) throws DAOException;

    public List<Pair<Share, Meta>> listSharesForMeta(long rootId, long metaId, int offset, int limit)
            throws DAOException;

    public Optional<Share> updateShare(long rootId, long metaId, long shareId, Share share) throws DAOException;

    public Optional<Share> deleteShare(long rootId, long metaId, long shareId) throws DAOException;

    public void deleteShares() throws DAOException;

    public void deleteSharesForRoot(long rootId) throws DAOException;

    public void deleteSharesForMeta(long rootId, long metaId) throws DAOException;

    public void deleteSharesExpiresBefore(Timestamp time) throws DAOException;

    public void updateMetaShareCount() throws DAOException;

    public void updateMetaShareCountForRoot(long rootId) throws DAOException;

    public void updateMetaShareCountForMeta(long rootId, long metaId) throws DAOException;

    public void setIdInitValue() throws DAOException;
}
