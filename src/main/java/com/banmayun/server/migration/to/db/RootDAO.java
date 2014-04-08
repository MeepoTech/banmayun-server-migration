package com.banmayun.server.migration.to.db;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Root;
import com.google.common.base.Optional;

public interface RootDAO {

    public Root createRoot(Root root) throws UniqueViolationException, DAOException;

    public Optional<Root> getRoot(long id) throws DAOException;

    public Pair<Integer, Long> countFilesAndBytes() throws DAOException;

    public Optional<Root> addRootQuota(long id, long quota) throws DAOException;

    public Optional<Root> setRootQuota(long id, long quota) throws DAOException;

    public Optional<Root> setRootDefaultPermission(long id, String defaultPermission) throws DAOException;

    public Optional<Root> deleteRoot(long id) throws DAOException;
}
