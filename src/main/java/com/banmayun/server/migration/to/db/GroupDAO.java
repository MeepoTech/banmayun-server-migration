package com.banmayun.server.migration.to.db;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Group;
import com.banmayun.server.migration.to.core.Group.GroupType;
import com.banmayun.server.migration.to.core.Root;
import com.google.common.base.Optional;

public interface GroupDAO {

    public Group createGroup(Group group) throws UniqueViolationException, DAOException;

    public Optional<Group> findGroupByName(String name, String source) throws DAOException;

    public Optional<Group> findGroupByRootId(long rootId) throws DAOException;

    public Optional<Group> getGroup(long groupId) throws DAOException;

    public int countGroups() throws DAOException;

    public int countGroups(GroupType type, Boolean isActivated, Boolean isBlocked, Boolean isPromoted, Boolean isVisible) throws DAOException;

    public List<Group> listGroups(int offset, int limit) throws DAOException;

    public List<Group> listGroups(GroupType type, Boolean isActivated, Boolean isBlocked, Boolean isPromoted, Boolean isVisible, int offset, int limit)
            throws DAOException;

    public List<Group> topGroupsByUserCount(int offset, int limit) throws DAOException;

    public List<Pair<Group, Root>> topGroupsByFileCount(int offset, int limit) throws DAOException;

    public List<Pair<Group, Root>> topGroupsByByteCount(int offset, int limit) throws DAOException;

    public Optional<Group> updateGroup(long groupId, Group group) throws UniqueViolationException, DAOException;

    public Optional<Group> deleteGroup(long groupId) throws DAOException;
    
    public Optional<Group> setIsPromoted(long groupId, boolean isPromoted) throws DAOException;

    public Optional<Group> markGroupAsDeleted(long groupId) throws DAOException;
}
