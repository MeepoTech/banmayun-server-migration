package com.banmayun.server.migration.to.db;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Group;
import com.banmayun.server.migration.to.core.Group.GroupType;
import com.banmayun.server.migration.to.core.Relation;
import com.banmayun.server.migration.to.core.Relation.RelationRole;
import com.banmayun.server.migration.to.core.User;
import com.banmayun.server.migration.to.core.User.UserRole;
import com.google.common.base.Optional;

public interface RelationDAO {

    public Relation createRelation(Relation relation) throws UniqueViolationException, DAOException;

    public void addUsersForGroup(long groupId, Relation relation) throws DAOException;

    public void addUsersForGroup(long groupId, UserRole role, Relation relation) throws DAOException;

    public void addGroupsForUser(long userId, Relation relation) throws DAOException;

    public void addGroupsForUser(long userId, GroupType type, Relation relation) throws DAOException;

    public Optional<Relation> getRelation(long userId, long groupId) throws DAOException;

    public Optional<Pair<User, Relation>> getGroupUser(long groupId, long userId) throws DAOException;

    public Optional<Pair<Group, Relation>> getUserGroup(long userId, long groupId) throws DAOException;

    public int countRelations() throws DAOException;

    public int countUsersForGroup(long groupId) throws DAOException;

    public int countUsersForGroup(long groupId, RelationRole role) throws DAOException;

    public int countGroupsForUser(long userId) throws DAOException;

    public int countGroupsForUser(long userId, RelationRole role) throws DAOException;

    public List<Relation> listRelations(int offset, int limit) throws DAOException;

    public List<Relation> listRelations(RelationRole role, int offset, int limit) throws DAOException;

    public List<Pair<User, Relation>> listUsersForGroup(long groupId, int offset, int limit) throws DAOException;

    public List<Pair<User, Relation>> listUsersForGroup(long groupId, RelationRole role, int offset, int limit)
            throws DAOException;

    public List<Pair<Group, Relation>> listGroupsForUser(long userId, int offset, int limit) throws DAOException;

    public List<Pair<Group, Relation>> listGroupsForUser(long userId, RelationRole role, int offset, int limit)
            throws DAOException;

    public Optional<Relation> updateRelation(long userId, long groupId, Relation relation) throws DAOException;

    public Optional<Relation> deleteRelation(long userId, long groupId) throws DAOException;

    public void removeGroupsForUser(long userId) throws DAOException;

    public void removeUsersForGroup(long groupId) throws DAOException;

    public void updateGroupCount() throws DAOException;

    public void updateGroupCountForUser(long userId) throws DAOException;

    public void updateUserCount() throws DAOException;

    public void updateUserCountForGroup(long groupId) throws DAOException;
}
