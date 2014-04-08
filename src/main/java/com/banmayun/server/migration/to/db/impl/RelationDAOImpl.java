package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Group;
import com.banmayun.server.migration.to.core.Group.GroupType;
import com.banmayun.server.migration.to.core.Relation;
import com.banmayun.server.migration.to.core.Relation.RelationRole;
import com.banmayun.server.migration.to.core.User;
import com.banmayun.server.migration.to.core.User.UserRole;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.RelationDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class RelationDAOImpl extends AbstractDAO implements RelationDAO {

    protected static final String TABLE_NAME = "relations";
    protected static final String TABLE_ALIAS = "_relation_";
    protected static final String[] COLUMN_NAMES = new String[] { "user_id", "group_id", "remarks", "role",
            "is_activated", "is_blocked", "created_at" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_UPDATE;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_UPDATE = StringUtils.replace(
                DAOUtils.getColumnsUpdate(TABLE_ALIAS, ArrayUtils.subarray(COLUMN_NAMES, 2, COLUMN_NAMES.length)),
                "role=?", "role=?::relation_role");
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Relation parseRelation(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        if (arg.get(COLUMN_ALIASES[0]) == null) {
            return null;
        }
        Relation ret = new Relation();
        ret.setUserId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRemarks((String) arg.get(COLUMN_ALIASES[2]));
        ret.setRole(Relation.RelationRole.valueOf(((String) arg.get(COLUMN_ALIASES[3]))));
        ret.setIsActivated((Boolean) arg.get(COLUMN_ALIASES[4]));
        ret.setIsBlocked((Boolean) arg.get(COLUMN_ALIASES[5]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[6]));
        return ret;
    }

    public static List<Relation> parseRelations(List<Map<String, Object>> arg) {
        List<Relation> ret = new ArrayList<Relation>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseRelation(map));
        }
        return ret;
    }

    public static Pair<User, Relation> parseUserRelation(Map<String, Object> arg) {
        return Pair.of(UserDAOImpl.parseUser(arg), parseRelation(arg));
    }

    public static List<Pair<User, Relation>> parseUserRelations(List<Map<String, Object>> arg) {
        List<Pair<User, Relation>> ret = new ArrayList<Pair<User, Relation>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseUserRelation(map));
        }
        return ret;
    }

    public static Pair<Group, Relation> parseGroupRelation(Map<String, Object> arg) {
        return Pair.of(GroupDAOImpl.parseGroup(arg), parseRelation(arg));
    }

    public static List<Pair<Group, Relation>> parseGroupRelations(List<Map<String, Object>> arg) {
        List<Pair<Group, Relation>> ret = new ArrayList<Pair<Group, Relation>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroupRelation(map));
        }
        return ret;
    }

    public RelationDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Relation createRelation(Relation relation) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?::relation_role, ?, ?, ?) "
                + "RETURNING %3$s", TABLE_NAME, COLUMNS_INSERT, COLUMNS_RETURN);
        Relation createdRelation = parseRelation(super.uniqueResult(sql, relation.getUserId(), relation.getGroupId(),
                relation.getRemarks(), relation.getRole().toString(), relation.getIsActivated(),
                relation.getIsBlocked(), relation.getCreatedAt()));

        // increment group_count for user
        sql = String.format("UPDATE %1$s %2$s SET group_count=%2$s.group_count+1 WHERE %2$s.id=?",
                UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS);
        super.update(sql, createdRelation.getUserId());

        // increment user_count for group
        sql = String.format("UPDATE %1$s %2$s SET user_count=%2$s.user_count+1 WHERE %2$s.id=?",
                GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS);
        super.update(sql, createdRelation.getGroupId());

        return createdRelation;
    }

    @Override
    public void addUsersForGroup(long groupId, Relation relation) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%3$s) SELECT %5$s.id, ?, ?, ?::relation_role, ?, ?, ? "
                + "FROM %4$s %5$s WHERE %5$s.id NOT IN (SELECT %2$s.user_id FROM %1$s %2$s WHERE %2$s.group_id=?)",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_INSERT, UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS);
        super.update(sql, groupId, relation.getRemarks(), relation.getRole().toString(), relation.getIsActivated(),
                relation.getIsBlocked(), relation.getCreatedAt(), groupId);
    }

    @Override
    public void addUsersForGroup(long groupId, UserRole role, Relation relation) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%3$s) SELECT %5$s.id, ?, ?, ?::relation_role, ?, ?, ? "
                + "FROM %4$s %5$s WHERE %5$s.id NOT IN (SELECT %2$s.user_id FROM %1$s %2$s WHERE %2$s.group_id=?) "
                + "AND %5$s.role=?::user_role", TABLE_NAME, TABLE_ALIAS, COLUMNS_INSERT, UserDAOImpl.TABLE_NAME,
                UserDAOImpl.TABLE_ALIAS);
        super.update(sql, groupId, relation.getRemarks(), relation.getRole().toString(), relation.getIsActivated(),
                relation.getIsBlocked(), relation.getCreatedAt(), groupId, role.toString());
    }

    @Override
    public void addGroupsForUser(long userId, Relation relation) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%3$s) SELECT ?, %5$s.id, ?, ?::relation_role, ?, ?, ? "
                + "FROM %4$s %5$s WHERE %5$s.id NOT IN (SELECT %2$s.group_id FROM %1$s %2$s WHERE %2$s.user_id=?)",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_INSERT, GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS);
        super.update(sql, userId, relation.getRemarks(), relation.getRole().toString(), relation.getIsActivated(),
                relation.getIsBlocked(), relation.getCreatedAt(), userId);
    }

    @Override
    public void addGroupsForUser(long userId, GroupType type, Relation relation) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%3$s) SELECT ?, %5$s.id, ?, ?::relation_role, ?, ?, ? "
                + "FROM %4$s %5$s WHERE %5$s.id NOT IN (SELECT %2$s.group_id FROM %1$s %2$s WHERE %2$s.user_id=?) "
                + "AND %5$s.type=?::group_type", TABLE_NAME, TABLE_ALIAS, COLUMNS_INSERT, GroupDAOImpl.TABLE_NAME,
                GroupDAOImpl.TABLE_ALIAS);
        super.update(sql, userId, relation.getRemarks(), relation.getRole().toString(), relation.getIsActivated(),
                relation.getIsBlocked(), relation.getCreatedAt(), userId, type.toString());
    }

    @Override
    public Optional<Relation> getRelation(long userId, long groupId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.user_id=? AND %2$s.group_id=?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        Map<String, Object> map = super.uniqueResult(sql, userId, groupId);
        return Optional.fromNullable(parseRelation(map));
    }

    @Override
    public Optional<Pair<User, Relation>> getGroupUser(long groupId, long userId) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.user_id "
                + "WHERE %2$s.group_id=? AND %2$s.user_id=?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT,
                UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS, UserDAOImpl.COLUMNS_SELECT);
        return Optional.fromNullable(parseUserRelation(super.uniqueResult(sql, groupId, userId)));
    }

    @Override
    public Optional<Pair<Group, Relation>> getUserGroup(long userId, long groupId) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.user_id "
                + "WHERE %2$s.user_id=? AND %2$s.group_id=?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT,
                GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS, GroupDAOImpl.COLUMNS_SELECT);
        return Optional.fromNullable(parseGroupRelation(super.uniqueResult(sql, userId, groupId)));
    }

    @Override
    public int countRelations() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countUsersForGroup(long groupId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.group_id=?", TABLE_NAME,
                TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, groupId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countUsersForGroup(long groupId, RelationRole role) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.group_id=? "
                + "AND %2$s.role=?::relation_role", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, groupId, role.toString());
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countGroupsForUser(long userId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.user_id=?", TABLE_NAME,
                TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, userId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countGroupsForUser(long userId, RelationRole role) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.user_id=? "
                + "AND %2$s.role=?::relation_role", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, userId, role.toString());
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Relation> listRelations(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.user_id DESC, %2$s.group_id DESC "
                + "OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseRelations(super.list(sql, offset, limit));
    }

    @Override
    public List<Relation> listRelations(RelationRole role, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.role=?::relation_role "
                + "ORDER BY %2$s.user_id DESC, %2$s.group_id DESC " + "OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return parseRelations(super.list(sql, role.toString(), offset, limit));
    }

    @Override
    public List<Pair<User, Relation>> listUsersForGroup(long groupId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.user_id "
                + "WHERE %2$s.group_id=? ORDER BY %2$s.user_id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT, UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS, UserDAOImpl.COLUMNS_SELECT);
        return parseUserRelations(super.list(sql, groupId, offset, limit));
    }

    @Override
    public List<Pair<User, Relation>> listUsersForGroup(long groupId, RelationRole role, int offset, int limit)
            throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.user_id "
                + "WHERE %2$s.group_id=? AND %2$s.role=?::relation_role ORDER BY %2$s.user_id DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS,
                UserDAOImpl.COLUMNS_SELECT);
        return parseUserRelations(super.list(sql, groupId, role.toString(), offset, limit));
    }

    @Override
    public List<Pair<Group, Relation>> listGroupsForUser(long userId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.group_id "
                + "WHERE %2$s.user_id=? ORDER BY %2$s.group_id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT, GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS, GroupDAOImpl.COLUMNS_SELECT);
        return parseGroupRelations(super.list(sql, userId, offset, limit));
    }

    @Override
    public List<Pair<Group, Relation>> listGroupsForUser(long userId, RelationRole role, int offset, int limit)
            throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.group_id "
                + "WHERE %2$s.user_id=? AND %2$s.role=?::relation_role ORDER BY %2$s.group_id DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS,
                GroupDAOImpl.COLUMNS_SELECT);
        return parseGroupRelations(super.list(sql, role.toString(), userId, offset, limit));
    }

    @Override
    public Optional<Relation> updateRelation(long userId, long groupId, Relation relation) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.user_id=? AND %2$s.group_id=? RETURNING %4$s",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional.fromNullable(parseRelation(super.uniqueResult(sql, relation.getRemarks(), relation.getRole()
                .toString(), relation.getIsActivated(), relation.getIsBlocked(), relation.getCreatedAt(), userId,
                groupId)));
    }

    @Override
    public Optional<Relation> deleteRelation(long userId, long groupId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.user_id=? AND %2$s.group_id=? RETURNING %3$s",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        Relation deletedRelation = parseRelation(super.uniqueResult(sql, userId, groupId));
        if (deletedRelation == null) {
            return Optional.absent();
        }

        // decrement group_count for user
        sql = String.format("UPDATE %1$s %2$s SET group_count=%2$s.group_count-1 WHERE %2$s.id=?",
                UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS);
        super.update(sql, deletedRelation.getUserId());

        // decrement user_count for group
        sql = String.format("UPDATE %1$s %2$s SET user_count=%2$s.user_count-1 WHERE %2$s.id=?",
                GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS);
        super.update(sql, deletedRelation.getGroupId());

        return Optional.of(deletedRelation);
    }

    @Override
    public void removeGroupsForUser(long userId) throws DAOException {
        // decrement user_count for groups
        String sql = String.format("WITH tmp AS (SELECT %2$s.group_id FROM %1$s %2$s WHERE %2$s.user_id=?) "
                + "UPDATE %3$s %4$s SET user_count=%4$s.user_count-1 FROM tmp WHERE %4$s.id=tmp.group_id", TABLE_NAME,
                TABLE_ALIAS, GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS);
        super.update(sql, userId);

        // delete relations
        sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.user_id=?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, userId);
    }

    @Override
    public void removeUsersForGroup(long groupId) throws DAOException {
        // decrement group_count for users
        String sql = String.format("WITH tmp AS (SELECT %2$s.user_id FROM %1$s %2$s WHERE %2$s.group_id=?) "
                + "UPDATE %3$s %4$s SET group_count=%4$s.group_count-1 FROM tmp WHERE %4$s.id=tmp.user_id", TABLE_NAME,
                TABLE_ALIAS, UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS);
        super.update(sql, groupId);

        // delete relations
        sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.group_id=?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, groupId);
    }

    @Override
    public void updateGroupCount() throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.user_id=%4$s.id GROUP BY %4$s.id) UPDATE %3$s %4$s "
                + "SET group_count=tmp.count FROM tmp WHERE %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS,
                UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS);
        super.update(sql);
    }

    @Override
    public void updateGroupCountForUser(long userId) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s WHERE %4$s.id=? "
                + "LEFT JOIN %1$s %2$s ON %2$s.user_id=%4$s.id GROUP BY %4$s.id) UPDATE %3$s %4$s "
                + "SET group_count=tmp.count FROM tmp WHERE %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS,
                UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS);
        super.update(sql, userId);
    }

    @Override
    public void updateUserCount() throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.group_id=%4$s.id GROUP BY %4$s.id) UPDATE %3$s %4$s "
                + "SET user_count=tmp.count FROM tmp WHERE %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS,
                GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS);
        super.update(sql);
    }

    @Override
    public void updateUserCountForGroup(long groupId) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s WHERE %4$s.id=? "
                + "LEFT JOIN %1$s %2$s ON %2$s.group_id=%4$s.id GROUP BY %4$s.id) UPDATE %3$s %4$s "
                + "SET user_count=tmp.count FROM tmp WHERE %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS,
                GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS);
        super.update(sql, groupId);
    }
}
