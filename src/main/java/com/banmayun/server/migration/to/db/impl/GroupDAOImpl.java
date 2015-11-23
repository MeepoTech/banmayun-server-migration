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
import com.banmayun.server.migration.to.core.Root;
import com.banmayun.server.migration.to.core.Root.RootType;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.GroupDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class GroupDAOImpl extends AbstractDAO implements GroupDAO {

    protected static final String TABLE_NAME = "groups";
    protected static final String TABLE_ALIAS = "_group_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "name", "source", "intro", "tags", "type",
            "is_visible", "is_activated", "is_blocked", "announce", "root_id", "created_at", "created_by", "is_promoted", "members_can_own", "user_count", "is_deleted"};
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_UPDATE;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length - 2));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_UPDATE = StringUtils.replace(
                DAOUtils.getColumnsUpdate(TABLE_ALIAS, ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length - 2)),
                "type=?", "type=?::group_type");
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Group parseGroup(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Group ret = new Group();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setName((String) arg.get(COLUMN_ALIASES[1]));
        ret.setSource((String) arg.get(COLUMN_ALIASES[2]));
        ret.setIntro((String) arg.get(COLUMN_ALIASES[3]));
        ret.setTags((String) arg.get(COLUMN_ALIASES[4]));
        ret.setType(Group.GroupType.valueOf(((String) arg.get(COLUMN_ALIASES[5]))));
        ret.setIsVisible((Boolean) arg.get(COLUMN_ALIASES[6]));
        ret.setIsActivated((Boolean) arg.get(COLUMN_ALIASES[7]));
        ret.setIsBlocked((Boolean) arg.get(COLUMN_ALIASES[8]));
        ret.setAnnounce((String) arg.get(COLUMN_ALIASES[9]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[10]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[11]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[12]));
        ret.setIsPromoted((Boolean) arg.get(COLUMN_ALIASES[13]));
        ret.setMembersCanOwn((Integer) arg.get(COLUMN_ALIASES[14]));
        ret.setUserCount((Integer) arg.get(COLUMN_ALIASES[15]));
        ret.setIsDeleted((Boolean) arg.get(COLUMN_ALIASES[16]));
        return ret;
    }

    public static List<Group> parseGroups(List<Map<String, Object>> arg) {
        List<Group> ret = new ArrayList<Group>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroup(map));
        }
        return ret;
    }

    public static Pair<Group, Root> parseGroupRoot(Map<String, Object> arg) {
        return Pair.of(parseGroup(arg), RootDAOImpl.parseRoot(arg));
    }

    public static List<Pair<Group, Root>> parseGroupRoots(List<Map<String, Object>> arg) {
        List<Pair<Group, Root>> ret = new ArrayList<Pair<Group, Root>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroupRoot(map));
        }
        return ret;
    }

    public GroupDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Group createGroup(Group group) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES "
                + "(?, ?, ?, ?, ?::group_type, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING %3$s", TABLE_NAME, COLUMNS_INSERT,
                COLUMNS_RETURN);
        return parseGroup(super.uniqueResult(sql, group.getName(), group.getSource(), group.getIntro(),
                group.getTags(), group.getType().toString(), group.getIsVisible(), group.getIsActivated(),
                group.getIsBlocked(), group.getAnnounce(), group.getRootId(), group.getCreatedAt(),
                group.getCreatedBy(), group.getIsPromoted(), group.getMembersCanOwn()));
    }

    @Override
    public Optional<Group> findGroupByName(String name, String source) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE lower(%2$s.name)=lower(?) "
                + "AND lower(%2$s.source)=lower(?) AND %2$s.is_deleted=false", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, name, source)));
    }

    @Override
    public Optional<Group> findGroupByRootId(long rootId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.is_deleted=false", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, rootId)));
    }

    @Override
    public Optional<Group> getGroup(long groupId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.id=? AND %2$s.is_deleted=false", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, groupId)));
    }

    @Override
    public int countGroups() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countGroups(GroupType type, Boolean isActivated, Boolean isBlocked, Boolean isPromoted, Boolean isVisible) throws DAOException {
        Pair<String, Object[]> sqlFormatAndArgs = this.getListSQLFormatAndArgs(
                "SELECT COUNT(*) AS count FROM %1$s %2$s", type, isActivated, isBlocked, isPromoted, isVisible);
        String sql = String.format(sqlFormatAndArgs.getLeft(), TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, sqlFormatAndArgs.getRight());
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Group> listGroups(int offset, int limit) throws DAOException {
        return this.listGroups(null, null, null, null, null, offset, limit);
    }

    @Override
    public List<Group> listGroups(GroupType type, Boolean isActivated, Boolean isBlocked, Boolean isPromoted, Boolean isVisible, int offset, int limit)
            throws DAOException {
        Pair<String, Object[]> sqlFormatAndArgs = this.getListSQLFormatAndArgs("SELECT %3$s FROM %1$s %2$s", type,
                isActivated, isBlocked, isPromoted, isVisible);
        String sql = String.format(sqlFormatAndArgs.getLeft() + " ORDER BY %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        Object[] args = ArrayUtils.addAll(sqlFormatAndArgs.getRight(), offset, limit);
        return parseGroups(super.list(sql, args));
    }

    @Override
    public List<Group> topGroupsByUserCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.user_count DESC, %2$s.id DESC "
                + "OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseGroups(super.list(sql, offset, limit));
    }

    @Override
    public List<Pair<Group, Root>> topGroupsByFileCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %4$s %5$s INNER JOIN %1$s %2$s ON %2$s.root_id=%5$s.id "
                + "WHERE %5$s.type=?::root_type ORDER BY %5$s.file_count DESC, %5$s.id DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS,
                RootDAOImpl.COLUMNS_SELECT);
        return parseGroupRoots(super.list(sql, RootType.GROUP.toString(), offset, limit));
    }

    @Override
    public List<Pair<Group, Root>> topGroupsByByteCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %4$s %5$s INNER JOIN %1$s %2$s ON %2$s.root_id=%5$s.id "
                + "WHERE %5$s.type=?::root_type ORDER BY %5$s.byte_count DESC, %5$s.id DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS,
                RootDAOImpl.COLUMNS_SELECT);
        return parseGroupRoots(super.list(sql, RootType.GROUP.toString(), offset, limit));
    }

    @Override
    public Optional<Group> updateGroup(long groupId, Group group) throws UniqueViolationException, DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.id=? AND %2$s.is_deleted=false RETURNING %4$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, group.getName(), group.getSource(),
                group.getIntro(), group.getTags(), group.getType().toString(), group.getIsVisible(),
                group.getIsActivated(), group.getIsBlocked(), group.getAnnounce(), group.getRootId(),
                group.getCreatedAt(), group.getCreatedBy(), group.getIsPromoted(), group.getMembersCanOwn(), groupId)));
    }
    
    @Override
    public Optional<Group> setIsPromoted(long groupId, boolean isPromoted) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET is_promoted =? "
                + "WHERE %2$s.id=? AND %2$s.is_deleted=false RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, isPromoted, groupId)));
    }
    
    @Override
    public Optional<Group> deleteGroup(long groupId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.id=? RETURNING %3$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_RETURN);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, groupId)));
    }

    @Override
    public Optional<Group> markGroupAsDeleted(long groupId) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET is_deleted=true WHERE %2$s.id=? RETURNING %3$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_RETURN);
        Group deletedGroup = parseGroup(super.uniqueResult(sql, groupId));

        if (deletedGroup != null) {
            // decrement group_count for users
            sql = String.format("WITH tmp AS (SELECT %2$s.user_id FROM %1$s %2$s WHERE %2$s.group_id=?) "
                    + "UPDATE %3$s %4$s SET group_count=%4$s.group_count-1 FROM tmp WHERE %4$s.id=tmp.user_id",
                    RelationDAOImpl.TABLE_NAME, RelationDAOImpl.TABLE_ALIAS, UserDAOImpl.TABLE_NAME, UserDAOImpl.TABLE_ALIAS);
            super.update(sql, groupId);

            // delete relations
            sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.group_id=?", RelationDAOImpl.TABLE_NAME, RelationDAOImpl.TABLE_ALIAS);
            super.update(sql, groupId);
        }
        return Optional.fromNullable(deletedGroup);
    }

    private Pair<String, Object[]> getListSQLFormatAndArgs(String sqlFormat, GroupType type, Boolean isActivated,
            Boolean isBlocked, Boolean isPromoted, Boolean isVisible) {
        List<Object> args = new ArrayList<Object>(3);
        boolean isWhere = true;
        if (type != null) {
            if (isWhere) {
                sqlFormat += " WHERE %2$s.type=?::group_type";
                isWhere = false;
            } else {
                sqlFormat += " AND %2$s.type=?::group_type";
            }
            args.add(type.toString());
        }
        if (isActivated != null) {
            if (isWhere) {
                sqlFormat += " WHERE %2$s.is_activated=?";
                isWhere = false;
            } else {
                sqlFormat += " AND %2$s.is_activated=?";
            }
            args.add(isActivated);
        }
        if (isBlocked != null) {
            if (isWhere) {
                sqlFormat += " WHERE %2$s.is_blocked=?";
                isWhere = false;
            } else {
                sqlFormat += " AND %2$s.is_blocked=?";
            }
            args.add(isBlocked);
        }
        if (isPromoted != null) {
            if (isWhere) {
                sqlFormat += " WHERE %2$s.is_promoted=?";
                isWhere = false;
            } else {
                sqlFormat += " AND %2$s.is_promoted=?";
            }
            args.add(isPromoted);
        }
        if (isVisible != null) {
            if (isWhere) {
                sqlFormat += " WHERE %2$s.is_visible=?";
                isWhere = false;
            } else {
                sqlFormat += " AND %2$s.is_visible=?";
            }
            args.add(isVisible);
        }
        
        if (isWhere) {
            sqlFormat += " WHERE %2$s.is_deleted=?";
            isWhere = false;
        } else {
            sqlFormat += " AND %2$s.is_deleted=?";
        }
        args.add(false);
        
        return Pair.of(sqlFormat, args.toArray(new Object[0]));
    }
}