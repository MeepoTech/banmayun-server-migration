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
            "is_visible", "is_activated", "is_blocked", "announce", "root_id", "created_at", "created_by", "user_count" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_UPDATE;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length - 1));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_UPDATE = StringUtils.replace(
                DAOUtils.getColumnsUpdate(TABLE_ALIAS, ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length - 1)),
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
        ret.setUserCount((Integer) arg.get(COLUMN_ALIASES[13]));
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
                + "(?, ?, ?, ?, ?::group_type, ?, ?, ?, ?, ?, ?, ?) RETURNING %3$s", TABLE_NAME, COLUMNS_INSERT,
                COLUMNS_RETURN);
        return parseGroup(super.uniqueResult(sql, group.getName(), group.getSource(), group.getIntro(),
                group.getTags(), group.getType().toString(), group.getIsVisible(), group.getIsActivated(),
                group.getIsBlocked(), group.getAnnounce(), group.getRootId(), group.getCreatedAt(),
                group.getCreatedBy()));
    }

    @Override
    public Optional<Group> findGroupByName(String name, String source) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE lower(%2$s.name)=lower(?) "
                + "AND lower(%2$s.source)=lower(?)", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, name, source)));
    }

    @Override
    public Optional<Group> findGroupByRootId(long rootId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, rootId)));
    }

    @Override
    public Optional<Group> getGroup(long groupId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.id=?", TABLE_NAME, TABLE_ALIAS,
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
    public int countGroups(GroupType type) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.type=?::group_type ",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, type.toString());
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Group> listGroups(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        return parseGroups(super.list(sql, offset, limit));
    }

    @Override
    public List<Group> listGroups(GroupType type, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.type=?::group_type "
                + "ORDER BY %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseGroups(super.list(sql, type.toString(), offset, limit));
    }

    @Override
    public List<Group> topGroupsByUserCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.user_count DESC, %2$s.id DESC "
                + "OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseGroups(super.list(sql, limit));
    }

    @Override
    public List<Pair<Group, Root>> topGroupsByFileCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %4$s %5$s LEFT JOIN %1$s %2$s ON %2$s.root_id=%5$s.id "
                + "WHERE %5$s.type=?::root_type ORDER BY %5$s.file_count DESC, %5$s.id DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS,
                RootDAOImpl.COLUMNS_SELECT);
        return parseGroupRoots(super.list(sql, RootType.GROUP, offset, limit));
    }

    @Override
    public List<Pair<Group, Root>> topGroupsByByteCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %4$s %5$s LEFT JOIN %1$s %2$s ON %2$s.root_id=%5$s.id "
                + "WHERE %5$s.type=?::root_type ORDER BY %5$s.byte_count DESC, %5$s.id DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS,
                RootDAOImpl.COLUMNS_SELECT);
        return parseGroupRoots(super.list(sql, RootType.GROUP, offset, limit));
    }

    @Override
    public Optional<Group> updateGroup(long groupId, Group group) throws UniqueViolationException, DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.id=? RETURNING %4$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, group.getName(), group.getSource(),
                group.getIntro(), group.getTags(), group.getType().toString(), group.getIsVisible(),
                group.getIsActivated(), group.getIsBlocked(), group.getAnnounce(), group.getRootId(),
                group.getCreatedAt(), group.getCreatedBy(), groupId)));
    }

    @Override
    public Optional<Group> deleteGroup(long groupId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.id=? RETURNING %3$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_RETURN);
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, groupId)));
    }
}
