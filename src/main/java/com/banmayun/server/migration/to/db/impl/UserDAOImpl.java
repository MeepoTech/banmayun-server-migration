package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Root;
import com.banmayun.server.migration.to.core.User;
import com.banmayun.server.migration.to.core.User.UserRole;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.banmayun.server.migration.to.db.UserDAO;
import com.google.common.base.Optional;

public class UserDAOImpl extends AbstractDAO implements UserDAO {

    protected static final String TABLE_NAME = "users";
    protected static final String TABLE_ALIAS = "_user_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "name", "email", "source", "display_name",
            "password_sha256", "role", "is_activated", "is_blocked", "groups_can_own", "root_id", "created_at",
            "group_count" };
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
                "role=?", "role=?::user_role");
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static User parseUser(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        User ret = new User();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setName((String) arg.get(COLUMN_ALIASES[1]));
        ret.setEmail((String) arg.get(COLUMN_ALIASES[2]));
        ret.setSource((String) arg.get(COLUMN_ALIASES[3]));
        ret.setDisplayName((String) arg.get(COLUMN_ALIASES[4]));
        ret.setPasswordSha256((String) arg.get(COLUMN_ALIASES[5]));
        ret.setRole(UserRole.valueOf(((String) arg.get(COLUMN_ALIASES[6]))));
        ret.setIsActivated((Boolean) arg.get(COLUMN_ALIASES[7]));
        ret.setIsBlocked((Boolean) arg.get(COLUMN_ALIASES[8]));
        ret.setGroupsCanOwn((Integer) arg.get(COLUMN_ALIASES[9]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[10]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[11]));
        ret.setGroupCount((Integer) arg.get(COLUMN_ALIASES[12]));
        return ret;
    }

    public static List<User> parseUsers(List<Map<String, Object>> arg) {
        List<User> ret = new ArrayList<User>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseUser(map));
        }
        return ret;
    }

    public static Pair<User, Root> parseUserRoot(Map<String, Object> arg) {
        return Pair.of(parseUser(arg), RootDAOImpl.parseRoot(arg));
    }

    public static List<Pair<User, Root>> parseUserRoots(List<Map<String, Object>> arg) {
        List<Pair<User, Root>> ret = new ArrayList<Pair<User, Root>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseUserRoot(map));
        }
        return ret;
    }

    public UserDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public User createUser(User user) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?, ?::user_role, ?, ?, ?, ?, ?) "
                + "RETURNING %3$s", TABLE_NAME, COLUMNS_INSERT, COLUMNS_RETURN);
        return parseUser(super.uniqueResult(sql, user.getName(), user.getEmail(), user.getSource(),
                user.getDisplayName(), user.getPasswordSha256(), user.getRole().toString(), user.getIsActivated(),
                user.getIsBlocked(), user.getGroupsCanOwn(), user.getRootId(), user.getCreatedAt()));
    }

    @Override
    public Optional<User> findUserByName(String name, String source) throws DAOException {
        if (source == null) {
            source = User.DEFAULT_SOURCE;
        }
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE lower(%2$s.name)=lower(?) "
                + "AND lower(%2$s.source)=lower(?)", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, name, source)));
    }

    @Override
    public Optional<User> findUserByEmail(String email, String source) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE lower(%2$s.email)=lower(?) "
                + "AND lower(%2$s.source)=lower(?)", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, email, source)));
    }

    @Override
    public Optional<User> findUserByRootId(long rootId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, rootId)));
    }

    @Override
    public Optional<User> getUser(long id) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.id=?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, id)));
    }

    @Override
    public int countUsers() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countUsers(UserRole role) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count WHERE %2$s.role=?::user_role FROM %1$s %2$s", TABLE_NAME,
                TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, role.toString());
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<User> listUsers(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        return parseUsers(super.list(sql, offset, limit));
    }

    @Override
    public List<User> listUsers(UserRole role, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.role=?::user_role "
                + "ORDER BY %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseUsers(super.list(sql, role.toString(), offset, limit));
    }

    @Override
    public List<User> topUsersByGroupCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.group_count DESC, %2$s.id DESC "
                + "OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseUsers(super.list(sql, offset, limit));
    }

    @Override
    public List<Pair<User, Root>> topUsersByFileCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %4$s %5$s LEFT JOIN %1$s %2$s ON %2$s.root_id=%5$s.id "
                + "ORDER BY %5$s.file_count DESC, %5$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS, RootDAOImpl.COLUMNS_SELECT);
        return parseUserRoots(super.list(sql, offset, limit));
    }

    @Override
    public List<Pair<User, Root>> topUsersByByteCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %4$s %5$s LEFT JOIN %1$s %2$s ON %2$s.root_id=%5$s.id "
                + "ORDER BY %5$s.byte_count DESC, %5$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS, RootDAOImpl.COLUMNS_SELECT);
        return parseUserRoots(super.list(sql, offset, limit));
    }

    @Override
    public Optional<User> updateUser(long id, User user) throws UniqueViolationException, DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.id=? RETURNING %4$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, user.getName(), user.getEmail(),
                user.getSource(), user.getDisplayName(), user.getPasswordSha256(), user.getRole().toString(),
                user.getIsActivated(), user.getIsBlocked(), user.getGroupsCanOwn(), user.getRootId(),
                user.getCreatedAt(), id)));
    }
}
