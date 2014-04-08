/* ****************************************************************************
 * MEEPOTECH CONFIDENTIAL
 * ----------------------
 * [2013] - [2014] MeePo Technology Incorporated
 * All Rights Reserved.
 *
 * IMPORTANT NOTICE:
 * All information contained herein is, and remains the property of MeePo
 * Technology Incorporated and its suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to MeePo Technology
 * Incorporated and its suppliers and may be covered by Chinese and Foreign
 * Patents, patents in process, and are protected by trade secret or copyright
 * law. Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained from
 * MeePo Technology Incorporated.
 * ****************************************************************************
 */

package com.banmayun.server.migration.from.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.from.core.Relation;
import com.banmayun.server.migration.from.core.User;
import com.google.common.base.Optional;

public class UserDAO extends AbstractDAO {

    public static final long ROOT_ID = 1L;
    public static final String DEFAULT_DOMAIN = "local";

    protected static final String[] COLUMN_NAMES = new String[] { "id", "name", "email", "full_name", "password",
            "role", "groups_can_own", "created", "domain" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    protected static String COLUMNS_COUNT;
    static {
        String tableAlias = "u";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = StringUtils.replace(COLUMNS_UPDATE, "role=?", "role=?::user_role");
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static User parseUser(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        User ret = new User();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setName((String) arg.get(COLUMN_ALIASES[1]));
        ret.setEmail((String) arg.get(COLUMN_ALIASES[2]));
        ret.setFullName((String) arg.get(COLUMN_ALIASES[3]));
        ret.setPassword((String) arg.get(COLUMN_ALIASES[4]));
        ret.setRole(User.Role.valueOf(((String) arg.get(COLUMN_ALIASES[5])).toUpperCase()));
        ret.setGroupsCanOwn((Integer) arg.get(COLUMN_ALIASES[6]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[7]));
        ret.setDomain((String) arg.get(COLUMN_ALIASES[8]));
        return ret;
    }

    public static List<User> parseUsers(List<Map<String, Object>> arg) {
        List<User> ret = new ArrayList<User>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseUser(map));
        }
        return ret;
    }

    public UserDAO(Connection conn) {
        super(conn);
    }

    public User create(User user) throws SQLException {
        String sql = "INSERT INTO tbl_user (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?::user_role, ?, ?, ?) "
                + "RETURNING " + COLUMNS_RETURN;
        return parseUser(super.uniqueResult(sql, user.getName(), user.getEmail(), user.getFullName(),
                user.getPassword(), user.getRole().toString().toLowerCase(), user.getGroupsCanOwn(), user.getCreated(),
                user.getDomain()));
    }

    public Optional<User> get(long id) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_user u " + "WHERE u.id=?";
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, id)));
    }

    public Optional<User> getByName(String name) throws SQLException {
        return getByNameAndDomain(name, DEFAULT_DOMAIN);
    }

    public Optional<User> getByNameAndDomain(String name, String domain) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_user u "
                + "WHERE lower(u.name)=lower(?) and lower(u.domain)=lower(?)";
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, name, domain)));
    }

    public Optional<User> getByEmail(String email) throws SQLException {
        return getByEmailAndDomain(email, DEFAULT_DOMAIN);
    }

    public Optional<User> getByEmailAndDomain(String email, String domain) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_user u "
                + "WHERE lower(u.email)=lower(?) and lower(u.domain)=lower(?)";
        return Optional.fromNullable(parseUser(super.uniqueResult(sql, email, domain)));
    }

    public int count() throws SQLException {
        return this.count(null);
    }

    public int count(Filter[] filters) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_user u " + getFilterWhereClause(filters);
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    public List<User> list(int offset, int limit) throws SQLException {
        return this.list(null, offset, limit);
    }

    public List<User> list(Filter[] filters, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_user u " + getFilterWhereClause(filters)
                + "ORDER BY u.id OFFSET ? LIMIT ?";
        return parseUsers(super.list(sql, offset, limit));
    }

    public List<User> listAndCount(int offset, int limit) throws SQLException {
        return this.list(null, offset, limit);
    }

    public Pair<Integer, List<User>> listAndCount(Filter[] filters, int offset, int limit) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_user u " + getFilterWhereClause(filters);
        Map<String, Object> ret = super.uniqueResult(sql);
        Integer count = ((Long) ret.get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_user u " + getFilterWhereClause(filters)
                + "ORDER BY u.created DESC OFFSET ? LIMIT ?";
        List<User> users = parseUsers(super.list(sql, offset, limit));

        return Pair.of(count, users);
    }

    public List<Pair<User, Relation>> listFetchRelation(Connection conn, long groupId, int offset, int limit)
            throws SQLException {
        return this.listFetchRelation(groupId, null, offset, limit);
    }

    public List<Pair<User, Relation>> listFetchRelation(long groupId, Filter[] filters, int offset, int limit)
            throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_user u "
                + "LEFT JOIN tbl_relation r ON r.user_id=u.id AND r.group_id=? " + getFilterWhereClause(filters)
                + "ORDER BY u.created DESC OFFSET ? LIMIT ?";
        return RelationDAO.parseUserRelations(super.list(sql, groupId, offset, limit));
    }

    protected static final String SEARCH_CLAUSE = "WHERE " + "(lower(u.name) LIKE lower(?) OR "
            + "lower(u.email) LIKE lower(?) OR " + "to_tsvector('chinese', coalesce(lower(u.full_name), '')) "
            + "@@ plainto_tsquery('chinese', lower(?))) ";

    public List<User> search(String query, int offset, int limit) throws SQLException {
        return this.search(query, null, offset, limit);
    }

    public List<User> search(String query, Filter[] filters, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_user u " + SEARCH_CLAUSE + getFilterAndClause(filters)
                + "OFFSET ? LIMIT ?";
        return parseUsers(super.list(sql, query + "%", query + "%", query, offset, limit));
    }

    public Pair<Integer, List<User>> searchAndCount(String query, Filter[] filters, int offset, int limit)
            throws SQLException {
        String whereClause = SEARCH_CLAUSE + getFilterAndClause(filters);
        String sql = "SELECT COUNT(*) FROM tbl_user u " + whereClause;
        Integer count = ((Long) super.uniqueResult(sql, query + "%", query + "%", query).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_user u " + whereClause + "OFFSET ? LIMIT ?";
        List<User> users = parseUsers(super.list(sql, query + "%", query + "%", query, offset, limit));
        return Pair.of(count, users);
    }

    public List<Pair<User, Relation>> searchFetchRelation(String query, long groupId, int offset, int limit)
            throws SQLException {
        return this.searchFetchRelation(query, groupId, null, offset, limit);
    }

    public List<Pair<User, Relation>> searchFetchRelation(String query, long groupId, Filter[] filters, int offset,
            int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_user u "
                + "LEFT JOIN tbl_relation r ON r.user_id=u.id AND r.group_id=? " + SEARCH_CLAUSE
                + getFilterAndClause(filters) + "OFFSET ? LIMIT ?";
        return RelationDAO.parseUserRelations(super.list(sql, groupId, query + "%", query + "%", query, offset, limit));
    }

    public List<Pair<User, Relation>> searchByGroupId(long groupId, String query, int offset, int limit)
            throws SQLException {
        return this.searchByGroupId(groupId, query, null, offset, limit);
    }

    public List<Pair<User, Relation>> searchByGroupId(long groupId, String query, Filter[] filters, int offset,
            int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_user u "
                + "INNER JOIN tbl_relation r ON r.user_id=u.id AND r.group_id=? "
                + RelationDAO.getFilterAndClause(filters) + SEARCH_CLAUSE + getFilterAndClause(filters)
                + "OFFSET ? LIMIT ?";
        return RelationDAO.parseUserRelations(super.list(sql, groupId, query + "%", query + "%", query, offset, limit));
    }

    public Pair<Integer, List<Pair<User, Relation>>> searchAndCountByGroupId(long groupId, String query,
            Filter[] filters, int offset, int limit) throws SQLException {
        String sql = "SELECT count(*) as count " + " FROM tbl_user u "
                + "INNER JOIN tbl_relation r ON r.user_id=u.id AND r.group_id=? "
                + RelationDAO.getFilterAndClause(filters) + SEARCH_CLAUSE + getFilterAndClause(filters);
        Integer count = ((Long) super.uniqueResult(sql, groupId, query + "%", query + "%", query).get("count"))
                .intValue();

        sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_user u "
                + "INNER JOIN tbl_relation r ON r.user_id=u.id AND r.group_id=? "
                + RelationDAO.getFilterAndClause(filters) + SEARCH_CLAUSE + getFilterAndClause(filters)
                + "OFFSET ? LIMIT ?";
        List<Pair<User, Relation>> userRelations = RelationDAO.parseUserRelations(super.list(sql, groupId, query + "%",
                query + "%", query, offset, limit));
        return Pair.of(count, userRelations);
    }

    public Optional<User> update(long id, User update) throws SQLException {
        User user = this.get(id).orNull();
        if (user == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_user u SET " + COLUMNS_UPDATE + " WHERE u.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseUser(super.uniqueResult(sql,
                update.getId() == null ? user.getId() : update.getId(), update.getName() == null ? user.getName()
                        : update.getName(), update.getEmail() == null ? user.getEmail() : update.getEmail(), update
                        .getFullName() == null ? user.getFullName() : update.getFullName(),
                update.getPassword() == null ? user.getPassword() : update.getPassword(),
                update.getRole() == null ? user.getRole().toString().toLowerCase() : update.getRole().toString()
                        .toLowerCase(),
                update.getGroupsCanOwn() == null ? user.getGroupsCanOwn() : update.getGroupsCanOwn(), update
                        .getCreated() == null ? user.getCreated() : update.getCreated(),
                update.getDomain() == null ? user.getDomain() : update.getDomain(), id)));
    }

    protected static String getFilterSQL(Filter[] filters) {
        Set<Filter.Field> fields = new HashSet<Filter.Field>();
        fields.add(Filter.Field.USER_ROLE);
        return Filter.getSQL(filters, fields);
    }

    protected static String getFilterWhereClause(Filter[] filters) {
        String filterSQL = getFilterSQL(filters);
        String ret = "";
        if (filterSQL.length() > 0) {
            ret = "WHERE " + filterSQL;
        }
        return ret;
    }

    protected static String getFilterAndClause(Filter[] filters) {
        String filterSQL = getFilterSQL(filters);
        String ret = "";
        if (filterSQL.length() > 0) {
            ret = "AND " + filterSQL;
        }
        return ret;
    }
}
