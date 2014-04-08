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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.from.core.Group;
import com.banmayun.server.migration.from.core.Relation;
import com.banmayun.server.migration.from.core.User;
import com.google.common.base.Optional;

public class RelationDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "user_id", "group_id", "remarks", "role", "created" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "r";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = StringUtils.replace(COLUMNS_UPDATE, "role=?", "role=?::relation_role");
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
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
        ret.setRole(Relation.Role.valueOf(((String) arg.get(COLUMN_ALIASES[3])).toUpperCase()));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[4]));
        return ret;
    }

    public static Pair<User, Relation> parseUserRelation(Map<String, Object> arg) {
        return Pair.of(UserDAO.parseUser(arg), parseRelation(arg));
    }

    public static Pair<Group, Relation> parseGroupRelation(Map<String, Object> arg) {
        return Pair.of(GroupDAO.parseGroup(arg), parseRelation(arg));
    }

    public static List<Relation> parseRelations(List<Map<String, Object>> arg) {
        List<Relation> ret = new ArrayList<Relation>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseRelation(map));
        }
        return ret;
    }

    public static List<Pair<User, Relation>> parseUserRelations(List<Map<String, Object>> arg) {
        List<Pair<User, Relation>> ret = new ArrayList<Pair<User, Relation>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseUserRelation(map));
        }
        return ret;
    }

    public static List<Pair<Group, Relation>> parseGroupRelations(List<Map<String, Object>> arg) {
        List<Pair<Group, Relation>> ret = new ArrayList<Pair<Group, Relation>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroupRelation(map));
        }
        return ret;
    }

    public RelationDAO(Connection conn) {
        super(conn);
    }

    public Relation create(Relation relation) throws SQLException {
        String sql = "INSERT INTO tbl_relation (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?::relation_role, ?) "
                + "RETURNING " + COLUMNS_RETURN;
        return parseRelation(super.uniqueResult(sql, relation.getUserId(), relation.getGroupId(),
                relation.getRemarks(), relation.getRole().toString().toLowerCase(), relation.getCreated()));
    }

    public List<Relation> list(int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_relation r " + "ORDER BY r.created OFFSET ? LIMIT ?";
        return parseRelations(super.list(sql, offset, limit));
    }

    public Optional<Relation> get(long userId, long groupId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_relation r " + "WHERE r.user_id=? AND r.group_id=?";
        Map<String, Object> map = super.uniqueResult(sql, userId, groupId);
        return Optional.fromNullable(parseRelation(map));
    }

    public int countByUserId(long userId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_relation r " + "WHERE r.user_id=?";
        Map<String, Object> ret = super.uniqueResult(sql, userId);
        return ((Long) ret.get("count")).intValue();
    }

    public int countByUserId(long userId, Filter[] filters) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_relation r " + "WHERE r.user_id=? " + getFilterAndClause(filters);
        Map<String, Object> ret = super.uniqueResult(sql, userId);
        return ((Long) ret.get("count")).intValue();
    }

    public List<Pair<Group, Relation>> listByUserId(long userId, int offset, int limit) throws SQLException {
        return this.listByUserId(userId, null, offset, limit);
    }

    public List<Pair<Group, Relation>> listByUserId(long userId, Filter[] filters, int offset, int limit)
            throws SQLException {
        String sql = "SELECT " + GroupDAO.COLUMNS_SELECT + ", " + COLUMNS_SELECT + " FROM tbl_relation r "
                + "INNER JOIN tbl_group g ON g.id=r.group_id " + GroupDAO.getFilterAndClause(filters, false)
                + "WHERE r.user_id=? " + getFilterAndClause(filters)
                + "ORDER BY r.created DESC, r.group_id OFFSET ? LIMIT ?";
        return parseGroupRelations(super.list(sql, userId, offset, limit));
    }

    public Pair<Integer, List<Pair<Group, Relation>>> listAndCountByUserId(long userId, Filter[] filters, int offset,
            int limit) throws SQLException {
        String sql = "SELECT count(*) as count " + " FROM tbl_relation r "
                + "INNER JOIN tbl_group g ON g.id=r.group_id " + GroupDAO.getFilterAndClause(filters, false)
                + "WHERE r.user_id=? " + getFilterAndClause(filters);
        Integer count = ((Long) super.uniqueResult(sql, userId).get("count")).intValue();

        sql = "SELECT " + GroupDAO.COLUMNS_SELECT + ", " + COLUMNS_SELECT + " FROM tbl_relation r "
                + "INNER JOIN tbl_group g ON g.id=r.group_id " + GroupDAO.getFilterAndClause(filters, false)
                + "WHERE r.user_id=? " + getFilterAndClause(filters)
                + "ORDER BY r.created DESC, r.group_id OFFSET ? LIMIT ?";
        List<Pair<Group, Relation>> groupRelations = parseGroupRelations(super.list(sql, userId, offset, limit));
        return Pair.of(count, groupRelations);
    }

    public int countByGroupId(long groupId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_relation r " + "WHERE r.group_id=?";
        Map<String, Object> ret = super.uniqueResult(sql, groupId);
        return ((Long) ret.get("count")).intValue();
    }

    public List<Pair<User, Relation>> listByGroupId(long groupId, int offset, int limit) throws SQLException {
        return this.listByGroupId(groupId, null, offset, limit);
    }

    public List<Pair<User, Relation>> listByGroupId(long groupId, Filter[] filters, int offset, int limit)
            throws SQLException {
        String sql = "SELECT " + UserDAO.COLUMNS_SELECT + ", " + COLUMNS_SELECT + " FROM tbl_relation r "
                + "INNER JOIN tbl_user u ON u.id=r.user_id " + UserDAO.getFilterAndClause(filters)
                + "WHERE r.group_id=? " + getFilterAndClause(filters)
                + "ORDER BY r.created DESC, r.user_id OFFSET ? LIMIT ?";
        return parseUserRelations(super.list(sql, groupId, offset, limit));
    }

    public Pair<Integer, List<Pair<User, Relation>>> listAndCountByGroupId(long groupId, Filter[] filters, int offset,
            int limit) throws SQLException {
        String sql = "SELECT count(*) as count " + " FROM tbl_relation r " + "INNER JOIN tbl_user u ON u.id=r.user_id "
                + UserDAO.getFilterAndClause(filters) + "WHERE r.group_id=? " + getFilterAndClause(filters);
        Integer count = ((Long) super.uniqueResult(sql, groupId).get("count")).intValue();

        sql = "SELECT " + UserDAO.COLUMNS_SELECT + ", " + COLUMNS_SELECT + " FROM tbl_relation r "
                + "INNER JOIN tbl_user u ON u.id=r.user_id " + UserDAO.getFilterAndClause(filters)
                + "WHERE r.group_id=? " + getFilterAndClause(filters)
                + "ORDER BY r.created DESC, r.user_id OFFSET ? LIMIT ?";
        List<Pair<User, Relation>> userRelations = parseUserRelations(super.list(sql, groupId, offset, limit));
        return Pair.of(count, userRelations);
    }

    public Optional<Relation> update(long userId, long groupId, Relation update) throws SQLException {
        Relation relation = this.get(userId, groupId).orNull();
        if (relation == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_relation r SET " + COLUMNS_UPDATE + " WHERE r.user_id=? AND r.group_id=? "
                + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseRelation(super.uniqueResult(sql,
                update.getUserId() == null ? relation.getUserId() : update.getUserId(),
                update.getGroupId() == null ? relation.getGroupId() : update.getGroupId(),
                update.getRemarks() == null ? relation.getRemarks() : update.getRemarks(),
                update.getRole() == null ? relation.getRole().toString().toLowerCase() : update.getRole().toString()
                        .toLowerCase(), update.getCreated() == null ? relation.getCreated() : update.getCreated(),
                userId, groupId)));
    }

    public Optional<Relation> delete(long userId, long groupId) throws SQLException {
        String sql = "DELETE FROM tbl_relation r " + "WHERE r.user_id=? AND r.group_id=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseRelation(super.uniqueResult(sql, userId, groupId)));
    }

    public int deleteByGroupId(long groupId) throws SQLException {
        String sql = "DELETE FROM tbl_relation r WHERE r.group_id=? ";
        return super.update(sql, groupId);
    }

    public int addAllUsersToSystemPublicGroup(long groupId, long groupOwnerId, Relation.Role relationRole,
            Filter[] filters) throws SQLException {
        String sql = "INSERT INTO tbl_relation (" + COLUMNS_INSERT + ") " + "SELECT u.id, ?, ?, ?::relation_role, ? "
                + "FROM tbl_user u WHERE u.id != ? " + UserDAO.getFilterAndClause(filters);
        return super.update(sql, groupId, "", relationRole.toString().toLowerCase(),
                new Timestamp(System.currentTimeMillis()), groupOwnerId);
    }

    public int addOtherUsersToSystemPublicGroup(long groupId, Relation.Role relationRole, Filter[] filters)
            throws SQLException {
        String sql = "INSERT INTO tbl_relation (" + COLUMNS_INSERT + ") " + "SELECT u.id, ?, ?, ?::relation_role, ? "
                + "FROM tbl_user u " + "WHERE u.id NOT IN "
                + "(SELECT r.user_id FROM tbl_relation r where r.group_id = ? ) " + UserDAO.getFilterAndClause(filters);
        return super.update(sql, groupId, "", relationRole.toString().toLowerCase(),
                new Timestamp(System.currentTimeMillis()), groupId);
    }

    protected static String getFilterSQL(Filter[] filters) {
        Set<Filter.Field> fields = new HashSet<Filter.Field>();
        fields.add(Filter.Field.RELATION_ROLE);
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
