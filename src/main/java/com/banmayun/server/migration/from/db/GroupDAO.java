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

import com.banmayun.server.migration.from.core.Group;
import com.banmayun.server.migration.from.core.Relation;
import com.google.common.base.Optional;

public class GroupDAO extends AbstractDAO {
    public static final String DEFAULT_DOMAIN = "local";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "name", "intro", "tags", "type", "is_visible",
            "status", "announce", "created", "created_by", "domain" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "g";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = StringUtils.replace(COLUMNS_UPDATE, "type=?", "type=?::group_type");
        COLUMNS_UPDATE = StringUtils.replace(COLUMNS_UPDATE, "status=?", "status=?::group_status");
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Group parseGroup(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Group ret = new Group();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setName((String) arg.get(COLUMN_ALIASES[1]));
        ret.setIntro((String) arg.get(COLUMN_ALIASES[2]));
        ret.setTags((String) arg.get(COLUMN_ALIASES[3]));
        ret.setType(Group.Type.valueOf(((String) arg.get(COLUMN_ALIASES[4])).toUpperCase()));
        ret.setIsVisible((Boolean) arg.get(COLUMN_ALIASES[5]));
        ret.setStatus(Group.Status.valueOf(((String) arg.get(COLUMN_ALIASES[6])).toUpperCase()));
        ret.setAnnounce((String) arg.get(COLUMN_ALIASES[7]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[8]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[9]));
        ret.setDomain((String) arg.get(COLUMN_ALIASES[10]));
        return ret;
    }

    public static Group parseGroupWithStatistic(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Group ret = new Group();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setName((String) arg.get(COLUMN_ALIASES[1]));
        ret.setIntro((String) arg.get(COLUMN_ALIASES[2]));
        ret.setTags((String) arg.get(COLUMN_ALIASES[3]));
        ret.setType(Group.Type.valueOf(((String) arg.get(COLUMN_ALIASES[4])).toUpperCase()));
        ret.setIsVisible((Boolean) arg.get(COLUMN_ALIASES[5]));
        ret.setStatus(Group.Status.valueOf(((String) arg.get(COLUMN_ALIASES[6])).toUpperCase()));
        ret.setAnnounce((String) arg.get(COLUMN_ALIASES[7]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[8]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[9]));
        ret.setDomain((String) arg.get(COLUMN_ALIASES[10]));

        return ret;
    }

    public static List<Group> parseGroups(List<Map<String, Object>> arg) {
        List<Group> ret = new ArrayList<Group>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroup(map));
        }
        return ret;
    }

    public GroupDAO(Connection conn) {
        super(conn);
    }

    public Group create(Group group) throws SQLException {
        String sql = "INSERT INTO tbl_group (" + COLUMNS_INSERT + ") "
                + "VALUES (?, ?, ?, ?::group_type, ?, ?::group_status, ?, ?, ?, ?) " + "RETURNING " + COLUMNS_RETURN;
        return parseGroup(super.uniqueResult(sql, group.getName(), group.getIntro(), group.getTags(), group.getType()
                .toString().toLowerCase(), group.getIsVisible(), group.getStatus().toString().toLowerCase(),
                group.getAnnounce(), group.getCreated(), group.getCreatedBy(), group.getDomain()));
    }

    public Optional<Group> get(long id) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group g " + "WHERE g.id=?";
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, id)));
    }

    public Optional<Group> getByName(String name) throws SQLException {
        return this.getByNameAndDomain(name, DEFAULT_DOMAIN);
    }

    public Optional<Group> getByNameAndDomain(String name, String domain) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group g "
                + "WHERE lower(g.name)=lower(?) and lower(u.domain)=lower(?)";
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, name, domain)));
    }

    public int count() throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_group g ";
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    public List<Group> list(int offset, int limit) throws SQLException {
        return this.list(null, true, offset, limit);
    }

    public List<Group> list(Filter[] filters, boolean visibleOnly, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group g " + getFilterWhereClause(filters, visibleOnly)
                + "ORDER BY g.created DESC OFFSET ? LIMIT ?";
        return parseGroups(super.list(sql, offset, limit));
    }

    public Pair<Integer, List<Group>> listAndCount(Filter[] filters, boolean visibleOnly, int offset, int limit)
            throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_group g " + getFilterWhereClause(filters, visibleOnly);
        Integer count = ((Long) super.uniqueResult(sql).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group g " + getFilterWhereClause(filters, visibleOnly)
                + "ORDER BY g.created DESC OFFSET ? LIMIT ?";
        List<Group> groups = parseGroups(super.list(sql, offset, limit));
        return Pair.of(count, groups);
    }

    public List<Pair<Group, Relation>> listFetchRelation(long userId, int offset, int limit) throws SQLException {
        return this.listFetchRelation(userId, null, false, offset, limit);
    }

    public List<Pair<Group, Relation>> listFetchRelation(long userId, Filter[] filters, boolean visibleOnly,
            int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_group g "
                + "LEFT JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id "
                + getFilterWhereClause(filters, visibleOnly) + "ORDER BY g.created DESC OFFSET ? LIMIT ?";
        return RelationDAO.parseGroupRelations(super.list(sql, userId, offset, limit));
    }

    public Pair<Integer, List<Pair<Group, Relation>>> listAndCountFetchRelation(long userId, Filter[] filters,
            boolean visibleOnly, int offset, int limit) throws SQLException {
        String sql = "SELECT count(*) as count " + " FROM tbl_group g "
                + "LEFT JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id "
                + getFilterWhereClause(filters, visibleOnly);
        Integer count = ((Long) super.uniqueResult(sql, userId).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_group g "
                + "LEFT JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id "
                + getFilterWhereClause(filters, visibleOnly) + "ORDER BY g.created DESC OFFSET ? LIMIT ?";
        List<Pair<Group, Relation>> groupRelations = RelationDAO.parseGroupRelations(super.list(sql, userId, offset,
                limit));
        return Pair.of(count, groupRelations);
    }

    private static final String SEARCH_CLAUSE = "WHERE " + "(lower(g.name) LIKE lower(?) OR to_tsvector('chinese', "
            + "coalesce(lower(g.name), '') || ' ' || " + "coalesce(lower(g.intro), '') || ' ' || "
            + "coalesce(lower(g.tags), '')) " + "@@ plainto_tsquery('chinese', lower(?))) ";

    public List<Group> search(String query, int offset, int limit) throws SQLException {
        return this.search(query, null, false, offset, limit);
    }

    public List<Group> search(String query, Filter[] filters, boolean visibleOnly, int offset, int limit)
            throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group g " + SEARCH_CLAUSE
                + getFilterAndClause(filters, visibleOnly) + "OFFSET ? LIMIT ?";
        return parseGroups(super.list(sql, query + "%", query, offset, limit));
    }

    public Pair<Integer, List<Group>> searchAndCount(String query, Filter[] filters, boolean visibleOnly, int offset,
            int limit) throws SQLException {
        String whereClause = SEARCH_CLAUSE + getFilterAndClause(filters, visibleOnly);

        String sql = "SELECT COUNT(*) FROM tbl_group g " + whereClause;
        Integer count = ((Long) super.uniqueResult(sql, query + "%", query).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group g " + whereClause + "OFFSET ? LIMIT ?";
        List<Group> groups = parseGroups(super.list(sql, query + "%", query, offset, limit));

        return Pair.of(count, groups);
    }

    public List<Pair<Group, Relation>> searchFetchRelation(String query, long userId, int offset, int limit)
            throws SQLException {
        return this.searchFetchRelation(userId, query, null, false, offset, limit);
    }

    public List<Pair<Group, Relation>> searchFetchRelation(long userId, String query, Filter[] filters,
            boolean visibleOnly, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_group g "
                + "LEFT JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id " + SEARCH_CLAUSE
                + getFilterAndClause(filters, visibleOnly) + "OFFSET ? LIMIT ?";
        return RelationDAO.parseGroupRelations(super.list(sql, userId, query + "%", query, offset, limit));
    }

    public Pair<Integer, List<Pair<Group, Relation>>> searchAndCountFetchRelation(long userId, String query,
            Filter[] filters, boolean visibleOnly, int offset, int limit) throws SQLException {
        String sql = "SELECT count(*) as count" + " FROM tbl_group g "
                + "LEFT JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id " + SEARCH_CLAUSE
                + getFilterAndClause(filters, visibleOnly);
        Integer count = ((Long) super.uniqueResult(sql, userId, query + "%", query).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_group g "
                + "LEFT JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id " + SEARCH_CLAUSE
                + getFilterAndClause(filters, visibleOnly) + "OFFSET ? LIMIT ?";
        List<Pair<Group, Relation>> groupRelations = RelationDAO.parseGroupRelations(super.list(sql, userId, query
                + "%", query, offset, limit));

        return Pair.of(count, groupRelations);
    }

    public List<Pair<Group, Relation>> searchByUserId(long userId, String query, int offset, int limit)
            throws SQLException {
        return this.searchByUserId(userId, query, null, false, offset, limit);
    }

    public List<Pair<Group, Relation>> searchByUserId(long userId, String query, Filter[] filters, boolean visibleOnly,
            int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_group g "
                + "INNER JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id "
                + RelationDAO.getFilterAndClause(filters) + SEARCH_CLAUSE + getFilterAndClause(filters, visibleOnly)
                + "OFFSET ? LIMIT ?";
        return RelationDAO.parseGroupRelations(super.list(sql, userId, query + "%", query, offset, limit));
    }

    public Pair<Integer, List<Pair<Group, Relation>>> searchAndCountByUserId(long userId, String query,
            Filter[] filters, boolean visibleOnly, int offset, int limit) throws SQLException {
        String sql = "SELECT count(*) as count " + " FROM tbl_group g "
                + "INNER JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id "
                + RelationDAO.getFilterAndClause(filters) + SEARCH_CLAUSE + getFilterAndClause(filters, visibleOnly);
        Integer count = ((Long) super.uniqueResult(sql, userId, query + "%", query).get("count")).intValue();

        sql = "SELECT " + COLUMNS_SELECT + ", " + RelationDAO.COLUMNS_SELECT + " FROM tbl_group g "
                + "INNER JOIN tbl_relation r ON r.user_id=? AND r.group_id=g.id "
                + RelationDAO.getFilterAndClause(filters) + SEARCH_CLAUSE + getFilterAndClause(filters, visibleOnly)
                + "OFFSET ? LIMIT ?";
        List<Pair<Group, Relation>> groupRelations = RelationDAO.parseGroupRelations(super.list(sql, userId, query
                + "%", query, offset, limit));
        return Pair.of(count, groupRelations);
    }

    public Optional<Group> update(long id, Group update) throws SQLException {
        Group group = this.get(id).orNull();
        if (group == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_group g SET " + COLUMNS_UPDATE + " WHERE g.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql,
                update.getId() == null ? group.getId() : update.getId(), update.getName() == null ? group.getName()
                        : update.getName(), update.getIntro() == null ? group.getIntro() : update.getIntro(), update
                        .getTags() == null ? group.getTags() : update.getTags(), update.getType() == null ? group
                        .getType().toString().toLowerCase() : update.getType().toString().toLowerCase(), update
                        .getIsVisible() == null ? group.getIsVisible() : update.getIsVisible(),
                update.getStatus() == null ? group.getStatus().toString().toLowerCase() : update.getStatus().toString()
                        .toLowerCase(), update.getAnnounce() == null ? group.getAnnounce() : update.getAnnounce(),
                update.getCreated() == null ? group.getCreated() : update.getCreated(),
                update.getCreatedBy() == null ? group.getCreatedBy() : update.getCreatedBy(),
                update.getDomain() == null ? group.getDomain() : update.getDomain(), id)));
    }

    public Optional<Group> delete(long id) throws SQLException {
        String sql = "DELETE FROM tbl_group g WHERE g.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseGroup(super.uniqueResult(sql, id)));
    }

    protected static String getFilterSQL(Filter[] filters) {
        Set<Filter.Field> fields = new HashSet<Filter.Field>();
        fields.add(Filter.Field.GROUP_STATUS);
        fields.add(Filter.Field.GROUP_TYPE);
        return Filter.getSQL(filters, fields);
    }

    protected static String getFilterWhereClause(Filter[] filters, boolean visibleOnly) {
        String filterSQL = getFilterSQL(filters);
        String ret = "";
        if (visibleOnly) {
            if (filterSQL.length() > 0) {
                ret = "WHERE " + filterSQL + " AND g.is_visible ";
            } else {
                ret = "WHERE g.is_visible ";
            }
        } else {
            if (filterSQL.length() > 0) {
                ret = "WHERE " + filterSQL;
            }
        }
        return ret;
    }

    protected static String getFilterAndClause(Filter[] filters, boolean visibleOnly) {
        String filterSQL = getFilterSQL(filters);
        String ret = "";
        if (visibleOnly) {
            if (filterSQL.length() > 0) {
                ret = "AND " + filterSQL + " AND g.is_visible ";
            } else {
                ret = "AND g.is_visible ";
            }
        } else {
            if (filterSQL.length() > 0) {
                ret = "AND " + filterSQL;
            }
        }
        return ret;
    }
}
