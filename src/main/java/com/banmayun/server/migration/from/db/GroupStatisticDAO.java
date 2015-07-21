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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.from.core.Group;
import com.banmayun.server.migration.from.core.GroupStatistic;
import com.google.common.base.Optional;

public class GroupStatisticDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "group_id", "date", "user_count", "meta_count",
            "file_count", "bytes", "is_personal_space" };

    public static enum OrderBy {
        USER_COUNT,
        META_COUNT,
        FILE_COUNT,
        BYTES
    }

    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "gs";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public GroupStatisticDAO(Connection conn) {
        super(conn);
    }

    public static GroupStatistic parseGroupStatistic(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        GroupStatistic ret = new GroupStatistic();
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setDate((Timestamp) arg.get(COLUMN_ALIASES[1]));
        ret.setUserCount((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setMetaCount((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setFileCount((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setIsPersonalSpace((Boolean) arg.get(COLUMN_ALIASES[6]));
        return ret;
    }

    public static List<GroupStatistic> parseGroupStatistics(List<Map<String, Object>> arg) {
        List<GroupStatistic> ret = new ArrayList<GroupStatistic>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroupStatistic(map));
        }
        return ret;
    }

    public static Pair<Group, GroupStatistic> parseGroupInfo(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Group group = new Group();
        group = GroupDAO.parseGroup(arg);

        GroupStatistic groupStat = new GroupStatistic();
        groupStat = parseGroupStatistic(arg);
        return Pair.of(group, groupStat);
    }

    public static List<Pair<Group, GroupStatistic>> parseGroupInfos(List<Map<String, Object>> arg) {
        List<Pair<Group, GroupStatistic>> ret = new ArrayList<Pair<Group, GroupStatistic>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroupInfo(map));
        }
        return ret;
    }

    public Optional<GroupStatistic> get(long groupId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group_statistic gs " + "WHERE gs.group_id=?";
        return Optional.fromNullable(parseGroupStatistic(super.uniqueResult(sql, groupId)));
    }

    public GroupStatistic updateOrCreate(GroupStatistic stat) throws SQLException {
        GroupStatistic groupStat = this.get(stat.getGroupId()).orNull();
        if (groupStat == null) {
            return this.create(stat);
        } else {
            return this.update(stat.getGroupId(), stat).orNull();
        }
    }
    
    public List<GroupStatistic> list(int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group_statistic gs " + "ORDER BY gs.group_id OFFSET ? LIMIT ?";
        return parseGroupStatistics(super.list(sql, offset, limit));
    }

    public GroupStatistic create(GroupStatistic stat) throws SQLException {
        String sql = "INSERT INTO tbl_group_statistic (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "RETURNING " + COLUMNS_RETURN;
        return parseGroupStatistic(super.uniqueResult(sql, stat.getGroupId(), stat.getDate(), stat.getUserCount(),
                stat.getMetaCount(), stat.getfileCount(), stat.getBytes(), stat.getIsPersonalSpace()));
    }

    public Optional<GroupStatistic> update(long groupId, GroupStatistic update) throws SQLException {
        GroupStatistic groupStat = this.get(groupId).orNull();
        if (groupStat == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_group_statistic gs SET " + COLUMNS_UPDATE + " WHERE gs.group_id=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseGroupStatistic(super.uniqueResult(sql,
                update.getGroupId() == null ? groupStat.getGroupId() : update.getGroupId(),
                update.getDate() == null ? groupStat.getDate() : update.getDate(),
                update.getUserCount() == null ? groupStat.getUserCount() : update.getUserCount(),
                update.getMetaCount() == null ? groupStat.getMetaCount() : update.getMetaCount(),
                update.getfileCount() == null ? groupStat.getfileCount() : update.getfileCount(),
                update.getBytes() == null ? groupStat.getBytes() : update.getBytes(),
                update.getIsPersonalSpace() == null ? groupStat.getIsPersonalSpace() : update.getIsPersonalSpace(),
                groupId)));
    }

    public Optional<GroupStatistic> get(long groupId, Timestamp date) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_group_statistic s " + "WHERE s.group_id=? AND s.date=? ";
        return Optional.fromNullable(parseGroupStatistic(super.uniqueResult(sql, groupId, date)));
    }

    public List<Pair<Group, GroupStatistic>> getTopGroup(OrderBy orderBy, long limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + ", " + GroupDAO.COLUMNS_SELECT + " FROM tbl_group_statistic gs "
                + "INNER JOIN tbl_group g ON gs.group_id = g.id " + "WHERE gs.is_personal_space = false " + "ORDER BY "
                + orderBy.toString().toLowerCase() + " DESC LIMIT ?";

        return parseGroupInfos(super.list(sql, limit));
    }
}
