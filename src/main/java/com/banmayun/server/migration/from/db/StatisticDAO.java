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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.banmayun.server.migration.from.core.Statistic;
import com.google.common.base.Optional;

public class StatisticDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "group_id", "root_id", "quota", "version", "bytes" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "s";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Statistic parseStatistic(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Statistic ret = new Statistic();
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setQuota((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[4]));
        return ret;
    }

    public static List<Statistic> parseStatistics(List<Map<String, Object>> arg) {
        List<Statistic> ret = new ArrayList<Statistic>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseStatistic(map));
        }
        return ret;
    }

    public StatisticDAO(Connection conn) {
        super(conn);
    }

    public Statistic create(Statistic stat) throws SQLException {
        String sql = "INSERT INTO tbl_stat (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?) " + "RETURNING "
                + COLUMNS_RETURN;
        return parseStatistic(super.uniqueResult(sql, stat.getGroupId(), stat.getRootId(), stat.getQuota(),
                stat.getVersion(), stat.getBytes()));
    }

    public Optional<Statistic> get(long groupId, long rootId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_stat s " + "WHERE s.group_id=? AND s.root_id=?";
        return Optional.fromNullable(parseStatistic(super.uniqueResult(sql, groupId, rootId)));
    }

    public Optional<Statistic> addBytes(long groupId, long rootId, long bytesDiff) throws SQLException {
        String sql = "UPDATE tbl_stat s SET bytes=s.bytes+? " + "WHERE s.group_id=? AND s.root_id=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseStatistic(super.uniqueResult(sql, bytesDiff, groupId, rootId)));
    }

    public Optional<Statistic> addQuota(long groupId, long rootId, long quotaDiff) throws SQLException {
        String sql = "UPDATE tbl_stat s SET quota=s.quota+? " + "WHERE s.group_id=? AND s.root_id=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseStatistic(super.uniqueResult(sql, quotaDiff, groupId, rootId)));
    }

    public Optional<Statistic> setQuota(long groupId, long rootId, long quota) throws SQLException {
        String sql = "UPDATE tbl_stat s SET quota=? " + "WHERE s.group_id=? AND s.root_id=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseStatistic(super.uniqueResult(sql, quota, groupId, rootId)));
    }

    public Optional<Statistic> incrementVersion(long groupId, long rootId) throws SQLException {
        String sql = "UPDATE tbl_stat s SET version=version+1 " + "WHERE s.group_id=? AND s.root_id=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional.fromNullable(parseStatistic(super.uniqueResult(sql, groupId, rootId)));
    }
}
