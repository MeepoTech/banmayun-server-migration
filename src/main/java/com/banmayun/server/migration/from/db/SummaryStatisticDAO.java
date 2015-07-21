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

import com.banmayun.server.migration.from.core.SummaryStatistic;
import com.google.common.base.Optional;

public class SummaryStatisticDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "date", "group_count", "user_count", "meta_count",
            "file_count", "bytes", "extension_count" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "ss";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public SummaryStatisticDAO(Connection conn) {
        super(conn);
    }

    public static SummaryStatistic parseSummaryStatistic(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        SummaryStatistic ret = new SummaryStatistic();
        ret.setDate((Timestamp) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupCount((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setUserCount((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setMetaCount((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setFileCount((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setExtensionCount((String) arg.get(COLUMN_ALIASES[6]));
        return ret;
    }

    public static List<SummaryStatistic> parseSummaryStatistics(List<Map<String, Object>> arg) {
        List<SummaryStatistic> ret = new ArrayList<SummaryStatistic>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseSummaryStatistic(map));
        }
        return ret;
    }

    public SummaryStatistic create(SummaryStatistic stat) throws SQLException {
        String sql = "INSERT INTO tbl_summary_statistic (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "RETURNING " + COLUMNS_RETURN;
        return parseSummaryStatistic(super.uniqueResult(sql, stat.getDate(), stat.getGroupCount(), stat.getUserCount(),
                stat.getMetaCount(), stat.getfileCount(), stat.getBytes(), stat.getExtensionCount()));
    }
    
    public List<SummaryStatistic> list(int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_summary_statistic ss " + "ORDER BY ss.date OFFSET ? LIMIT ?";
        return parseSummaryStatistics(super.list(sql, offset, limit));
    }

    public SummaryStatistic updateOrCreate(SummaryStatistic stat) throws SQLException {
        SummaryStatistic groupStat = this.get(stat.getDate()).orNull();
        if (groupStat == null) {
            return this.create(stat);
        } else {
            return this.update(stat.getDate(), stat).orNull();
        }
    }

    public Optional<SummaryStatistic> update(Timestamp date, SummaryStatistic update) throws SQLException {
        SummaryStatistic summaryStat = this.get(date).orNull();
        if (summaryStat == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_summary_statistic ss SET " + COLUMNS_UPDATE + " WHERE ss.date=? " + "RETURNING "
                + COLUMNS_RETURN;
        return Optional
                .fromNullable(parseSummaryStatistic(super.uniqueResult(
                        sql,
                        update.getDate() == null ? summaryStat.getDate() : update.getDate(),
                        update.getGroupCount() == null ? summaryStat.getGroupCount() : update.getGroupCount(),
                        update.getUserCount() == null ? summaryStat.getUserCount() : update.getUserCount(),
                        update.getMetaCount() == null ? summaryStat.getMetaCount() : update.getMetaCount(),
                        update.getfileCount() == null ? summaryStat.getfileCount() : update.getfileCount(),
                        update.getBytes() == null ? summaryStat.getBytes() : update.getBytes(),
                        update.getExtensionCount() == null ? summaryStat.getExtensionCount() : update
                                .getExtensionCount(), date)));
    }

    public Optional<SummaryStatistic> get(Timestamp date) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_summary_statistic ss " + "WHERE ss.date=? ";
        return Optional.fromNullable(parseSummaryStatistic(super.uniqueResult(sql, date)));
    }

    public Optional<SummaryStatistic> getLatest(Timestamp date) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_summary_statistic ss "
                + "WHERE ss.date <= ? ORDER BY ss.date desc limit 1";
        return Optional.fromNullable(parseSummaryStatistic(super.uniqueResult(sql, date)));
    }

    public Optional<SummaryStatistic> getLatest() throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_summary_statistic ss " + "ORDER BY ss.date desc limit 1";
        return Optional.fromNullable(parseSummaryStatistic(super.uniqueResult(sql)));
    }

    public List<SummaryStatistic> listAll() throws Exception {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_summary_statistic ss ORDER BY ss.date";
        return parseSummaryStatistics(super.list(sql));
    }
}
