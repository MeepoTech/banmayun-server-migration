package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.banmayun.server.migration.to.core.StatisticSummary;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.StatisticSummaryDAO;
import com.google.common.base.Optional;

public class StatisticSummaryDAOImpl extends AbstractDAO implements StatisticSummaryDAO {

    protected static final String TABLE_NAME = "statistic_summary";
    protected static final String TABLE_ALIAS = "_stat_summary_";

    protected static final String[] COLUMN_NAMES = new String[] { "date", "group_count", "user_count", "meta_count",
            "file_count", "bytes", "extension_count" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_UPDATE = DAOUtils.getColumnsUpdate(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public StatisticSummaryDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    public static StatisticSummary parseStatisticSummary(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        StatisticSummary ret = new StatisticSummary();
        ret.setDate((Timestamp) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupCount((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setUserCount((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setMetaCount((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setFileCount((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setExtensionCount((String) arg.get(COLUMN_ALIASES[6]));
        return ret;
    }

    public static List<StatisticSummary> parseStatisticSummaries(List<Map<String, Object>> arg) {
        List<StatisticSummary> ret = new ArrayList<StatisticSummary>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseStatisticSummary(map));
        }
        return ret;
    }

    private Optional<StatisticSummary> get(Timestamp date) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.date = ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseStatisticSummary(super.uniqueResult(sql, date)));
    }

    @Override
    public StatisticSummary create(StatisticSummary stat) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING %3$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        return parseStatisticSummary(super.uniqueResult(sql, stat.getDate(), stat.getGroupCount(), stat.getUserCount(),
                stat.getMetaCount(), stat.getfileCount(), stat.getBytes(), stat.getExtensionCount()));
    }

    @Override
    public StatisticSummary updateOrCreate(StatisticSummary stat) throws DAOException {
        StatisticSummary groupStat = this.get(stat.getDate()).orNull();
        if (groupStat == null) {
            return this.create(stat);
        } else {
            return this.update(stat.getDate(), stat).orNull();
        }
    }

    @Override
    public Optional<StatisticSummary> update(Timestamp date, StatisticSummary update) throws DAOException {
        StatisticSummary summaryStat = this.get(date).orNull();
        if (summaryStat == null) {
            return Optional.absent();
        }
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.date = ? RETURNING %4$s ", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional
                .fromNullable(parseStatisticSummary(super.uniqueResult(
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

    @Override
    public Optional<StatisticSummary> getLatest(Timestamp date) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.date <= ? ORDER BY %2$s.date DESC LIMIT 1",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseStatisticSummary(super.uniqueResult(sql, date)));
    }

    @Override
    public Optional<StatisticSummary> getLatest() throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.date DESC LIMIT 1", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseStatisticSummary(super.uniqueResult(sql)));
    }

    @Override
    public List<StatisticSummary> listAll() throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.date", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return parseStatisticSummaries(super.list(sql));
    }

}
