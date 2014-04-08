package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.banmayun.server.migration.to.core.Statistic;
import com.banmayun.server.migration.to.core.Statistic.StatisticType;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.StatisticDAO;

public class StatisticDAOImpl extends AbstractDAO implements StatisticDAO {

    protected static final String TABLE_NAME = "stats";
    protected static final String TABLE_ALIAS = "_stat_";
    protected static final String[] COLUMN_NAMES = new String[] { "type", "int_value", "long_value", "string_value",
            "created_at" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Statistic parseStatistic(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Statistic ret = new Statistic();
        ret.setType(Statistic.StatisticType.valueOf(((String) arg.get(COLUMN_ALIASES[0]))));
        ret.setIntValue((Integer) arg.get(COLUMN_ALIASES[1]));
        ret.setLongValue((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setStringValue((String) arg.get(COLUMN_ALIASES[3]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[4]));
        return ret;
    }

    public static List<Statistic> parseStatistics(List<Map<String, Object>> arg) {
        List<Statistic> ret = new ArrayList<Statistic>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseStatistic(map));
        }
        return ret;
    }

    public StatisticDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Statistic createStatistic(Statistic statistic) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?:stat_type, ?, ?, ?, ?) RETURNING %3$s",
                TABLE_NAME, COLUMNS_INSERT, COLUMNS_RETURN);
        return parseStatistic(super.uniqueResult(sql, statistic.getType().toString(), statistic.getIntValue(),
                statistic.getLongValue(), statistic.getStringValue(), statistic.getCreatedAt()));
    }

    @Override
    public List<Statistic> listStatistics(StatisticType type, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s ORDER BY %2$s.created_at DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseStatistics(super.list(sql, offset, limit));
    }

    @Override
    public void deleteStatisticsCreatedBefore(Timestamp time) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.created_at<?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, time);
    }

    @Override
    public void deleteStatisticsCreatedBefore(Timestamp time, StatisticType type) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.created_at<? AND %2$s.type=?::stat_type",
                TABLE_NAME, TABLE_ALIAS);
        super.update(sql, time, type);
    }
}
