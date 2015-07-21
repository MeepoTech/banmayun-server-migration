package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.tuple.Pair;






import com.banmayun.server.migration.to.core.Group;
import com.banmayun.server.migration.to.core.StatisticGroup;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.StatisticGroupDAO;
import com.google.common.base.Optional;

public class StatisticGroupDAOImpl extends AbstractDAO implements StatisticGroupDAO {

    protected static final String TABLE_NAME = "statistic_group";
    protected static final String TABLE_ALIAS = "_stat_group_";

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
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_UPDATE = DAOUtils.getColumnsUpdate(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public StatisticGroupDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    public static StatisticGroup parseStatisticGroup(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        StatisticGroup ret = new StatisticGroup();
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setDate((Timestamp) arg.get(COLUMN_ALIASES[1]));
        ret.setUserCount((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setMetaCount((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setFileCount((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setIsPersonalSpace((Boolean) arg.get(COLUMN_ALIASES[6]));
        return ret;
    }

    public static List<StatisticGroup> parseStatisticGroups(List<Map<String, Object>> arg) {
        List<StatisticGroup> ret = new ArrayList<StatisticGroup>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseStatisticGroup(map));
        }
        return ret;
    }

    public static Pair<Group, StatisticGroup> parseGroupInfo(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Group group = new Group();
        group = GroupDAOImpl.parseGroup(arg);

        StatisticGroup groupStat = new StatisticGroup();
        groupStat = parseStatisticGroup(arg);
        return Pair.of(group, groupStat);
    }

    public static List<Pair<Group, StatisticGroup>> parseGroupInfos(List<Map<String, Object>> arg) {
        List<Pair<Group, StatisticGroup>> ret = new ArrayList<Pair<Group, StatisticGroup>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseGroupInfo(map));
        }
        return ret;
    }

    private Optional<StatisticGroup> get(long groupId) throws DAOException {
        String sql = String.format("SELECT %1$s FROM %2$s %3$s WHERE %3$s.group_id = ?", COLUMNS_SELECT, TABLE_NAME,
                TABLE_ALIAS);
        return Optional.fromNullable(parseStatisticGroup(super.uniqueResult(sql, groupId)));
    }

    @Override
    public StatisticGroup create(StatisticGroup stat) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING %3$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        return parseStatisticGroup(super.uniqueResult(sql, stat.getGroupId(), stat.getDate(), stat.getUserCount(),
                stat.getMetaCount(), stat.getfileCount(), stat.getBytes(), stat.getIsPersonalSpace()));
    }

    @Override
    public Optional<StatisticGroup> update(long groupId, StatisticGroup update) throws DAOException {
        StatisticGroup groupStat = this.get(groupId).orNull();
        if (groupStat == null) {
            return Optional.absent();
        }
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.group_id = ? RETURNING %4$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional.fromNullable(parseStatisticGroup(super.uniqueResult(sql,
                update.getGroupId() == null ? groupStat.getGroupId() : update.getGroupId(),
                update.getDate() == null ? groupStat.getDate() : update.getDate(),
                update.getUserCount() == null ? groupStat.getUserCount() : update.getUserCount(),
                update.getMetaCount() == null ? groupStat.getMetaCount() : update.getMetaCount(),
                update.getfileCount() == null ? groupStat.getfileCount() : update.getfileCount(),
                update.getBytes() == null ? groupStat.getBytes() : update.getBytes(),
                update.getIsPersonalSpace() == null ? groupStat.getIsPersonalSpace() : update.getIsPersonalSpace(),
                groupId)));
    }

    @Override
    public StatisticGroup updateOrCreate(StatisticGroup stat) throws DAOException {
        StatisticGroup groupStat = this.get(stat.getGroupId()).orNull();
        if (groupStat == null) {
            return this.create(stat);
        } else {
            return this.update(stat.getGroupId(), stat).orNull();
        }
    }

    @Override
    public List<Pair<Group, StatisticGroup>> getTopGroup(OrderBy orderBy, long limit) throws DAOException {
        String sql = String
                .format("SELECT %3$s, %6$s FROM %1$s %2$s INNER JOIN %4$s %5$s ON %2$s.group_id = %6$s.id WHERE %2$s.is_personal_space = false ORDER BY ? DESC LIMIT ?",
                        TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, GroupDAOImpl.TABLE_NAME, GroupDAOImpl.TABLE_ALIAS,
                        GroupDAOImpl.COLUMNS_SELECT);
        return parseGroupInfos(super.list(sql, orderBy.toString().toLowerCase(), limit));
    }
}
