package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;
import java.util.List;

import com.banmayun.server.migration.to.core.Statistic;
import com.banmayun.server.migration.to.core.Statistic.StatisticType;

public interface StatisticDAO {

    public Statistic createStatistic(Statistic statistic) throws DAOException;

    public List<Statistic> listStatistics(StatisticType type, int offset, int limit) throws DAOException;

    public void deleteStatisticsCreatedBefore(Timestamp time) throws DAOException;

    public void deleteStatisticsCreatedBefore(Timestamp time, StatisticType type) throws DAOException;
}
