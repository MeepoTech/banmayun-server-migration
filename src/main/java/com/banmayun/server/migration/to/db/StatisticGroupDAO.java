package com.banmayun.server.migration.to.db;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Group;
import com.banmayun.server.migration.to.core.StatisticGroup;
import com.banmayun.server.migration.to.db.impl.StatisticGroupDAOImpl.OrderBy;
import com.google.common.base.Optional;

public interface StatisticGroupDAO {

    StatisticGroup create(StatisticGroup stat) throws DAOException;

    List<Pair<Group, StatisticGroup>> getTopGroup(OrderBy orderBy, long limit) throws DAOException;

    StatisticGroup updateOrCreate(StatisticGroup stat) throws DAOException;

    Optional<StatisticGroup> update(long groupId, StatisticGroup update) throws DAOException;

}
