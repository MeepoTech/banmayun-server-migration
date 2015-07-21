package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;
import java.util.List;

import com.banmayun.server.migration.to.core.StatisticSummary;
import com.google.common.base.Optional;

public interface StatisticSummaryDAO {

    public StatisticSummary create(StatisticSummary stat) throws DAOException;

    public StatisticSummary updateOrCreate(StatisticSummary stat) throws DAOException;

    public Optional<StatisticSummary> update(Timestamp date, StatisticSummary update) throws DAOException;

    public Optional<StatisticSummary> getLatest(Timestamp date) throws DAOException;

    public Optional<StatisticSummary> getLatest() throws DAOException;

    public List<StatisticSummary> listAll() throws DAOException;
}
