package com.banmayun.server.migration.to.db;

import java.util.List;

import com.banmayun.server.migration.to.core.Data;
import com.google.common.base.Optional;

public interface DataDAO {

    public Data createData(Data data) throws UniqueViolationException, DAOException;

    public Optional<Data> getData(String md5, long bytes) throws DAOException;

    public Optional<Data> deleteOrphanData(String md5, long bytes) throws DAOException;

    public List<Data> deleteOrphanData() throws DAOException;

	public void batchInsertDatas(List<Data> datas) throws UniqueViolationException,
			DAOException;
}
