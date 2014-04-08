package com.banmayun.server.migration.to.db.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.to.core.Data;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.DataDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class DataDAOImpl extends AbstractDAO implements DataDAO {

    protected static final String TABLE_NAME = "datas";
    protected static final String TABLE_ALIAS = "_data_";
    protected static final String[] COLUMN_NAMES = new String[] { "md5", "bytes", "location", "ref_count" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 0, COLUMN_NAMES.length - 1));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Data parseDataObject(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Data ret = new Data();
        ret.setMD5((String) arg.get(COLUMN_ALIASES[0]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setLocation((String) arg.get(COLUMN_ALIASES[2]));
        ret.setRefCount((Integer) arg.get(COLUMN_ALIASES[3]));
        return ret;
    }

    public static List<Data> parseDataObjects(List<Map<String, Object>> arg) {
        List<Data> ret = new ArrayList<Data>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseDataObject(map));
        }
        return ret;
    }

    public DataDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Data createData(Data data) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?) RETURNING %3$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        return parseDataObject(super.uniqueResult(sql, data.getMD5(), data.getBytes(), data.getLocation()));
    }

    @Override
    public Optional<Data> getData(String md5, long bytes) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.md5=? AND %2$s.bytes=?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseDataObject(super.uniqueResult(sql, md5, bytes)));
    }

    @Override
    public Optional<Data> deleteOrphanData(String md5, long bytes) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.md5=? AND %2$s.bytes=? AND %2$s.ref_count<=0 "
                + "RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseDataObject(super.uniqueResult(sql, md5, bytes)));
    }

    @Override
    public List<Data> deleteOrphanData() throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.ref_count<=0 RETURNING %3$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_RETURN);
        return parseDataObjects(super.list(sql));
    }
}
