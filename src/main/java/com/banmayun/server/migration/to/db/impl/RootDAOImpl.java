package com.banmayun.server.migration.to.db.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Root;
import com.banmayun.server.migration.to.core.Root.RootType;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.RootDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class RootDAOImpl extends AbstractDAO implements RootDAO {

    protected static final String TABLE_NAME = "roots";
    protected static final String TABLE_ALIAS = "_root_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "type", "quota", "default_perm", "file_count",
            "byte_count" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length - 2));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Root parseRoot(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Root ret = new Root();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setType(RootType.valueOf((String) arg.get(COLUMN_ALIASES[1])));
        ret.setQuota((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setDefaultPermission((String) arg.get(COLUMN_ALIASES[3]));
        ret.setFileCount((Integer) arg.get(COLUMN_ALIASES[4]));
        ret.setByteCount((Long) arg.get(COLUMN_ALIASES[5]));
        return ret;
    }

    public static List<Root> parseRoots(List<Map<String, Object>> arg) {
        List<Root> ret = new ArrayList<Root>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseRoot(map));
        }
        return ret;
    }

    public RootDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Root createRoot(Root root) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?::root_type, ?, ?) RETURNING %3$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        return parseRoot(super.uniqueResult(sql, root.getType().toString(), root.getQuota(),
                root.getDefaultPermission()));
    }

    @Override
    public Optional<Root> getRoot(long id) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.id=?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseRoot(super.uniqueResult(sql, id)));
    }

    @Override
    public Pair<Integer, Long> countFilesAndBytes() throws DAOException {
        String sql = String.format("SELECT coalesce(SUM(%2$s.file_count), 0) AS file_count, "
                + "coalesce(SUM(%2$s.byte_count), 0) AS byte_count FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql);
        int fileCount = 0;
        long byteCount = 0;
        if (((Long) ret.get("file_count")) != null) {
            fileCount = ((Long) ret.get("file_count")).intValue();
        }
        if (((BigDecimal) ret.get("byte_count")) != null) {
            byteCount = ((BigDecimal) ret.get("byte_count")).longValue();
        }
        return Pair.of(fileCount, byteCount);
    }

    @Override
    public Optional<Root> addRootQuota(long id, long quota) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET quota=%2$s.quota+? WHERE %2$s.id=? RETURNING %3$s",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseRoot(super.uniqueResult(sql, quota, id)));
    }

    @Override
    public Optional<Root> setRootQuota(long id, long quota) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET quota=? WHERE %2$s.id=? RETURNING %3$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseRoot(super.uniqueResult(sql, quota, id)));
    }

    @Override
    public Optional<Root> setRootDefaultPermission(long id, String defaultPermission) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET default_perm=? WHERE %2$s.id=? RETURNING %3$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseRoot(super.uniqueResult(sql, defaultPermission, id)));
    }

    @Override
    public Optional<Root> deleteRoot(long id) throws DAOException {
        throw new RuntimeException("not implemented");
    }
}
