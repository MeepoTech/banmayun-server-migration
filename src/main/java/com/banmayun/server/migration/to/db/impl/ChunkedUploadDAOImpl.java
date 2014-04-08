package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.to.core.ChunkedUpload;
import com.banmayun.server.migration.to.db.ChunkedUploadDAO;
import com.banmayun.server.migration.to.db.DAOException;
import com.google.common.base.Optional;

public class ChunkedUploadDAOImpl extends AbstractDAO implements ChunkedUploadDAO {

    protected static final String TABLE_NAME = "chunked_uploads";
    protected static final String TABLE_ALIAS = "_chunked_upload_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "location", "pos", "bytes", "expires_at",
            "created_at" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    protected static String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_UPDATE = DAOUtils.getColumnsUpdate(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static ChunkedUpload parseChunkedUpload(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        ChunkedUpload ret = new ChunkedUpload();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setLocation((String) arg.get(COLUMN_ALIASES[1]));
        ret.setPos((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setExpiresAt((Timestamp) arg.get(COLUMN_ALIASES[4]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[5]));
        return ret;
    }

    public static List<ChunkedUpload> parseChunkedUploads(List<Map<String, Object>> arg) {
        List<ChunkedUpload> ret = new ArrayList<ChunkedUpload>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseChunkedUpload(map));
        }
        return ret;
    }

    public ChunkedUploadDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public ChunkedUpload createChunkedUpload(ChunkedUpload chunkedUpload) throws DAOException {
        String sql = String.format("INSERT INTO %1$s %2$s (%3$s) VALUES (?, ?, ?, ?, ?) RETURNING %4$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_INSERT, COLUMNS_RETURN);
        return parseChunkedUpload(super.uniqueResult(sql, chunkedUpload.getLocation(), chunkedUpload.getPos(),
                chunkedUpload.getBytes(), chunkedUpload.getExpiresAt(), chunkedUpload.getCreatedAt()));
    }

    @Override
    public Optional<ChunkedUpload> getChunkedUpload(long chunkedUploadId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.id=?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseChunkedUpload(super.uniqueResult(sql, chunkedUploadId)));
    }

    @Override
    public Optional<ChunkedUpload> updateChunkedUpload(long chunkedUploadId, ChunkedUpload chunkedUpload)
            throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.id=? RETURNING %4$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional.fromNullable(parseChunkedUpload(super.uniqueResult(sql, chunkedUpload.getLocation(),
                chunkedUpload.getPos(), chunkedUpload.getBytes(), chunkedUpload.getExpiresAt(),
                chunkedUpload.getCreatedAt(), chunkedUploadId)));
    }

    @Override
    public Optional<ChunkedUpload> deleteChunkedUpload(long chunkedUploadId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.id=? RETURNING %3$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_RETURN);
        return Optional.fromNullable(parseChunkedUpload(super.uniqueResult(sql, chunkedUploadId)));
    }
}
