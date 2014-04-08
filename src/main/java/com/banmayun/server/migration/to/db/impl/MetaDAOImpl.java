package com.banmayun.server.migration.to.db.impl;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.cli.PathUtils;
import com.banmayun.server.migration.to.core.Data;
import com.banmayun.server.migration.to.core.Meta;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.MetaDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class MetaDAOImpl extends AbstractDAO implements MetaDAO {

    protected static final String TABLE_NAME = "metas";
    protected static final String TABLE_ALIAS = "_meta_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "version", "root_id", "path", "nonce",
            "parent_path", "name", "is_dir", "md5", "bytes", "perm", "created_at", "created_by", "modified_at",
            "modified_by", "client_modified_at", "comment_count", "share_count" };
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

    public static Meta parseMeta(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Meta ret = new Meta();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setPath((String) arg.get(COLUMN_ALIASES[3]));
        ret.setNonce((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setParentPath((String) arg.get(COLUMN_ALIASES[5]));
        ret.setName((String) arg.get(COLUMN_ALIASES[6]));
        ret.setIsDir((Boolean) arg.get(COLUMN_ALIASES[7]));
        ret.setMD5((String) arg.get(COLUMN_ALIASES[8]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[9]));
        ret.setPermission((String) arg.get(COLUMN_ALIASES[10]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[11]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[12]));
        ret.setModifiedAt((Timestamp) arg.get(COLUMN_ALIASES[13]));
        ret.setModifiedBy((Long) arg.get(COLUMN_ALIASES[14]));
        ret.setClientModifiedAt((Timestamp) arg.get(COLUMN_ALIASES[15]));
        ret.setCommentCount((Integer) arg.get(COLUMN_ALIASES[16]));
        ret.setShareCount((Integer) arg.get(COLUMN_ALIASES[17]));
        return ret;
    }

    public static List<Meta> parseMetaList(List<Map<String, Object>> arg) {
        List<Meta> ret = new ArrayList<Meta>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseMeta(map));
        }
        return ret;
    }

    public MetaDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Meta createMeta(Meta meta) throws UniqueViolationException, DAOException {
        if (meta.getIsDir()) {
            meta.setBytes(0L);
            meta.setMD5(null);
        } else {
            if (meta.getMD5() == null) {
                throw new NullPointerException("md5 must not be null for file");
            }
        }

        String path = PathUtils.normalize(meta.getPath());
        String parentPath = PathUtils.getParentPath(path);
        String name = PathUtils.getName(path);
        meta.setPath(path);
        // meta.setNonce(0L);
        meta.setParentPath(parentPath);
        meta.setName(name);

        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "RETURNING %3$s", TABLE_NAME, COLUMNS_INSERT, COLUMNS_RETURN);
        Meta createdMeta = parseMeta(super.uniqueResult(sql, meta.getVersion(), meta.getRootId(), meta.getPath(),
                meta.getNonce(), meta.getParentPath(), meta.getName(), meta.getIsDir(), meta.getMD5(), meta.getBytes(),
                meta.getPermission(), meta.getCreatedAt(), meta.getCreatedBy(), meta.getModifiedAt(),
                meta.getModifiedBy(), meta.getClientModifiedAt()));

        if (createdMeta.getIsDir()) {
            return createdMeta;
        }

        this.adjustCountsForPath(createdMeta.getRootId(), createdMeta.getPath(), Meta.DEFAULT_NONCE, false);
        return createdMeta;
    }

    private void adjustDataRefCountForPath(long rootId, String path, long nonce, boolean isMinus) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %2$s.md5, %2$s.bytes, COUNT(%2$s) AS count FROM %1$s %2$s "
                + "WHERE %2$s.root_id=? AND lower(%2$s.path)=lower(?) AND %2$s.nonce=? AND %2$s.is_dir=FALSE "
                + "GROUP BY %2$s.md5, %2$s.bytes) UPDATE %3$s %4$s SET ref_count=%4$s.ref_count%5$stmp.count FROM tmp "
                + "WHERE %4$s.md5=tmp.md5 AND %4$s.bytes=tmp.bytes", TABLE_NAME, TABLE_ALIAS, DataDAOImpl.TABLE_NAME,
                DataDAOImpl.TABLE_ALIAS, isMinus ? "-" : "+");
        super.update(sql, rootId, path, nonce);
    }

    private void adjustDataRefCountRecursivelyUnderPath(long rootId, String path, long nonce, boolean isMinus)
            throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %2$s.md5, %2$s.bytes, COUNT(%2$s) AS count FROM %1$s %2$s "
                + "WHERE %2$s.root_id=? AND lower(%2$s.path) LIKE lower(?) AND %2$s.nonce=? AND %2$s.is_dir=FALSE "
                + "GROUP BY %2$s.md5, %2$s.bytes) UPDATE %3$s %4$s SET ref_count=%4$s.ref_count%5$stmp.count FROM tmp "
                + "WHERE %4$s.md5=tmp.md5 AND %4$s.bytes=tmp.bytes", TABLE_NAME, TABLE_ALIAS, DataDAOImpl.TABLE_NAME,
                DataDAOImpl.TABLE_ALIAS, isMinus ? "-" : "+");
        super.update(sql, rootId, path + PathUtils.SEPARATOR + "%", nonce);
    }

    private void adjustRootByteCountForPath(long rootId, String path, long nonce, boolean isMinus) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %2$s.root_id AS root_id, "
                + "coalesce(SUM(%2$s.bytes), 0) AS byte_count FROM %1$s %2$s WHERE %2$s.root_id=? "
                + "AND lower(%2$s.path)=lower(?) AND %2$s.nonce=? AND %2$s.is_dir=FALSE GROUP BY %2$s.root_id) "
                + "UPDATE %3$s %4$s SET byte_count=%4$s.byte_count%5$stmp.byte_count "
                + "FROM tmp WHERE %4$s.id=tmp.root_id", TABLE_NAME, TABLE_ALIAS, RootDAOImpl.TABLE_NAME,
                RootDAOImpl.TABLE_ALIAS, isMinus ? "-" : "+");
        super.update(sql, rootId, path, nonce);
    }

    private void adjustRootByteCountAndFileCountForPath(long rootId, String path, long nonce, boolean isMinus)
            throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %2$s.root_id AS root_id, COUNT(%2$s) AS file_count, "
                + "coalesce(SUM(%2$s.bytes), 0) AS byte_count FROM %1$s %2$s WHERE %2$s.root_id=? "
                + "AND lower(%2$s.path)=lower(?) AND %2$s.nonce=? AND %2$s.is_dir=FALSE GROUP BY %2$s.root_id) "
                + "UPDATE %3$s %4$s SET file_count=%4$s.file_count%5$stmp.file_count, "
                + "byte_count=%4$s.byte_count%5$stmp.byte_count FROM tmp WHERE %4$s.id=tmp.root_id", TABLE_NAME,
                TABLE_ALIAS, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS, isMinus ? "-" : "+");
        super.update(sql, rootId, path, nonce);
    }

    private void adjustRootByteCountAndFileCountRecursivelyUnderPath(long rootId, String path, long nonce,
            boolean isMinus) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %2$s.root_id AS root_id, COUNT(%2$s) AS file_count, "
                + "coalesce(SUM(%2$s.bytes), 0) AS byte_count FROM %1$s %2$s WHERE %2$s.root_id=? "
                + "AND lower(%2$s.path) LIKE lower(?) AND %2$s.nonce=? AND %2$s.is_dir=FALSE GROUP BY %2$s.root_id) "
                + "UPDATE %3$s %4$s SET file_count=%4$s.file_count%5$stmp.file_count, "
                + "byte_count=%4$s.byte_count%5$stmp.byte_count FROM tmp WHERE %4$s.id=tmp.root_id", TABLE_NAME,
                TABLE_ALIAS, RootDAOImpl.TABLE_NAME, RootDAOImpl.TABLE_ALIAS, isMinus ? "-" : "+");
        super.update(sql, rootId, path + PathUtils.SEPARATOR + "%", nonce);
    }

    private void adjustRootByteCountAndFileCountRecursivelyForPath(long rootId, String path, long nonce, boolean isMinus)
            throws DAOException {
        this.adjustRootByteCountAndFileCountForPath(rootId, path, nonce, isMinus);
        this.adjustRootByteCountAndFileCountRecursivelyUnderPath(rootId, path, nonce, isMinus);
    }

    private void adjustCountsForPath(long rootId, String path, long nonce, boolean isMinus) throws DAOException {
        this.adjustDataRefCountForPath(rootId, path, nonce, isMinus);
        this.adjustRootByteCountAndFileCountForPath(rootId, path, nonce, isMinus);
    }

    private void adjustCountsRecursivelyUnderPath(long rootId, String path, long nonce, boolean isMinus)
            throws DAOException {
        this.adjustDataRefCountRecursivelyUnderPath(rootId, path, nonce, isMinus);
        this.adjustRootByteCountAndFileCountRecursivelyUnderPath(rootId, path, nonce, isMinus);
    }

    @Override
    public Optional<Meta> findMetaByPath(long rootId, String path) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND lower(%2$s.path)=lower(?) "
                + "AND %2$s.nonce=?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, rootId, path, Meta.DEFAULT_NONCE)));
    }

    @Override
    public Optional<Meta> getMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.id=? AND %2$s.nonce=?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, rootId, metaId, Meta.DEFAULT_NONCE)));
    }

    @Override
    public int countMetas() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.nonce=?", TABLE_NAME,
                TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, Meta.DEFAULT_NONCE);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countMetasForRoot(long rootId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.nonce=?",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId, Meta.DEFAULT_NONCE);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countFiles() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.nonce=? AND %2$s.is_dir=FALSE",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, Meta.DEFAULT_NONCE);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countFilesForRoot(long rootId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.nonce=? "
                + "AND %2$s.is_dir=FALSE", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId, Meta.DEFAULT_NONCE);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countFolders() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.nonce=? AND %2$s.is_dir=TRUE",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, Meta.DEFAULT_NONCE);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countFoldersForRoot(long rootId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.nonce=? "
                + "AND %2$s.is_dir=TRUE", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId, Meta.DEFAULT_NONCE);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public long countBytes() throws DAOException {
        String sql = String.format("SELECT coalesce(SUM(%2$s.bytes), 0) AS sum FROM %1$s %2$s WHERE %2$s.nonce=?",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, Meta.DEFAULT_NONCE);
        return ((BigDecimal) ret.get("sum")).longValue();
    }

    @Override
    public long countBytesForRoot(long rootId) throws DAOException {
        String sql = String.format("SELECT coalesce(SUM(%2$s.bytes), 0) AS sum FROM %1$s %2$s WHERE %2$s.root_id=? "
                + "AND %2$s.nonce=?", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId, Meta.DEFAULT_NONCE);
        return ((BigDecimal) ret.get("sum")).longValue();
    }

    @Override
    public List<Meta> listMetas(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.nonce=? ORDER BY %2$s.id DESC "
                + "OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseMetaList(super.list(sql, Meta.DEFAULT_NONCE, offset, limit));
    }

    @Override
    public List<Meta> listMetasByPath(long rootId, String path) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? "
                + "AND lower(%2$s.parent_path)=lower(?) AND %2$s.nonce=?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseMetaList(super.list(sql, rootId, path, Meta.DEFAULT_NONCE));
    }

    @Override
    public List<Meta> topMetasByCommentCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.nonce=? ORDER BY %2$s.comment_count DESC, "
                + "%2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseMetaList(super.list(sql, Meta.DEFAULT_NONCE, offset, limit));
    }

    @Override
    public List<Meta> topMetasByCommentCountForRoot(long rootId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.nonce=? "
                + "ORDER BY %2$s.comment_count DESC, %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return parseMetaList(super.list(sql, rootId, Meta.DEFAULT_NONCE, offset, limit));
    }

    @Override
    public List<Meta> topFilesByShareCount(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.nonce=? AND %2$s.is_dir=FALSE "
                + "ORDER BY %2$s.share_count DESC, %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return parseMetaList(super.list(sql, Meta.DEFAULT_NONCE, offset, limit));
    }

    @Override
    public List<Meta> topFilesByShareCountForRoot(long rootId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.is_dir=FALSE "
                + "AND %2$s.nonce=? ORDER BY %2$s.share_count DESC, %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_SELECT);
        return parseMetaList(super.list(sql, rootId, Meta.DEFAULT_NONCE, offset, limit));
    }

    @Override
    public Optional<Meta> setMetaPermission(long rootId, long metaId, String permission) throws DAOException {
        String sql = String.format(
                "UPDATE %1$s %2$s SET perm=? WHERE %2$s.root_id=? AND %2$s.id=? " + "RETURNING %3$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, permission, rootId, metaId)));
    }

    @Override
    public Optional<Meta> commitFileModification(long rootId, long metaId, long userId, Timestamp clientModifiedAt,
            Data data) throws DAOException {
        Meta meta = this.getMeta(rootId, metaId).orNull();
        if (meta == null) {
            return Optional.absent();
        }

        // decrement byte_count
        this.adjustRootByteCountForPath(meta.getRootId(), meta.getPath(), meta.getNonce(), true);
        // decrement ref_count
        this.adjustDataRefCountForPath(meta.getRootId(), meta.getPath(), meta.getNonce(), true);

        String sql = String.format("UPDATE %1$s %2$s SET ref_count=%2$s.ref_count-1 WHERE %2$s.md5=? AND %2$s.bytes=?",
                DataDAOImpl.TABLE_NAME, DataDAOImpl.TABLE_ALIAS);
        super.update(sql, meta.getMD5(), meta.getBytes());

        // update meta
        sql = String.format(
                "UPDATE %1$s %2$s SET version=nextval('metas_version_seq'), md5=?, bytes=?, modified_at=?, "
                        + "modified_by=?, client_modified_at=? WHERE %2$s.root_id=? AND %2$s.id=? RETURNING %3$s",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        Optional<Meta> ret = Optional.fromNullable(parseMeta(super.uniqueResult(sql, data.getMD5(), data.getBytes(),
                new Timestamp(System.currentTimeMillis()), userId, clientModifiedAt, rootId, metaId)));

        // increment ref_count
        this.adjustDataRefCountForPath(meta.getRootId(), meta.getPath(), meta.getNonce(), false);
        // increment byte_count
        this.adjustRootByteCountForPath(meta.getRootId(), meta.getPath(), meta.getNonce(), false);

        return ret;
    }

    @Override
    public Optional<Meta> utimeFolder(long rootId, long metaId, Timestamp clientModifiedAt) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET version=nextval('metas_version_seq'), client_modified_at=? "
                + "WHERE %2$s.root_id=? AND %2$s.id=? AND %2$s.is_dir=TRUE RETURNING %3$s", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_RETURN);
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, clientModifiedAt, rootId, metaId)));
    }

    @Override
    public Optional<Meta> copyMetasRecursivelyByPath(long rootId, String path, String toPath, long userId)
            throws UniqueViolationException, DAOException {
        path = PathUtils.normalize(path);
        toPath = PathUtils.normalize(toPath);
        String toParentPath = PathUtils.getParentPath(toPath);
        String toName = PathUtils.getName(toPath);

        // copy
        String sql = String.format("INSERT INTO %1$s (%3$s) SELECT %2$s.root_id, ?, ?, ?, ?, %2$s.is_dir, "
                + "%2$s.md5, %2$s.bytes, ?, ?, ?, %2$s.modified_at, %2$s.modified_by, %2$s.client_modified_at "
                + "FROM %1$s %2$s WHERE %2$s.root_id=? AND lower(%2$s.path)=lower(?) AND %2$s.nonce=? RETURNING %4$s",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_INSERT, COLUMNS_RETURN);
        Optional<Meta> ret = Optional.fromNullable(parseMeta(super.uniqueResult(sql, toPath, Meta.DEFAULT_NONCE,
                toParentPath, toName, null, new Timestamp(System.currentTimeMillis()), userId, rootId, path,
                Meta.DEFAULT_NONCE)));
        if (!ret.isPresent()) {
            return ret;
        }
        this.adjustCountsForPath(rootId, toPath, Meta.DEFAULT_NONCE, false);

        // copy recursively
        sql = String.format("INSERT INTO %1$s (%3$s) SELECT %2$s.root_id, "
                + "regexp_replace(%2$s.path, '(.{%4$d}(.*))', E'%5$s\\\\2'), "
                + "?, regexp_replace(%2$s.parent_path, '(.{%4$d}(.*))', E'%5$s\\\\2'), %2$s.name, %2$s.is_dir,"
                + "%2$s.md5, %2$s.bytes, ?, ?, ?, %2$s.modified_at, %2$s.modified_by, %2$s.client_modified_at "
                + "FROM %1$s %2$s WHERE %2$s.root_id=? AND lower(%2$s.path) LIKE lower(?) AND %2$s.nonce=?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_INSERT, path.length(), toPath);
        super.update(sql, Meta.DEFAULT_NONCE, null, new Timestamp(System.currentTimeMillis()), userId, rootId, path
                + PathUtils.SEPARATOR + "%", Meta.DEFAULT_NONCE);
        this.adjustCountsRecursivelyUnderPath(rootId, toParentPath, Meta.DEFAULT_NONCE, false);

        return ret;
    }

    private Optional<Meta> moveMetasRecursivelyByPath(long rootId, String path, long nonce, String toPath,
            long toNonce, long userId) throws DAOException {
        path = PathUtils.normalize(path);
        toPath = PathUtils.normalize(toPath);
        String toParentPath = PathUtils.getParentPath(toPath);
        String toName = PathUtils.getName(toPath);

        // move
        String sql = String.format("UPDATE %1$s %2$s SET version=nextval('metas_version_seq'), path=?, nonce=?, "
                + "parent_path=?, name=? WHERE %2$s.root_id=? AND lower(%2$s.path)=lower(?) AND %2$s.nonce=? "
                + "RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        Optional<Meta> ret = Optional.fromNullable(parseMeta(super.uniqueResult(sql, toPath, toNonce, toParentPath,
                toName, rootId, path, nonce)));
        if (!ret.isPresent()) {
            return ret;
        }

        // move recursively
        sql = String.format("UPDATE %1$s %2$s SET version=nextval('metas_version_seq'), "
                + "path=regexp_replace(%2$s.path, '(.{%3$d}(.*))', E'%4$s\\\\2'), nonce=?, "
                + "parent_path=regexp_replace(%2$s.parent_path, '(.{%3$d}(.*))', E'%4$s\\\\2') "
                + "WHERE %2$s.root_id=? AND lower(%2$s.path) LIKE lower(?) AND %2$s.nonce=?", TABLE_NAME, TABLE_ALIAS,
                path.length(), toPath);
        super.update(sql, toNonce, rootId, path + PathUtils.SEPARATOR + "%", nonce);

        // store a stub
        this.copyMetaAsStub(rootId, toPath, toNonce, path, nonce == Meta.DEFAULT_NONCE ? System.currentTimeMillis()
                : nonce);
        return ret;
    }

    private Optional<Meta> copyMetaAsStub(long rootId, String path, long nonce, String toPath, long toNonce)
            throws DAOException {
        String toParentPath = PathUtils.getParentPath(toPath);
        String toName = PathUtils.getName(toPath);

        String sql = String.format("INSERT INTO %1$s (%3$s) SELECT %2$s.root_id, ?, ?, ?, ?, %2$s.is_dir, "
                + "%2$s.md5, %2$s.bytes, %2$s.perm, %2$s.created_at, %2$s.created_by, %2$s.modified_at, "
                + "%2$s.modified_by, %2$s.client_modified_at FROM %1$s %2$s "
                + "WHERE %2$s.root_id=? AND lower(%2$s.path)=lower(?) AND %2$s.nonce=? RETURNING %4$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_INSERT, COLUMNS_RETURN);
        Optional<Meta> ret = Optional.fromNullable(parseMeta(super.uniqueResult(sql, toPath, toNonce, toParentPath,
                toName, rootId, path, nonce)));
        if (!ret.isPresent()) {
            return ret;
        }
        this.adjustDataRefCountForPath(rootId, toPath, toNonce, false);

        return ret;
    }

    @Override
    public Optional<Meta> moveMetasRecursivelyByPath(long rootId, String path, String toPath, long userId)
            throws UniqueViolationException, DAOException {
        return this.moveMetasRecursivelyByPath(rootId, path, Meta.DEFAULT_NONCE, toPath, Meta.DEFAULT_NONCE, userId);
    }

    private Optional<Meta> updateNonceRecursivelyByPath(long rootId, String path, long nonce, long toNonce)
            throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET version=nextval('metas_version_seq'), nonce=? "
                + "WHERE %2$s.root_id=? AND lower(%2$s.path)=lower(?) AND %2$s.nonce=? RETURNING %3$s", TABLE_NAME,
                TABLE_ALIAS, COLUMNS_RETURN);
        Optional<Meta> ret = Optional.fromNullable(parseMeta(super.uniqueResult(sql, toNonce, rootId, path, nonce)));
        if (!ret.isPresent()) {
            return ret;
        }

        sql = String.format("UPDATE %1$s %2$s SET version=nextval('metas_version_seq'), nonce=? "
                + "WHERE %2$s.root_id=? AND lower(%2$s.path) LIKE lower(?) AND %2$s.nonce=?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, toNonce, rootId, path + PathUtils.SEPARATOR + "%", nonce);
        return ret;
    }

    @Override
    public Optional<Meta> trashMetasRecursivelyByPath(long rootId, String path, long userId) throws DAOException {
        path = PathUtils.normalize(path);
        long toNonce = System.currentTimeMillis();
        Optional<Meta> ret = this.updateNonceRecursivelyByPath(rootId, path, Meta.DEFAULT_NONCE, toNonce);
        if (!ret.isPresent()) {
            return ret;
        }
        this.adjustRootByteCountAndFileCountRecursivelyForPath(rootId, path, toNonce, true);
        return ret;
    }

    @Override
    public Optional<Meta> restoreMetasRecursivelyByPath(long rootId, String path, long nonce, long userId)
            throws DAOException {
        path = PathUtils.normalize(path);
        Optional<Meta> ret = this.updateNonceRecursivelyByPath(rootId, path, nonce, Meta.DEFAULT_NONCE);
        if (!ret.isPresent()) {
            return ret;
        }
        this.adjustRootByteCountAndFileCountRecursivelyForPath(rootId, path, Meta.DEFAULT_NONCE, false);
        return ret;
    }

    @Override
    public Optional<Meta> restoreMetasRecursivelyByPath(long rootId, String path, long nonce, String toPath, long userId)
            throws DAOException {
        path = PathUtils.normalize(path);
        toPath = PathUtils.normalize(toPath);
        Optional<Meta> ret = this.moveMetasRecursivelyByPath(rootId, path, nonce, toPath, Meta.DEFAULT_NONCE, userId);
        if (!ret.isPresent()) {
            return ret;
        }
        this.adjustRootByteCountAndFileCountRecursivelyForPath(rootId, toPath, Meta.DEFAULT_NONCE, false);
        return ret;
    }

    @Override
    public void deleteMetasTrashedBefore(Timestamp time) throws DAOException {
        // decrement ref_count
        String sql = String.format("WITH tmp AS (SELECT %2$s.md5, %2$s.bytes, COUNT(%2$s) AS count FROM %1$s %2$s "
                + "WHERE %2$s.nonce!=? AND %2$s.nonce<? GROUP BY %2$s.md5, %2$s.bytes) UPDATE %3$s %4$s "
                + "SET ref_count=%4$s.ref_count-tmp.count FROM tmp WHERE %4$s.md5=tmp.md5 AND %4$s.bytes=tmp.bytes",
                TABLE_NAME, TABLE_ALIAS, DataDAOImpl.TABLE_NAME, DataDAOImpl.TABLE_ALIAS);
        super.update(sql, Meta.DEFAULT_NONCE, time.getTime());

        // delete
        sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.nonce!=? AND %2$s.nonce<?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, Meta.DEFAULT_NONCE, time.getTime());
    }

    @Override
    public void updateRootFileCountAndByteCount() throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.id, COUNT(%2$s) AS file_count, "
                + "coalesce(SUM(%2$s.bytes), 0) AS byte_count FROM %3$s %4$s LEFT JOIN %1$s %2$s "
                + "ON %2$s.root_id=%4$s.id AND %2$s.is_dir=FALSE AND %2$s.nonce=? GROUP BY %4$s.id) "
                + "UPDATE %3$s %4$s SET file_count=tmp.file_count, byte_count=tmp.byte_count "
                + "FROM tmp WHERE %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS, RootDAOImpl.TABLE_NAME,
                RootDAOImpl.TABLE_ALIAS);
        super.update(sql, Meta.DEFAULT_NONCE);
    }

    @Override
    public void updateRootFileCountAndByteCountForRoot(long rootId) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.id, COUNT(%2$s) AS file_count, "
                + "coalesce(SUM(%2$s.bytes), 0) AS byte_count FROM %3$s %4$s WHERE %4$s.id=? LEFT JOIN %1$s %2$s "
                + "ON %2$s.root_id=%4$s.id AND %2$s.is_dir=FALSE AND %2$s.nonce=? GROUP BY %4$s.id) "
                + "UPDATE %3$s %4$s SET file_count=tmp.file_count, byte_count=tmp.byte_count "
                + "FROM tmp WHERE %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS, RootDAOImpl.TABLE_NAME,
                RootDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId, Meta.DEFAULT_NONCE);
    }

    @Override
    public List<Meta> deltaBetweenVersion(long rootId, long versionLow, long versionHigh, int offset, int limit)
            throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.version>? "
                + "AND %2$s.version<=? ORDER BY %2$s.version, %2$s.id OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return parseMetaList(super.list(sql, Meta.DEFAULT_NONCE, rootId, versionLow, versionHigh, offset, limit));
    }

    @Override
    public List<Meta> deltaAfterVersion(long rootId, long version, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.version>? "
                + "ORDER BY %2$s.version, %2$s.id OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseMetaList(super.list(sql, Meta.DEFAULT_NONCE, rootId, version, offset, limit));
    }

    @Override
    public void setVersionInitValue() throws DAOException {
        // TODO Auto-generated method stub
        String sql = String.format("SELECT setval('metas_version_seq', MAX(version)) FROM %1$s", TABLE_NAME);
        super.uniqueResult(sql);
    }
}
