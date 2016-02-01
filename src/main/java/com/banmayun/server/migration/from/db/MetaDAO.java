/* ****************************************************************************
 * MEEPOTECH CONFIDENTIAL
 * ----------------------
 * [2013] - [2014] MeePo Technology Incorporated
 * All Rights Reserved.
 *
 * IMPORTANT NOTICE:
 * All information contained herein is, and remains the property of MeePo
 * Technology Incorporated and its suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to MeePo Technology
 * Incorporated and its suppliers and may be covered by Chinese and Foreign
 * Patents, patents in process, and are protected by trade secret or copyright
 * law. Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained from
 * MeePo Technology Incorporated.
 * ****************************************************************************
 */

package com.banmayun.server.migration.from.db;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.cli.PathUtils;
import com.banmayun.server.migration.from.core.Meta;
import com.google.common.base.Optional;

public class MetaDAO extends AbstractDAO {

    public static final long MOST_RECENT_VERSION = 0L;
    public static final long INITIAL_VERSION = 0L;
    public static final long DEFAULT_NONCE = 0L;
    public static final long ALL_BYTES = -1L;

    protected static final String[] COLUMN_NAMES = new String[] { "file_id", "group_id", "root_id", "path", "nonce",
            "parent_path", "name", "is_dir", "version", "md5", "bytes", "created", "created_by", "modified",
            "modified_by", "client_modified" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "m";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Meta parseMeta(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Meta ret = new Meta();
        ret.setFileId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setPath((String) arg.get(COLUMN_ALIASES[3]));
        ret.setNonce((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setParentPath((String) arg.get(COLUMN_ALIASES[5]));
        ret.setName((String) arg.get(COLUMN_ALIASES[6]));
        ret.setIsDir((Boolean) arg.get(COLUMN_ALIASES[7]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[8]));
        ret.setMD5((String) arg.get(COLUMN_ALIASES[9]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[10]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[11]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[12]));
        ret.setModified((Timestamp) arg.get(COLUMN_ALIASES[13]));
        ret.setModifiedBy((Long) arg.get(COLUMN_ALIASES[14]));
        ret.setClientModified((Timestamp) arg.get(COLUMN_ALIASES[15]));
        return ret;
    }

    public static Meta parseMetaWithTrash(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Meta ret = new Meta();
        ret.setFileId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setPath((String) arg.get(COLUMN_ALIASES[3]));
        ret.setNonce((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setParentPath((String) arg.get(COLUMN_ALIASES[5]));
        ret.setName((String) arg.get(COLUMN_ALIASES[6]));
        ret.setIsDir((Boolean) arg.get(COLUMN_ALIASES[7]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[8]));
        ret.setMD5((String) arg.get(COLUMN_ALIASES[9]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[10]));
        ret.setCreated((Timestamp) arg.get(TrashDAO.COLUMN_ALIASES[3]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[12]));
        ret.setModified((Timestamp) arg.get(COLUMN_ALIASES[13]));
        ret.setModifiedBy((Long) arg.get(COLUMN_ALIASES[14]));
        ret.setClientModified((Timestamp) arg.get(COLUMN_ALIASES[15]));
        return ret;
    }

    public static List<Meta> parseMetas(List<Map<String, Object>> arg) {
        List<Meta> ret = new ArrayList<Meta>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseMeta(map));
        }
        return ret;
    }

    public static List<Meta> parseMetasWithTrash(List<Map<String, Object>> arg) {
        List<Meta> ret = new ArrayList<Meta>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseMetaWithTrash(map));
        }
        return ret;
    }

    public MetaDAO(Connection conn) {
        super(conn);
    }

    public Meta create(Meta meta) throws SQLException {
        String sql = "INSERT INTO tbl_meta (" + COLUMNS_INSERT + ") "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING " + COLUMNS_RETURN;
        return parseMeta(super.uniqueResult(sql, meta.getGroupId(), meta.getRootId(), meta.getPath(), meta.getNonce(),
                meta.getParentPath(), meta.getName(), meta.getIsDir(), meta.getVersion(), meta.getMD5(),
                meta.getBytes(), meta.getCreated(), meta.getCreatedBy(), meta.getModified(), meta.getModifiedBy(),
                meta.getClientModified()));
    }

    public List<Meta> listByGroup(long groupId, long rootId, int offset, int limit) throws SQLException {
        String sql = "SELECT "
                + COLUMNS_SELECT
                + " FROM tbl_meta m WHERE m.group_id=? AND m.root_id=? ORDER BY m.version, m.file_id DESC OFFSET ? LIMIT ?";
        return parseMetas(super.list(sql, groupId, rootId, offset, limit));
    }
    
    public int countByGroup(long groupId, long rootId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_meta m where m.group_id=? AND m.root_id=?";
        Map<String, Object> ret = super.uniqueResult(sql, groupId, rootId);
        return ((Long) ret.get("count")).intValue();
    }

    public int count() throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_meta m where m.nonce = 0";
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    public int countMetaByGroup(long groupId, long rootId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_meta m where m.group_id=? AND m.root_id=? AND m.nonce = 0";
        Map<String, Object> ret = super.uniqueResult(sql, groupId, rootId);
        return ((Long) ret.get("count")).intValue();
    }

    public int countFileByGroup(long groupId, long rootId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_meta m where m.group_id=? AND m.root_id=? AND m.nonce = 0 AND m.is_dir=false";
        Map<String, Object> ret = super.uniqueResult(sql, groupId, rootId);
        return ((Long) ret.get("count")).intValue();
    }

    public Optional<Meta> get(long fileId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_meta m " + "WHERE m.file_id=?";
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, fileId)));
    }

    public Optional<Meta> getByPath(long groupId, long rootId, String path) throws SQLException {
        return this.getByPath(groupId, rootId, path, DEFAULT_NONCE);
    }

    public Optional<Meta> getByPath(long groupId, long rootId, String path, long nonce) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_meta m "
                + "WHERE m.group_id=? AND m.root_id=? AND lower(m.path)=lower(?) " + "AND m.nonce=?";
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, groupId, rootId, path, nonce)));
    }

    public List<Meta> listByParent(long groupId, long rootId, String parentPath) throws SQLException {
        return this.listByParent(groupId, rootId, parentPath, DEFAULT_NONCE);
    }

    public List<Meta> listByParent(long groupId, long rootId, String parentPath, long nonce) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_meta m " + "WHERE m.group_id=? AND m.root_id=? "
                + "AND lower(m.parent_path)=lower(?) AND m.nonce=?";
        return parseMetas(super.list(sql, groupId, rootId, parentPath, nonce));
    }

    public List<Meta> listByGroup(long groupId, long rootId, int offset, int limit, long nonce) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_meta m " + "WHERE m.group_id=? AND m.root_id=? "
                + "AND m.nonce=? ORDER BY m.version, m.file_id DESC OFFSET ? LIMIT ?";
        return parseMetas(super.list(sql, groupId, rootId, nonce, offset, limit));
    }

    public List<Meta> deltaBetween(long groupId, long rootId, long versionLow, long versionHigh, int offset, int limit)
            throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_meta m " + "WHERE m.group_id=? AND m.root_id=? "
                + "AND m.version>? AND m.version<=? " + "ORDER BY m.version, m.file_id OFFSET ? LIMIT ?";
        return parseMetas(super.list(sql, groupId, rootId, versionLow, versionHigh, offset, limit));
    }

    public List<Meta> deltaAfter(long groupId, long rootId, long version, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_meta m "
                + "WHERE m.group_id=? AND m.root_id=? AND m.version>? "
                + "ORDER BY m.version, m.file_id OFFSET ? LIMIT ?";
        return parseMetas(super.list(sql, groupId, rootId, version, offset, limit));
    }

    public long total(long groupId, long rootId, String path) throws SQLException {
        return this.total(groupId, rootId, path, MetaDAO.DEFAULT_NONCE);
    }

    public long total(long groupId, long rootId, String path, long nonce) throws SQLException {
        String sql = "SELECT SUM(m.bytes) FROM tbl_meta m "
                + "WHERE m.group_id=? AND m.root_id=? AND (lower(m.path)=lower(?) "
                + "OR lower(m.path) LIKE lower(?)) AND m.nonce=?";
        Map<String, Object> ret = super.uniqueResult(sql, groupId, rootId, path, path + PathUtils.SEPARATOR + "%",
                nonce);
        if (((BigDecimal) ret.get("sum")) != null) {
            return ((BigDecimal) ret.get("sum")).longValue();
        } else {
            return 0;
        }
    }

    public Optional<Meta> update(long fileId, Meta update) throws SQLException {
        Meta meta = this.get(fileId).orNull();
        if (meta == null) {
            return Optional.absent();
        }
        String sql = "UPDATE tbl_meta m SET " + COLUMNS_UPDATE + " WHERE m.file_id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, update.getFileId() == null ? meta.getFileId()
                : update.getFileId(), update.getGroupId() == null ? meta.getGroupId() : update.getGroupId(), update
                .getRootId() == null ? meta.getRootId() : update.getRootId(), update.getPath() == null ? meta.getPath()
                : update.getPath(), update.getNonce() == null ? meta.getNonce() : update.getNonce(), update
                .getParentPath() == null ? meta.getParentPath() : update.getParentPath(),
                update.getName() == null ? meta.getName() : update.getName(),
                update.getIsDir() == null ? meta.getIsDir() : update.getIsDir(),
                update.getVersion() == null ? meta.getVersion() : update.getVersion(),
                update.getMD5() == null ? meta.getMD5() : update.getMD5(), update.getBytes() == null ? meta.getBytes()
                        : update.getBytes(), update.getCreated() == null ? meta.getCreated() : update.getCreated(),
                update.getCreatedBy() == null ? meta.getCreatedBy() : update.getCreatedBy(),
                update.getModified() == null ? meta.getModified() : update.getModified(),
                update.getModifiedBy() == null ? meta.getModifiedBy() : update.getModifiedBy(), update
                        .getClientModified() == null ? meta.getClientModified() : update.getClientModified(), fileId)));
    }

    public Optional<Meta> copy(long groupId, long rootId, String path, String toPath, long version, Timestamp time,
            long operatorId) throws SQLException {
        if (PathUtils.directoryContains(path, toPath) || PathUtils.directoryContains(toPath, path)) {
            return Optional.absent();
        }

        return this.copy(groupId, rootId, path, DEFAULT_NONCE, toPath, DEFAULT_NONCE, version, time, operatorId);
    }

    private Optional<Meta> copy(long groupId, long rootId, String path, long nonce, String toPath, long toNonce,
            long version, Timestamp time, long operatorId) throws SQLException {
        Optional<Meta> ret = this.copyOne(groupId, rootId, path, nonce, toPath, toNonce, version, time, operatorId);
        if (!ret.isPresent()) {
            return ret;
        }

        // increment refs
        String sql = "WITH s AS " + "(SELECT m.md5, m.bytes, COUNT(m) AS count FROM tbl_meta m "
                + "WHERE m.group_id=? AND m.root_id=? " + "AND lower(m.path) LIKE lower(?) AND m.nonce=? "
                + "GROUP BY m.md5, m.bytes) " + "UPDATE tbl_data d SET refs=d.refs+s.count FROM s "
                + "WHERE d.md5=s.md5 AND d.bytes=s.bytes";
        super.update(sql, groupId, rootId, path + PathUtils.SEPARATOR + "%", nonce);

        sql = String.format("INSERT INTO tbl_meta (" + COLUMNS_INSERT + ") " + "SELECT m.group_id, m.root_id, "
                + "regexp_replace(m.path, '(.{%d})(.*)', E'%s\\\\2'), ?, "
                + "regexp_replace(m.parent_path, '(.{%d}(.*))', E'%s\\\\2'), "
                + "m.name, m.is_dir, ?, m.md5, m.bytes, ?, ?, " + "m.modified, m.modified_by, m.client_modified "
                + "FROM tbl_meta m WHERE m.group_id=? AND m.root_id=? "
                + "AND lower(m.path) LIKE lower(?) AND m.nonce=?", path.length(), toPath, path.length(), toPath);
        super.update(sql, toNonce, version, time, operatorId, groupId, rootId, path + PathUtils.SEPARATOR + "%", nonce);

        return ret;
    }

    public Optional<Meta> trash(long groupId, long rootId, String path, long toNonce, long version) throws SQLException {
        return this.trash(groupId, rootId, path, DEFAULT_NONCE, toNonce, version);
    }

    public Optional<Meta> restore(long groupId, long rootId, String path, long nonce, long version) throws SQLException {
        return this.trash(groupId, rootId, path, nonce, DEFAULT_NONCE, version);
    }

    public Optional<Meta> restoreTo(long groupId, long rootId, String path, long nonce, String toPath, long version,
            Timestamp time, long operatorId) throws SQLException {
        return this.move(groupId, rootId, path, nonce, toPath, DEFAULT_NONCE, nonce, version, time, operatorId);
    }

    private Optional<Meta> trash(long groupId, long rootId, String path, long nonce, long toNonce, long version)
            throws SQLException {
        String sql = "UPDATE tbl_meta m SET nonce=?, version=? "
                + "WHERE m.group_id=? AND m.root_id=? AND lower(m.path)=lower(?) " + "AND m.nonce=? RETURNING "
                + COLUMNS_RETURN;
        Optional<Meta> ret = Optional.fromNullable(parseMeta(super.uniqueResult(sql, toNonce, version, groupId, rootId,
                path, nonce)));
        if (!ret.isPresent()) {
            return ret;
        }

        sql = "UPDATE tbl_meta m SET nonce=? " + "WHERE m.group_id=? AND m.root_id=? "
                + "AND lower(m.path) LIKE lower(?) " + "AND m.nonce=?";
        super.update(sql, toNonce, groupId, rootId, path + PathUtils.SEPARATOR + "%", nonce);
        return ret;
    }

    public Optional<Meta> move(long groupId, long rootId, String path, String toPath, long stubNonce, long version,
            Timestamp time, long operatorId) throws SQLException {
        if (PathUtils.directoryContains(path, toPath) || PathUtils.directoryContains(toPath, path)) {
            return Optional.absent();
        }

        assert stubNonce != DEFAULT_NONCE;
        return this.move(groupId, rootId, path, DEFAULT_NONCE, toPath, DEFAULT_NONCE, stubNonce, version, time,
                operatorId);
    }

    private Optional<Meta> move(long groupId, long rootId, String path, long nonce, String toPath, long toNonce,
            long stubNonce, long version, Timestamp time, long operatorId) throws SQLException {
        String toParentPath = PathUtils.getParentPath(toPath);
        String toName = PathUtils.getName(toPath);

        String sql = "UPDATE tbl_meta m SET path=?, nonce=?, parent_path=?, "
                + "name=?, version=?, created=?, created_by=? "
                + "WHERE m.group_id=? AND m.root_id=? AND lower(m.path)=lower(?) " + "AND m.nonce=? RETURNING "
                + COLUMNS_RETURN;
        Optional<Meta> ret = Optional.fromNullable(parseMeta(super.uniqueResult(sql, toPath, toNonce, toParentPath,
                toName, version, time, operatorId, groupId, rootId, path, nonce)));
        if (!ret.isPresent()) {
            return ret;
        }

        sql = String.format("UPDATE tbl_meta m SET " + "path=regexp_replace(m.path, '(.{%d})(.*)', E'%s\\\\2'), "
                + "nonce=?, " + "parent_path=regexp_replace(m.parent_path, '(.{%d}(.*))', " + "E'%s\\\\2'), version=? "
                + "WHERE m.group_id=? AND m.root_id=? " + "AND lower(m.path) LIKE lower(?) AND m.nonce=?",
                path.length(), toPath, path.length(), toPath);
        super.update(sql, toNonce, version, groupId, rootId, path + PathUtils.SEPARATOR + "%", nonce);

        // copy a stub back
        this.copyOne(groupId, rootId, toPath, nonce, path, stubNonce, version, time, operatorId);
        return ret;
    }

    private Optional<Meta> copyOne(long groupId, long rootId, String path, long nonce, String toPath, long toNonce,
            long version, Timestamp time, long operatorId) throws SQLException {
        // increment refs
        String sql = "UPDATE tbl_data d SET refs=d.refs+1 " + "WHERE (d.md5, d.bytes) IN "
                + "(SELECT m.md5, m.bytes FROM tbl_meta m "
                + "WHERE m.group_id=? AND m.root_id=? AND lower(m.path)=lower(?) " + "AND m.nonce=?)";
        super.update(sql, groupId, rootId, path, nonce);

        String toParentPath = PathUtils.getParentPath(toPath);
        String toName = PathUtils.getName(toPath);

        sql = "INSERT INTO tbl_meta (" + COLUMNS_INSERT + ") "
                + "SELECT m.group_id, m.root_id, ?, ?, ?, ?, m.is_dir, ?, m.md5, "
                + "m.bytes, ?, ?, m.modified, m.modified_by, m.client_modified "
                + "FROM tbl_meta m WHERE m.group_id=? AND m.root_id=? " + "AND lower(m.path)=lower(?) AND m.nonce=? "
                + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseMeta(super.uniqueResult(sql, toPath, toNonce, toParentPath, toName, version,
                time, operatorId, groupId, rootId, path, nonce)));
    }

    public void deleteTrashByVersionBelowOrEqual(long groupId, long rootId, long version) throws SQLException {
        // decrement refs
        String sql = "WITH s AS " + "(SELECT m.md5, m.bytes, COUNT(m) AS count FROM tbl_meta m "
                + "WHERE m.group_id=? AND m.root_id=? AND m.version<=? AND m.nonce!=? " + "GROUP BY m.md5, m.bytes) "
                + "UPDATE tbl_data d SET refs=d.refs-s.count FROM s " + "WHERE d.md5=s.md5 AND d.bytes=s.bytes";
        super.update(sql, groupId, rootId, version, DEFAULT_NONCE);

        sql = "DELETE FROM tbl_meta m " + "WHERE m.group_id=? AND m.root_id=? AND m.version<=? AND m.nonce!=? ";
        super.update(sql, groupId, rootId, version, DEFAULT_NONCE);
    }

    public List<Map<String, Object>> checkDataRefs() throws SQLException {
        String sql = "WITH s AS " + "(SELECT m.md5, m.bytes, COUNT(m) AS count FROM tbl_meta m "
                + "GROUP BY m.md5, m.bytes) " + "SELECT d.md5 AS md5, d.bytes AS bytes, d.refs AS refs, count "
                + "FROM tbl_data d, s " + "WHERE (NOT s.md5 is NULL) "
                + "AND ((d.md5=s.md5 AND d.bytes=s.bytes AND d.refs<count) " + "  OR NOT EXISTS "
                + "(SELECT 1 FROM tbl_data d WHERE d.md5=s.md5 AND d.bytes=s.bytes))";
        return super.list(sql);
    }
    
    public int countValidMetasForValidUsers() throws SQLException {
    	String sql = "SELECT count(*) FROM tbl_meta m INNER JOIN tbl_user u ON m.root_id=u.id";
    	Map<String, Object> ret = super.uniqueResult(sql);
    	return ((Long)ret.get("count")).intValue();
    }
    
    public int countValidMetasForValidGroups() throws SQLException {
    	String sql = "SELECT count(*) FROM tbl_meta m INNER JOIN tbl_group g ON m.root_id=g.id";
    	Map<String, Object> ret = super.uniqueResult(sql);
    	return ((Long)ret.get("count")).intValue();
    }
}
