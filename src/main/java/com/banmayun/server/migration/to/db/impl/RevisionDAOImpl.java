package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.banmayun.server.migration.to.core.Revision;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.RevisionDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class RevisionDAOImpl extends AbstractDAO implements RevisionDAO {

    protected static final String TABLE_NAME = "revisions";
    protected static final String TABLE_ALIAS = "_revision_";
    protected static final String[] COLUMN_NAMES = new String[] { "meta_id", "version", "root_id", "md5", "bytes",
            "modified_at", "modified_by", "client_modified_at" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Revision parseRevision(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Revision ret = new Revision();
        ret.setMetaId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setVersion((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setMD5((String) arg.get(COLUMN_ALIASES[3]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[4]));
        ret.setModifiedAt((Timestamp) arg.get(COLUMN_ALIASES[5]));
        ret.setModifiedBy((Long) arg.get(COLUMN_ALIASES[6]));
        ret.setClientModifiedAt((Timestamp) arg.get(COLUMN_ALIASES[7]));
        return ret;
    }

    public static List<Revision> parseRevisions(List<Map<String, Object>> arg) {
        List<Revision> ret = new ArrayList<Revision>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseRevision(map));
        }
        return ret;
    }

    public RevisionDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Revision createRevision(Revision revision) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING %2$s",
                TABLE_NAME, COLUMNS_INSERT, COLUMNS_RETURN);

        Revision createdRevision = parseRevision(super.uniqueResult(sql, revision.getMetaId(), revision.getVersion(),
                revision.getRootId(), revision.getMD5(), revision.getBytes(), revision.getModifiedAt(),
                revision.getModifiedBy(), revision.getClientModifiedAt()));

        sql = String.format("UPDATE %1$s %2$s SET ref_count=%2$s.ref_count+1 WHERE %2$s.md5=? AND %2$s.bytes=?",
                DataDAOImpl.TABLE_NAME, DataDAOImpl.TABLE_ALIAS);
        super.update(sql, revision.getMD5(), revision.getBytes());

        return createdRevision;
    }

    @Override
    public Optional<Revision> getRevision(long rootId, long metaId, long version) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? "
                + "AND %2$s.version=?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseRevision(super.uniqueResult(sql, rootId, metaId, version)));
    }

    @Override
    public int countRevisionsForMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=?",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId, metaId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Revision> listRevisionsForMeta(long rootId, long metaId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? "
                + "ORDER BY %2$s.version DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return parseRevisions(super.list(sql, rootId, metaId, offset, limit));
    }

    @Override
    public Optional<Revision> deleteRevision(long rootId, long metaId, long version) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? AND %2$s.version=? "
                + "RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseRevision(super.uniqueResult(sql, rootId, metaId, version)));
    }

    @Override
    public void deleteRevisionsModifiedBefore(Timestamp time) throws DAOException {
        // decrement ref_count for data objects
        String sql = String.format("WITH tmp AS (SELECT %2$s.md5, %2$s.bytes, COUNT(%2$s) AS count FROM %1$s %2$s "
                + "WHERE %2$s.modified_at<? GROUP BY %2$s.md5, %2$s.bytes) UPDATE %3$s %4$s "
                + "SET ref_count=%4$s.ref_count-tmp.count FROM tmp WHERE %4$s.md5=tmp.md5 AND %4$s.bytes=tmp.bytes",
                TABLE_NAME, TABLE_ALIAS, DataDAOImpl.TABLE_NAME, DataDAOImpl.TABLE_ALIAS);
        super.update(sql, time);

        // delete
        sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.modified_at<?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, time);
    }
}
