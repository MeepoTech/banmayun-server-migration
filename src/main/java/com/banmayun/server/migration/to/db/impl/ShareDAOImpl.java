package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Meta;
import com.banmayun.server.migration.to.core.Share;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.ShareDAO;
import com.google.common.base.Optional;

public class ShareDAOImpl extends AbstractDAO implements ShareDAO {

    protected static final String TABLE_NAME = "shares";
    protected static final String TABLE_ALIAS = "_share_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "root_id", "meta_id", "password",
            "expires_at", "created_at", "created_by" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_UPDATE;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 0, COLUMN_NAMES.length));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_UPDATE = DAOUtils.getColumnsUpdate(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 3, COLUMN_NAMES.length));
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Share parseShare(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Share ret = new Share();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setMetaId((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setPasswordSha256((String) arg.get(COLUMN_ALIASES[3]));
        ret.setExpiresAt((Timestamp) arg.get(COLUMN_ALIASES[4]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[5]));
        ret.setCreatedBy((Long) arg.get(COLUMN_ALIASES[6]));
        return ret;
    }

    public static List<Share> parseShares(List<Map<String, Object>> arg) {
        List<Share> ret = new ArrayList<Share>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseShare(map));
        }
        return ret;
    }

    public static Pair<Share, Meta> parseShareMeta(Map<String, Object> arg) {
        return Pair.of(parseShare(arg), MetaDAOImpl.parseMeta(arg));
    }

    public static List<Pair<Share, Meta>> parseShareMetaList(List<Map<String, Object>> arg) {
        List<Pair<Share, Meta>> ret = new ArrayList<Pair<Share, Meta>>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseShareMeta(map));
        }
        return ret;
    }

    public ShareDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Share createShare(Share share) throws DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING %3$s", TABLE_NAME,
                COLUMNS_INSERT, COLUMNS_RETURN);
        Share createdShare = parseShare(super.uniqueResult(sql, share.getId(), share.getRootId(), share.getMetaId(),
                share.getPasswordSha256(), share.getExpiresAt(), share.getCreatedAt(), share.getCreatedBy()));

        sql = String.format("UPDATE %1$s %2$s SET share_count=%2$s.share_count+1 WHERE %2$s.id=?",
                MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, createdShare.getMetaId());
        return createdShare;
    }

    @Override
    public Optional<Share> getShare(long rootId, long metaId, long shareId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? AND %2$s.id=?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseShare(super.uniqueResult(sql, rootId, metaId, shareId)));
    }

    @Override
    public Optional<Share> getShareById(long shareId) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE  %2$s.id=?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return Optional.fromNullable(parseShare(super.uniqueResult(sql, shareId)));
    }

    @Override
    public int countShares() throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countSharesForRoot(long rootId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=?", TABLE_NAME,
                TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countSharesForMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=?",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, rootId, metaId);
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Pair<Share, Meta>> listShares(int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.meta_id"
                + " ORDER By %2$s.created_at DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT,
                MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS, MetaDAOImpl.COLUMNS_SELECT);
        return parseShareMetaList(super.list(sql, offset, limit));
    }

    @Override
    public List<Pair<Share, Meta>> listSharesForRoot(long rootId, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.meta_id"
                + " WHERE %2$s.root_id=? ORDER By %2$s.created_at DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT, MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS, MetaDAOImpl.COLUMNS_SELECT);
        return parseShareMetaList(super.list(sql, rootId, offset, limit));
    }

    @Override
    public List<Pair<Share, Meta>> listSharesForMeta(long rootId, long metaId, int offset, int limit)
            throws DAOException {
        String sql = String.format("SELECT %3$s, %6$s FROM %1$s %2$s LEFT JOIN %4$s %5$s ON %5$s.id=%2$s.meta_id"
                + " WHERE %2$s.root_id=? AND %2$s.meta_id=? ORDER By %2$s.created_at DESC OFFSET ? LIMIT ?",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT, MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS,
                MetaDAOImpl.COLUMNS_SELECT);
        return parseShareMetaList(super.list(sql, rootId, metaId, offset, limit));
    }

    @Override
    public Optional<Share> updateShare(long rootId, long metaId, long shareId, Share share) throws DAOException {
        String sql = String.format("UPDATE %1$s %2$s SET %3$s WHERE %2$s.root_id=? AND %2$s.meta_id=? "
                + "AND %2$s.id=? RETURNING %4$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_UPDATE, COLUMNS_RETURN);
        return Optional.fromNullable(parseShare(super.uniqueResult(sql, share.getPasswordSha256(),
                share.getExpiresAt(), share.getCreatedAt(), share.getCreatedBy(), rootId, metaId, shareId)));
    }

    @Override
    public void deleteShares() throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s", TABLE_NAME, TABLE_ALIAS);
        super.update(sql);

        sql = String.format("UPDATE %1$s %2$s SET share_count=0", MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql);
    }

    @Override
    public void deleteSharesForRoot(long rootId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, rootId);

        sql = String.format("UPDATE %1$s %2$s SET share_count=0 WHERE %2$s.root_id=?", MetaDAOImpl.TABLE_NAME,
                MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId);
    }

    @Override
    public void deleteSharesForMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=?", TABLE_NAME,
                TABLE_ALIAS);
        super.update(sql, rootId, metaId);

        sql = String.format("UPDATE %1$s %2$s SET share_count=0 WHERE %2$s.root_id=? AND %2$s.id=?",
                MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId, metaId);
    }

    @Override
    public Optional<Share> deleteShare(long rootId, long metaId, long shareId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.root_id=? AND %2$s.meta_id=? AND %2$s.id=? "
                + "RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        Share deletedShare = parseShare(super.uniqueResult(sql, rootId, metaId, shareId));
        if (deletedShare == null) {
            return Optional.absent();
        }

        sql = String.format("UPDATE %1$s %2$s SET share_count=%2$s.share_count-1 WHERE %2$s.root_id=? AND %2$s.id=?",
                MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId, metaId);

        return Optional.of(deletedShare);
    }

    @Override
    public void deleteSharesExpiresBefore(Timestamp time) throws DAOException {
        // decrement share_count for metas
        String sql = String.format("WITH tmp (SELECT %2$s.meta_id, COUNT(*) AS count FROM %1$s %2$s "
                + "WHERE %2$s.expires_at<? GROUP BY %2$s.meta_id) UPDATE %3$s %4$s "
                + "SET share_count=%4$s.share_count-tmp.count FROM tmp WHERE %3$s.id=tmp.meta_id", TABLE_NAME,
                TABLE_ALIAS, MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, time);

        // delete
        sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.expires_at<?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, time);
    }

    @Override
    public void updateMetaShareCount() throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.meta_id=%4$s.id GROUP BY %4$s.id) UPDATE %3$s %4$s "
                + "SET share_count=tmp.count FROM tmp WHERE %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS,
                MetaDAOImpl.TABLE_NAME, MetaDAOImpl.TABLE_ALIAS);
        super.update(sql);
    }

    @Override
    public void updateMetaShareCountForRoot(long rootId) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.root_id, %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.root_id=%4$s.root_id AND %2$s.meta_id=%4$s.id "
                + "WHERE %4$s.root_id=? GROUP BY %4$s.root_id, %4$s.id) "
                + "UPDATE %3$s %4$s SET share_count=tmp.count FROM tmp "
                + "WHERE %4$s.root_id=tmp.root_id AND %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS, MetaDAOImpl.TABLE_NAME,
                MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId);
    }

    @Override
    public void updateMetaShareCountForMeta(long rootId, long metaId) throws DAOException {
        String sql = String.format("WITH tmp AS (SELECT %4$s.root_id, %4$s.id, COUNT(%2$s) AS count FROM %3$s %4$s "
                + "LEFT JOIN %1$s %2$s ON %2$s.root_id=%4$s.root_id AND %2$s.meta_id=%4$s.id "
                + "WHERE %4$s.root_id=? AND %4$s.id=? GROUP BY %4$s.root_id, %4$s.id) "
                + "UPDATE %3$s %4$s SET share_count=tmp.count FROM tmp "
                + "WHERE %4$s.root_id=tmp.root_id AND %4$s.id=tmp.id", TABLE_NAME, TABLE_ALIAS, MetaDAOImpl.TABLE_NAME,
                MetaDAOImpl.TABLE_ALIAS);
        super.update(sql, rootId, metaId);
    }

    @Override
    public void setIdInitValue() throws DAOException {
        // TODO Auto-generated method stub
        String sql = String.format("SELECT setval('shares_id_seq', MAX(id)) FROM %1$s", TABLE_NAME);
        super.uniqueResult(sql);
    }
}
