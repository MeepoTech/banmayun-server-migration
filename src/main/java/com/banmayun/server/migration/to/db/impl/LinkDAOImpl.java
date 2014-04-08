package com.banmayun.server.migration.to.db.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ArrayUtils;

import com.banmayun.server.migration.to.core.Link;
import com.banmayun.server.migration.to.core.Link.LinkCategory;
import com.banmayun.server.migration.to.core.Link.LinkDevice;
import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.LinkDAO;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.google.common.base.Optional;

public class LinkDAOImpl extends AbstractDAO implements LinkDAO {

    protected static final String TABLE_NAME = "links";
    protected static final String TABLE_ALIAS = "_link_";
    protected static final String[] COLUMN_NAMES = new String[] { "id", "user_id", "token", "category", "name",
            "device", "expires_at", "created_at" };
    protected static final String[] COLUMN_ALIASES;
    protected static final String COLUMNS_INSERT;
    protected static final String COLUMNS_SELECT;
    protected static final String COLUMNS_RETURN;
    static {
        COLUMN_ALIASES = DAOUtils.getColumnAliases(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_INSERT = DAOUtils.getColumnsInsert(TABLE_ALIAS,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = DAOUtils.getColumnsSelect(TABLE_ALIAS, COLUMN_NAMES);
        COLUMNS_RETURN = DAOUtils.getColumnsReturn(TABLE_ALIAS, COLUMN_NAMES);
    }

    public static Link parseLink(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Link ret = new Link();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setUserId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setToken((String) arg.get(COLUMN_ALIASES[2]));
        ret.setCategory(LinkCategory.valueOf(((String) arg.get(COLUMN_ALIASES[3]))));
        ret.setName((String) arg.get(COLUMN_ALIASES[4]));
        ret.setDevice(LinkDevice.valueOf(((String) arg.get(COLUMN_ALIASES[5]))));
        ret.setExpiresAt((Timestamp) arg.get(COLUMN_ALIASES[6]));
        ret.setCreatedAt((Timestamp) arg.get(COLUMN_ALIASES[7]));
        return ret;
    }

    public static List<Link> parseLinks(List<Map<String, Object>> arg) {
        List<Link> ret = new ArrayList<Link>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseLink(map));
        }
        return ret;
    }

    public LinkDAOImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Link createLink(Link link) throws UniqueViolationException, DAOException {
        String sql = String.format("INSERT INTO %1$s (%2$s) VALUES (?, ?, ?::link_category, ?, ?::link_device, ?, ?) "
                + "RETURNING %3$s", TABLE_NAME, COLUMNS_INSERT, COLUMNS_RETURN);
        return parseLink(super.uniqueResult(sql, link.getUserId(), link.getToken(), link.getCategory().toString(),
                link.getName(), link.getDevice().toString(), link.getExpiresAt(), link.getCreatedAt()));
    }

    @Override
    public Optional<Link> findLinkByToken(String token, LinkCategory category) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.token=? AND %2$s.category=?::link_category",
                TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, token, category.toString())));
    }

    @Override
    public Optional<Link> getLink(long userId, long linkId, LinkCategory category) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.user_id=? AND %2$s.id=? "
                + "AND %2$s.category=?::link_category", TABLE_NAME, TABLE_ALIAS, COLUMNS_SELECT);
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, userId, linkId, category.toString())));
    }

    @Override
    public int countLinks(Link.LinkCategory category) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.category=?::link_category",
                TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, category.toString());
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public int countLinksForUser(long userId, LinkCategory category) throws DAOException {
        String sql = String.format("SELECT COUNT(*) AS count FROM %1$s %2$s WHERE %2$s.user_id=? "
                + "AND %2$s.category=?::link_category", TABLE_NAME, TABLE_ALIAS);
        Map<String, Object> ret = super.uniqueResult(sql, userId, category.toString());
        return ((Long) ret.get("count")).intValue();
    }

    @Override
    public List<Link> listLinksForUser(long userId, LinkCategory category, int offset, int limit) throws DAOException {
        String sql = String.format("SELECT %3$s FROM %1$s %2$s WHERE %2$s.user_id=? "
                + "AND %2$s.category=?::link_category ORDER BY %2$s.id DESC OFFSET ? LIMIT ?", TABLE_NAME, TABLE_ALIAS,
                COLUMNS_SELECT);
        return parseLinks(super.list(sql, userId, category.toString(), offset, limit));
    }

    @Override
    public Optional<Link> deleteLink(long userId, long linkId, LinkCategory category) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.user_id=? AND %2$s.id=? "
                + "AND %2$s.category=?::link_category RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, userId, linkId, category.toString())));
    }

    @Override
    public Optional<Link> deleteLinkByToken(String token, LinkCategory category) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.token=? AND %2$s.category=?::link_category "
                + "RETURNING %3$s", TABLE_NAME, TABLE_ALIAS, COLUMNS_RETURN);
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, token, category.toString())));
    }

    @Override
    public void deleteLinksForUser(long userId, LinkCategory category) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.user_id=? AND %2$s.category=?::link_category",
                TABLE_NAME, TABLE_ALIAS);
        super.update(sql, userId, category.toString());
    }

    @Override
    public void deleteLinksForUserExcept(long userId, LinkCategory category, long linkId) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.user_id=? AND %2$s.category=?::link_category "
                + "AND %2$s.id!=?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, userId, category.toString(), linkId);
    }

    @Override
    public void deleteLinksForUserExceptToken(long userId, LinkCategory category, String token) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.user_id=? AND %2$s.category=?::link_category "
                + "AND %2$s.token!=?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, userId, category.toString(), token);
    }

    @Override
    public void deleteLinksExpiresBefore(Timestamp time) throws DAOException {
        String sql = String.format("DELETE FROM %1$s %2$s WHERE %2$s.expires_at<?", TABLE_NAME, TABLE_ALIAS);
        super.update(sql, time);
    }
}
