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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.from.core.Link;
import com.banmayun.server.migration.from.core.Link.Device;
import com.google.common.base.Optional;

public class LinkDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "id", "name", "device", "token", "category",
            "owner_id", "expires", "created" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "l";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = StringUtils.replace(COLUMNS_UPDATE, "device=?", "device=?::link_device");
        COLUMNS_UPDATE = StringUtils.replace(COLUMNS_UPDATE, "category=?", "category=?::link_category");
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Link parseLink(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Link ret = new Link();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setName((String) arg.get(COLUMN_ALIASES[1]));
        ret.setDevice(Device.valueOf(((String) arg.get(COLUMN_ALIASES[2])).toUpperCase()));
        ret.setToken((String) arg.get(COLUMN_ALIASES[3]));
        ret.setCategory(Link.Category.valueOf(((String) arg.get(COLUMN_ALIASES[4])).toUpperCase()));
        ret.setOwnerId((Long) arg.get(COLUMN_ALIASES[5]));
        ret.setExpires((Timestamp) arg.get(COLUMN_ALIASES[6]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[7]));
        return ret;
    }

    public static List<Link> parseLinks(List<Map<String, Object>> arg) {
        List<Link> ret = new ArrayList<Link>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseLink(map));
        }
        return ret;
    }

    public LinkDAO(Connection conn) {
        super(conn);
    }

    public Link create(Link link) throws SQLException {
        String sql = "INSERT INTO tbl_link (" + COLUMNS_INSERT + ") "
                + "VALUES (?, ?::link_device, ?, ?::link_category, ?, ?, ?) " + "RETURNING " + COLUMNS_RETURN;
        return parseLink(super.uniqueResult(sql, link.getName(), link.getDevice().toString().toLowerCase(),
                link.getToken(), link.getCategory().toString().toLowerCase(), link.getOwnerId(), link.getExpires(),
                link.getCreated()));
    }

    public List<Link> list(int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_link l " + "ORDER BY l.created OFFSET ? LIMIT ?";
        return parseLinks(super.list(sql, offset, limit));
    }

    public int countAvailableToken() throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_link l WHERE l.expires > ? ";
        Map<String, Object> ret = super.uniqueResult(sql, new Timestamp(System.currentTimeMillis()));
        return ((Long) ret.get("count")).intValue();
    }

    public Optional<Link> get(long id) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_link l " + "WHERE l.id=?";
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, id)));
    }

    public Optional<Link> getByToken(String token) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_link l " + "WHERE l.token=?";
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, token)));
    }

    public List<Link> listByOwnerId(long ownerId, int offset, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_link l "
                + "WHERE l.owner_id=? AND l.category=?::link_category " + "ORDER BY l.created DESC OFFSET ? LIMIT ?";
        return parseLinks(super.list(sql, ownerId, Link.Category.ACCESS.toString().toLowerCase(), offset, limit));
    }

    public int countByOwnerId(long ownerId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM tbl_link l " + "WHERE l.owner_id=? AND l.category=?::link_category";
        Map<String, Object> ret = super.uniqueResult(sql, ownerId, Link.Category.ACCESS.toString().toLowerCase());
        return ((Long) ret.get("count")).intValue();
    }

    public Pair<Integer, List<Link>> listAndCountByOwnerId(long ownerId, int offset, int limit) throws SQLException {
        Integer count = this.countByOwnerId(ownerId);
        List<Link> links = this.listByOwnerId(ownerId, offset, limit);
        return Pair.of(count, links);
    }

    public Optional<Link> delete(long id) throws SQLException {
        String sql = "DELETE FROM tbl_link l WHERE l.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, id)));
    }

    public Optional<Link> deleteByToken(String token) throws SQLException {
        String sql = "DELETE FROM tbl_link l WHERE l.token=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseLink(super.uniqueResult(sql, token)));
    }

    public int deleteByOwnerId(long ownerId) throws SQLException {
        String sql = "DELETE FROM tbl_link l " + "WHERE l.owner_id=? AND l.category=?::link_category";
        return super.update(sql, ownerId, Link.Category.ACCESS.toString().toLowerCase());
    }

    public int deleteByOwnerIdExcept(long ownerId, long id) throws SQLException {
        String sql = "DELETE FROM tbl_link l " + "WHERE l.owner_id=? AND l.category=?::link_category "
                + "AND (NOT l.id=?)";
        return super.update(sql, ownerId, Link.Category.ACCESS.toString().toLowerCase(), id);
    }

    public int deleteByOwnerIdExceptByToken(long ownerId, String token) throws SQLException {
        String sql = "DELETE FROM tbl_link l " + "WHERE l.owner_id=? AND l.category=?::link_category "
                + "AND (NOT l.token=?)";
        return super.update(sql, ownerId, Link.Category.ACCESS.toString().toLowerCase(), token);
    }

    public int deleteExpired() throws SQLException {
        String sql = "DELETE FROM tbl_link l " + "WHERE l.expires<=?";
        return super.update(sql, new Timestamp(System.currentTimeMillis()));
    }
}
