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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.banmayun.server.migration.from.core.Permission;
import com.google.common.base.Optional;

public class PermissionDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "group_id", "root_id", "path", "can_create",
            "can_owner_read", "can_owner_write", "can_owner_delete", "can_others_read", "can_others_write",
            "can_others_delete" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "p";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias, COLUMN_NAMES);
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Permission parsePermission(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Permission ret = new Permission();
        ret.setGroupId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setRootId((Long) arg.get(COLUMN_ALIASES[1]));
        ret.setPath((String) arg.get(COLUMN_ALIASES[2]));
        ret.setCanCreate((Boolean) arg.get(COLUMN_ALIASES[3]));
        ret.setCanOwnerRead((Boolean) arg.get(COLUMN_ALIASES[4]));
        ret.setCanOwnerWrite((Boolean) arg.get(COLUMN_ALIASES[5]));
        ret.setCanOwnerDelete((Boolean) arg.get(COLUMN_ALIASES[6]));
        ret.setCanOthersRead((Boolean) arg.get(COLUMN_ALIASES[7]));
        ret.setCanOthersWrite((Boolean) arg.get(COLUMN_ALIASES[8]));
        ret.setCanOthersDelete((Boolean) arg.get(COLUMN_ALIASES[9]));
        return ret;
    }

    public static List<Permission> parsePermissions(List<Map<String, Object>> arg) {
        List<Permission> ret = new ArrayList<Permission>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parsePermission(map));
        }
        return ret;
    }

    public PermissionDAO(Connection conn) {
        super(conn);
    }

    public Permission create(Permission perm) throws SQLException {
        String sql = "INSERT INTO tbl_perm (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "RETURNING " + COLUMNS_RETURN;
        return parsePermission(super.uniqueResult(sql, perm.getGroupId(), perm.getRootId(), perm.getPath(),
                perm.getCanCreate(), perm.getCanOwnerRead(), perm.getCanOwnerWrite(), perm.getCanOwnerDelete(),
                perm.getCanOthersRead(), perm.getCanOthersWrite(), perm.getCanOthersDelete()));
    }

    public Permission createOrSet(Permission perm) throws SQLException {
        boolean exists = this.getByPath(perm.getGroupId(), perm.getRootId(), perm.getPath()).isPresent();
        Permission updatedPerm = null;
        if (exists) {
            updatedPerm = this.update(perm.getGroupId(), perm.getRootId(), perm.getPath(), perm).orNull();
        } else {
            updatedPerm = this.create(perm);
        }
        return updatedPerm;
    }

    public Optional<Permission> getByPath(long groupId, long rootId, String path) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_perm p "
                + "WHERE p.group_id=? AND p.root_id=? AND lower(p.path)=lower(?)";
        return Optional.fromNullable(parsePermission(super.uniqueResult(sql, groupId, rootId, path)));
    }

    public List<Permission> listByRoot(long groupId, long rootId) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_perm p " + "WHERE p.group_id=? AND p.root_id=?";
        return parsePermissions(super.list(sql, groupId, rootId));
    }

    public Optional<Permission> update(long groupId, long rootId, String path, Permission update) throws SQLException {
        Permission perm = this.getByPath(groupId, rootId, path).orNull();
        if (perm == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_perm p SET " + COLUMNS_UPDATE
                + " WHERE p.group_id=? AND p.root_id=? AND lower(p.path)=lower(?) " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parsePermission(super.uniqueResult(sql,
                update.getGroupId() == null ? perm.getGroupId() : update.getGroupId(),
                update.getRootId() == null ? perm.getRootId() : update.getRootId(),
                update.getPath() == null ? perm.getPath() : update.getPath(),
                update.getCanCreate() == null ? perm.getCanCreate() : update.getCanCreate(),
                update.getCanOwnerRead() == null ? perm.getCanOwnerRead() : update.getCanOwnerRead(), update
                        .getCanOwnerWrite() == null ? perm.getCanOwnerWrite() : update.getCanOwnerWrite(), update
                        .getCanOwnerDelete() == null ? perm.getCanOwnerDelete() : update.getCanOwnerDelete(), update
                        .getCanOthersRead() == null ? perm.getCanOthersRead() : update.getCanOthersRead(), update
                        .getCanOthersWrite() == null ? perm.getCanOthersWrite() : update.getCanOthersWrite(), update
                        .getCanOthersDelete() == null ? perm.getCanOthersDelete() : update.getCanOthersDelete(),
                groupId, rootId, path)));
    }

    public Optional<Permission> delete(long groupId, long rootId, String path) throws SQLException {
        String sql = "DELETE FROM tbl_perm p " + "WHERE p.group_id=? AND p.root_id=? AND lower(p.path)=lower(?) "
                + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parsePermission(super.uniqueResult(sql, groupId, rootId, path)));
    }
}
