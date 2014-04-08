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

import com.banmayun.server.migration.from.core.Upload;
import com.google.common.base.Optional;

public class UploadDAO extends AbstractDAO {

    protected static final String[] COLUMN_NAMES = new String[] { "id", "location", "pos", "bytes", "expires",
            "created" };
    protected static final String[] COLUMN_ALIASES;
    protected static String COLUMNS_INSERT;
    protected static String COLUMNS_RETURN;
    protected static String COLUMNS_SELECT;
    protected static String COLUMNS_UPDATE;
    static {
        String tableAlias = "u";
        COLUMN_ALIASES = AbstractDAO.getColumnAliases(tableAlias, COLUMN_NAMES);
        COLUMNS_INSERT = AbstractDAO.getColumnsInsert(tableAlias,
                ArrayUtils.subarray(COLUMN_NAMES, 1, COLUMN_NAMES.length));
        COLUMNS_SELECT = AbstractDAO.getColumnsSelect(tableAlias, COLUMN_NAMES);
        COLUMNS_UPDATE = AbstractDAO.getColumnsUpdate(tableAlias, COLUMN_NAMES);
        COLUMNS_RETURN = AbstractDAO.getColumnsReturn(tableAlias, COLUMN_NAMES);
    }

    public static Upload parseUpload(Map<String, Object> arg) {
        if (arg == null) {
            return null;
        }
        Upload ret = new Upload();
        ret.setId((Long) arg.get(COLUMN_ALIASES[0]));
        ret.setLocation((String) arg.get(COLUMN_ALIASES[1]));
        ret.setPos((Long) arg.get(COLUMN_ALIASES[2]));
        ret.setBytes((Long) arg.get(COLUMN_ALIASES[3]));
        ret.setExpires((Timestamp) arg.get(COLUMN_ALIASES[4]));
        ret.setCreated((Timestamp) arg.get(COLUMN_ALIASES[5]));
        return ret;
    }

    public static List<Upload> parseUploads(List<Map<String, Object>> arg) {
        List<Upload> ret = new ArrayList<Upload>(arg.size());
        for (Map<String, Object> map : arg) {
            ret.add(parseUpload(map));
        }
        return ret;
    }

    public UploadDAO(Connection conn) {
        super(conn);
    }

    public Upload create(Upload upload) throws SQLException {
        String sql = "INSERT INTO tbl_upload (" + COLUMNS_INSERT + ") " + "VALUES (?, ?, ?, ?, ?) " + "RETURNING "
                + COLUMNS_RETURN;
        return parseUpload(super.uniqueResult(sql, upload.getLocation(), upload.getPos(), upload.getBytes(),
                upload.getExpires(), upload.getCreated()));
    }

    public Optional<Upload> get(long id) throws SQLException {
        String sql = "SELECT " + COLUMNS_SELECT + " FROM tbl_upload u " + "WHERE u.id=?";
        return Optional.fromNullable(parseUpload(super.uniqueResult(sql, id)));
    }

    public Optional<Upload> update(long id, Upload update) throws SQLException {
        Upload upload = this.get(id).orNull();
        if (upload == null) {
            return Optional.absent();
        }

        String sql = "UPDATE tbl_upload u SET " + COLUMNS_UPDATE + " WHERE u.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseUpload(super.uniqueResult(sql, update.getId() == null ? upload.getId()
                : update.getId(), update.getLocation() == null ? upload.getLocation() : update.getLocation(), update
                .getPos() == null ? upload.getPos() : update.getPos(), update.getBytes() == null ? upload.getBytes()
                : update.getBytes(), update.getExpires() == null ? upload.getExpires() : update.getExpires(), update
                .getCreated() == null ? upload.getCreated() : update.getCreated(), id)));
    }

    public Optional<Upload> delete(long id) throws SQLException {
        String sql = "DELETE FROM tbl_upload u WHERE u.id=? " + "RETURNING " + COLUMNS_RETURN;
        return Optional.fromNullable(parseUpload(super.uniqueResult(sql, id)));
    }
}
