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

import javax.sql.DataSource;

public class DAOFactory {

    public static String UNIQUE_VIOLATION = "23505";

    private static DAOFactory instance = null;

    public static synchronized DAOFactory initializeInstance(DataSource dataSource) {
        if (instance == null) {
            instance = new DAOFactory(dataSource);
        }
        return instance;
    }

    public static DAOFactory getInstance() {
        return instance;
    }

    protected DataSource dataSource = null;

    private DAOFactory(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Connection getConnection() throws SQLException {
        Connection conn = this.dataSource.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    public LinkDAO getLinkDAO(Connection conn) {
        return new LinkDAO(conn);
    }

    public UserDAO getUserDAO(Connection conn) {
        return new UserDAO(conn);
    }

    public GroupDAO getGroupDAO(Connection conn) {
        return new GroupDAO(conn);
    }

    public RelationDAO getRelationDAO(Connection conn) {
        return new RelationDAO(conn);
    }

    public UploadDAO getUploadDAO(Connection conn) {
        return new UploadDAO(conn);
    }

    public PermissionDAO getPermissionDAO(Connection conn) {
        return new PermissionDAO(conn);
    }

    public CommentDAO getCommentDAO(Connection conn) {
        return new CommentDAO(conn);
    }

    public ShareDAO getShareDAO(Connection conn) {
        return new ShareDAO(conn);
    }

    public MetaDAO getMetaDAO(Connection conn) {
        return new MetaDAO(conn);
    }

    public DataDAO getDataDAO(Connection conn) {
        return new DataDAO(conn);
    }

    public RevisionDAO getRevisionDAO(Connection conn) {
        return new RevisionDAO(conn);
    }

    public TrashDAO getTrashDAO(Connection conn) {
        return new TrashDAO(conn);
    }

    public CursorDAO getCursorDAO(Connection conn) {
        return new CursorDAO(conn);
    }

    public StatisticDAO getStatisticDAO(Connection conn) {
        return new StatisticDAO(conn);
    }

    public GroupStatisticDAO getGroupStatisticDAO(Connection conn) {
        return new GroupStatisticDAO(conn);
    }

    public SummaryStatisticDAO getSummaryStatisticDAO(Connection conn) {
        return new SummaryStatisticDAO(conn);
    }
}
