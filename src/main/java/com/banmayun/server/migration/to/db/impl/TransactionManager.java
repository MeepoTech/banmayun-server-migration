package com.banmayun.server.migration.to.db.impl;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class TransactionManager {

    private DataSource dataSource = null;

    public TransactionManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void start() throws SQLException {
        Connection connection = this.getConnection();
        connection.setAutoCommit(false);
    }

    public void commit() throws SQLException {
        Connection connection = this.getConnection();
        connection.commit();
    }

    public void rollback() {
        Connection connection = null;
        try {
            connection = this.getConnection();
            connection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        Connection connection = null;
        try {
            connection = this.getConnection();
            connection.setAutoCommit(true);
            connection.setReadOnly(false);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ThreadLocalConnectionHolder.getInstance().removeConnection(this.dataSource);
    }

    private Connection getConnection() throws SQLException {
        return ThreadLocalConnectionHolder.getInstance().getConnection(this.dataSource);
    }
}
