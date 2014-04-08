package com.banmayun.server.migration.to.db.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

public class ConnectionHolder {

    private Map<DataSource, Connection> connections = null;

    public ConnectionHolder() {
        this.connections = new HashMap<DataSource, Connection>();
    }

    public Connection getConnection(DataSource dataSource) throws SQLException {
        Connection connection = this.connections.get(dataSource);
        if (connection == null || connection.isClosed()) {
            connection = dataSource.getConnection();
            this.connections.put(dataSource, connection);
        }
        return connection;
    }

    public void removeConnection(DataSource dataSource) {
        this.connections.remove(dataSource);
    }
}
