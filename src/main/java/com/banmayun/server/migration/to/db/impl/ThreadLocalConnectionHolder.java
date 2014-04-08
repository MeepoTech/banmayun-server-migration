package com.banmayun.server.migration.to.db.impl;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class ThreadLocalConnectionHolder {

    private static ThreadLocalConnectionHolder instance = null;

    public static ThreadLocalConnectionHolder getInstance() {
        if (instance == null) {
            initializeInstance();
        }
        assert instance != null;
        return instance;
    }

    private static synchronized void initializeInstance() {
        if (instance != null) {
            return;
        }
        instance = new ThreadLocalConnectionHolder();
    }

    private ThreadLocal<ConnectionHolder> threadLocalHolder = null;

    private ThreadLocalConnectionHolder() {
        this.threadLocalHolder = new ThreadLocal<ConnectionHolder>();
    }

    public Connection getConnection(DataSource dataSource) throws SQLException {
        return getConnectionHolder().getConnection(dataSource);
    }

    public void removeConnection(DataSource dataSource) {
        this.getConnectionHolder().removeConnection(dataSource);
    }

    private ConnectionHolder getConnectionHolder() {
        ConnectionHolder holder = this.threadLocalHolder.get();
        if (holder == null) {
            holder = new ConnectionHolder();
            this.threadLocalHolder.set(holder);
        }
        return holder;
    }
}
