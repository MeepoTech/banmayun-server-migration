package com.banmayun.server.migration.to.db.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import com.banmayun.server.migration.to.db.DAOException;
import com.banmayun.server.migration.to.db.UniqueViolationException;

public abstract class AbstractDAO {

    protected static final String UNIQUE_VIOLATION = "23505";

    protected static boolean isUniqueViolation(SQLException e) {
        return UNIQUE_VIOLATION.equals(e.getSQLState());
    }

    protected DataSource dataSource = null;
    protected QueryRunner runner = new QueryRunner();
    protected MapHandler handler = new MapHandler();
    protected MapListHandler listHandler = new MapListHandler();

    public AbstractDAO(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Connection getConnection() throws SQLException {
        return ThreadLocalConnectionHolder.getInstance().getConnection(this.dataSource);
    }

    protected Map<String, Object> uniqueResult(String sql) throws DAOException {
        try {
        	return this.runner.query(this.getConnection(), sql, this.handler);
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                throw new UniqueViolationException();
            } else {
                throw new DAOException(e);
            }
        }
    }

    protected Map<String, Object> uniqueResult(String sql, Object... params) throws DAOException {
        try {
            return this.runner.query(this.getConnection(), sql, this.handler, params);
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                throw new UniqueViolationException();
            } else {
                throw new DAOException(e);
            }
        }
    }

    protected List<Map<String, Object>> list(String sql) throws DAOException {
        try {
            return this.runner.query(this.getConnection(), sql, this.listHandler);
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                throw new UniqueViolationException();
            } else {
                throw new DAOException(e);
            }
        }
    }

    protected List<Map<String, Object>> list(String sql, Object... params) throws DAOException {
        try {
            return this.runner.query(this.getConnection(), sql, this.listHandler, params);
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                throw new UniqueViolationException();
            } else {
                throw new DAOException(e);
            }
        }
    }

    protected int update(String sql) throws DAOException {
        try {
            return this.runner.update(this.getConnection(), sql);
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                throw new UniqueViolationException();
            } else {
                throw new DAOException(e);
            }
        }
    }

    protected int update(String sql, Object... params) throws DAOException {
        try {
            return this.runner.update(this.getConnection(), sql, params);
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                throw new UniqueViolationException();
            } else {
                throw new DAOException(e);
            }
        }
    }
    
    protected int[] batch(String sql, Object[][] params) throws DAOException {
        try {
            return this.runner.batch(this.getConnection(), sql, params);
        } catch (SQLException e) {
            if (isUniqueViolation(e)) {
                throw new UniqueViolationException();
            } else {
                throw new DAOException(e);
            }
        }
    }
}
