package com.banmayun.server.migration.to.db;

public class DAOException extends Exception {

    private static final long serialVersionUID = 1L;

    public DAOException() {
        super();
    }

    public DAOException(String message) {
        super(message);
    }

    public DAOException(String message, Throwable e) {
        super(message, e);
    }

    public DAOException(Throwable e) {
        super(e);
    }
}
