package com.banmayun.server.migration.to.db;

public class UniqueViolationException extends DAOException {

    private static final long serialVersionUID = 1L;

    public UniqueViolationException() {
        super();
    }

    public UniqueViolationException(String message) {
        super(message);
    }

    public UniqueViolationException(String message, Throwable e) {
        super(message, e);
    }

    public UniqueViolationException(Throwable e) {
        super(e);
    }
}
