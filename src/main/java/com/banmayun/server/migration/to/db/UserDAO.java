package com.banmayun.server.migration.to.db;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.banmayun.server.migration.to.core.Root;
import com.banmayun.server.migration.to.core.User;
import com.banmayun.server.migration.to.core.User.UserRole;
import com.google.common.base.Optional;

public interface UserDAO {

    public User createUser(User user) throws UniqueViolationException, DAOException;

    public Optional<User> findUserByName(String name, String source) throws DAOException;

    public Optional<User> findUserByEmail(String email, String source) throws DAOException;

    public Optional<User> findUserByRootId(long rootId) throws DAOException;

    public Optional<User> getUser(long userId) throws DAOException;

    public int countUsers() throws DAOException;

    public int countUsers(UserRole role) throws DAOException;

    public List<User> listUsers(int offset, int limit) throws DAOException;

    public List<User> listUsers(UserRole role, int offset, int limit) throws DAOException;

    public List<User> topUsersByGroupCount(int offset, int limit) throws DAOException;

    public List<Pair<User, Root>> topUsersByFileCount(int offset, int limit) throws DAOException;

    public List<Pair<User, Root>> topUsersByByteCount(int offset, int limit) throws DAOException;

    public Optional<User> updateUser(long userId, User user) throws UniqueViolationException, DAOException;
}
