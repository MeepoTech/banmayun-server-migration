package com.banmayun.server.migration.to.db;

import java.sql.Timestamp;
import java.util.List;

import com.banmayun.server.migration.to.core.Link;
import com.banmayun.server.migration.to.core.Link.LinkCategory;
import com.google.common.base.Optional;

public interface LinkDAO {

    public Link createLink(Link link) throws UniqueViolationException, DAOException;

    public Optional<Link> findLinkByToken(String token, LinkCategory category) throws DAOException;

    public Optional<Link> getLink(long userId, long linkId, LinkCategory category) throws DAOException;

    public int countLinks(LinkCategory category) throws DAOException;

    public int countLinksForUser(long userId, LinkCategory category) throws DAOException;

    public List<Link> listLinksForUser(long userId, LinkCategory category, int offset, int limit) throws DAOException;

    public Optional<Link> deleteLink(long userId, long linkId, LinkCategory category) throws DAOException;

    public Optional<Link> deleteLinkByToken(String token, LinkCategory category) throws DAOException;

    public void deleteLinksForUser(long userId, LinkCategory category) throws DAOException;

    public void deleteLinksForUserExcept(long userId, LinkCategory category, long linkId) throws DAOException;

    public void deleteLinksForUserExceptToken(long userId, LinkCategory category, String token) throws DAOException;

    public void deleteLinksExpiresBefore(Timestamp time) throws DAOException;
}
