package com.banmayun.server.migration.to.db;

import java.util.List;

import com.banmayun.server.migration.to.core.Comment;
import com.google.common.base.Optional;

public interface CommentDAO {

    public Comment createComment(Comment comment) throws DAOException;

    public Optional<Comment> getComment(long rootId, long metaId, long commentId) throws DAOException;

    public int countComments() throws DAOException;

    public int countCommentsForRoot(long rootId) throws DAOException;

    public int countCommentsForMeta(long rootId, long metaId) throws DAOException;

    public List<Comment> listComments(int offset, int limit) throws DAOException;

    public List<Comment> listCommentsForRoot(long rootId, int offset, int limit) throws DAOException;

    public List<Comment> listCommentsForMeta(long rootId, long metaId, int offset, int limit) throws DAOException;

    public Optional<Comment> deleteComment(long rootId, long metaId, long commentId) throws DAOException;

    public void deleteComments() throws DAOException;

    public void deleteCommentsForRoot(long rootId) throws DAOException;

    public void deleteCommentsForMeta(long rootId, long metaId) throws DAOException;

    public void updateMetaCommentCount() throws DAOException;

    public void updateMetaCommentCountForRoot(long rootId) throws DAOException;

    public void updateMetaCommentCountForMeta(long rootId, long metaId) throws DAOException;
}
