package com.banmayun.server.migration.to.db.impl;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.banmayun.server.migration.to.db.ChunkedUploadDAO;
import com.banmayun.server.migration.to.db.CommentDAO;
import com.banmayun.server.migration.to.db.CursorDAO;
import com.banmayun.server.migration.to.db.DataDAO;
import com.banmayun.server.migration.to.db.GroupDAO;
import com.banmayun.server.migration.to.db.LinkDAO;
import com.banmayun.server.migration.to.db.MetaDAO;
import com.banmayun.server.migration.to.db.RelationDAO;
import com.banmayun.server.migration.to.db.RevisionDAO;
import com.banmayun.server.migration.to.db.RootDAO;
import com.banmayun.server.migration.to.db.ShareDAO;
import com.banmayun.server.migration.to.db.StatisticDAO;
import com.banmayun.server.migration.to.db.TrashDAO;
import com.banmayun.server.migration.to.db.UserDAO;

public class DAOManager {

    private static DAOManager instance = null;

    public static DAOManager getInstance() {
        return instance;
    }

    public synchronized static void initializeInstance(DataSource dataSource) {
        if (instance == null) {
            instance = new DAOManager(dataSource);
        }
    }

    private DataSource dataSource = null;
    private Map<Class<?>, Object> instances = null;

    private DAOManager(DataSource dataSource) {
        this.dataSource = dataSource;
        this.initializeInstances();
    }

    private void initializeInstances() {
        this.instances = new HashMap<Class<?>, Object>();
        this.instances.put(ChunkedUploadDAO.class, new ChunkedUploadDAOImpl(this.dataSource));
        this.instances.put(CommentDAO.class, new CommentDAOImpl(this.dataSource));
        this.instances.put(CursorDAO.class, new CursorDAOImpl(this.dataSource));
        this.instances.put(DataDAO.class, new DataDAOImpl(this.dataSource));
        this.instances.put(GroupDAO.class, new GroupDAOImpl(this.dataSource));
        this.instances.put(LinkDAO.class, new LinkDAOImpl(this.dataSource));
        this.instances.put(MetaDAO.class, new MetaDAOImpl(this.dataSource));
        this.instances.put(RelationDAO.class, new RelationDAOImpl(this.dataSource));
        this.instances.put(RevisionDAO.class, new RevisionDAOImpl(this.dataSource));
        this.instances.put(RootDAO.class, new RootDAOImpl(this.dataSource));
        this.instances.put(ShareDAO.class, new ShareDAOImpl(this.dataSource));
        this.instances.put(StatisticDAO.class, new StatisticDAOImpl(this.dataSource));
        this.instances.put(TrashDAO.class, new TrashDAOImpl(this.dataSource));
        this.instances.put(UserDAO.class, new UserDAOImpl(this.dataSource));
    }

    @SuppressWarnings("unchecked")
    public <T> T getDAO(Class<T> daoClass) {
        return (T) this.instances.get(daoClass);
    }

    public TransactionManager getTransactionManager() {
        return new TransactionManager(this.dataSource);
    }
}
