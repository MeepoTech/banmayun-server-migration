package com.banmayun.server.migration.cli;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;

import com.banmayun.server.migration.MigrationConfiguration;
import com.banmayun.server.migration.from.core.Cursor;
import com.banmayun.server.migration.from.core.Data;
import com.banmayun.server.migration.from.core.Group;
import com.banmayun.server.migration.from.core.GroupStatistic;
import com.banmayun.server.migration.from.core.Link;
import com.banmayun.server.migration.from.core.Meta;
import com.banmayun.server.migration.from.core.Permission;
import com.banmayun.server.migration.from.core.Relation;
import com.banmayun.server.migration.from.core.Revision;
import com.banmayun.server.migration.from.core.Share;
import com.banmayun.server.migration.from.core.Statistic;
import com.banmayun.server.migration.from.core.SummaryStatistic;
import com.banmayun.server.migration.from.core.Trash;
import com.banmayun.server.migration.from.core.User;
import com.banmayun.server.migration.from.db.CursorDAO;
import com.banmayun.server.migration.from.db.DAOFactory;
import com.banmayun.server.migration.from.db.DataDAO;
import com.banmayun.server.migration.from.db.GroupDAO;
import com.banmayun.server.migration.from.db.GroupStatisticDAO;
import com.banmayun.server.migration.from.db.LinkDAO;
import com.banmayun.server.migration.from.db.MetaDAO;
import com.banmayun.server.migration.from.db.PermissionDAO;
import com.banmayun.server.migration.from.db.RelationDAO;
import com.banmayun.server.migration.from.db.RevisionDAO;
import com.banmayun.server.migration.from.db.ShareDAO;
import com.banmayun.server.migration.from.db.StatisticDAO;
import com.banmayun.server.migration.from.db.SummaryStatisticDAO;
import com.banmayun.server.migration.from.db.TrashDAO;
import com.banmayun.server.migration.from.db.UserDAO;
import com.banmayun.server.migration.to.core.Group.GroupType;
import com.banmayun.server.migration.to.core.Link.LinkCategory;
import com.banmayun.server.migration.to.core.Link.LinkDevice;
import com.banmayun.server.migration.to.core.Relation.RelationRole;
import com.banmayun.server.migration.to.core.Root.RootType;
import com.banmayun.server.migration.to.core.User.UserRole;
import com.banmayun.server.migration.to.db.UniqueViolationException;
import com.banmayun.server.migration.to.db.impl.DAOManager;
import com.banmayun.server.migration.to.db.impl.TransactionManager;

public class Migrator {

	private static final int LIST_LIMIT = 1000;
	private Map<Long, Long> userIds = null;
	private Map<Long, Long> groupIds = null;
	private Map<Long, Long> metaIds = null;
	private Map<Long, Long> userRootIds = null;
	private Map<Long, Long> groupRootIds = null;
	private MigrationConfiguration config = null;

	private com.banmayun.server.migration.to.db.RootDAO rootDAO = null;
	private com.banmayun.server.migration.to.db.UserDAO userDAO = null;
	private com.banmayun.server.migration.to.db.GroupDAO groupDAO = null;
	private com.banmayun.server.migration.to.db.RelationDAO relationDAO = null;
	private com.banmayun.server.migration.to.db.DataDAO dataDAO = null;
	private com.banmayun.server.migration.to.db.LinkDAO linkDAO = null;
	private com.banmayun.server.migration.to.db.MetaDAO metaDAO = null;
	private com.banmayun.server.migration.to.db.RevisionDAO revisionDAO = null;
	private com.banmayun.server.migration.to.db.TrashDAO trashDAO = null;
	private com.banmayun.server.migration.to.db.CursorDAO cursorDAO = null;
	private com.banmayun.server.migration.to.db.ShareDAO shareDAO = null;
	private com.banmayun.server.migration.to.db.StatisticGroupDAO statisticGroupDAO = null;
	private com.banmayun.server.migration.to.db.StatisticSummaryDAO statisticSummaryDAO = null;

	public static final char[] TRUE_CHARS = new char[] { 'i', 'r', 'w', 'd',
			'i', 'r', 'w', 'd' };
	public static final char FALSE_CHAR = '-';

	public Migrator(MigrationConfiguration config) {
		this.config = config;
		this.rootDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.RootDAO.class);
		this.groupDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.GroupDAO.class);
		this.userDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.UserDAO.class);
		this.relationDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.RelationDAO.class);
		this.dataDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.DataDAO.class);
		this.linkDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.LinkDAO.class);
		this.metaDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.MetaDAO.class);
		this.revisionDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.RevisionDAO.class);
		this.trashDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.TrashDAO.class);
		this.cursorDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.CursorDAO.class);
		this.shareDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.ShareDAO.class);
		this.statisticGroupDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.StatisticGroupDAO.class);
		this.statisticSummaryDAO = DAOManager.getInstance().getDAO(
				com.banmayun.server.migration.to.db.StatisticSummaryDAO.class);

		this.userIds = new HashMap<Long, Long>();
		this.groupIds = new HashMap<Long, Long>();
		this.metaIds = new HashMap<Long, Long>();
		this.userRootIds = new HashMap<Long, Long>();
		this.groupRootIds = new HashMap<Long, Long>();
	}

	public void migrate() {
		try {

			this.migrateUsers();
			this.migrateGroups();
			this.migrateRelations();
			this.migrateLinks();
			this.migrateStatistic();
			this.migrateDatas();
			this.migrateSpaces();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void migrateUsers() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<User> users = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				UserDAO userDAO = DAOFactory.getInstance().getUserDAO(conn);
				users = userDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Users: " + users.size());
			for (User user : users) {
				this.migrateOneUser(user);
			}

			offset += users.size();
			if (users.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateOneUser(User user) throws Exception {
		com.banmayun.server.migration.to.core.User newUser = new com.banmayun.server.migration.to.core.User();
		newUser.setName(user.getName());
		newUser.setEmail(user.getEmail());
		newUser.setDisplayName(user.getFullName());
		newUser.setCreatedAt(user.getCreated());
		newUser.setGroupsCanOwn(user.getGroupsCanOwn());
		newUser.setPasswordSha256(user.getPassword());
		newUser.setRole(migrateUserRole(user.getRole()));
		if (user.getRole().equals(User.Role.BLOCKED)) {
			newUser.setIsBlocked(true);
		} else {
			newUser.setIsBlocked(false);
		}
		newUser.setIsActivated(true);
		newUser.setSource(user.getDomain().equalsIgnoreCase("local") ? com.banmayun.server.migration.to.core.User.DEFAULT_SOURCE
				: user.getDomain());

		Connection conn = null;
		Statistic stat = null;
		try {
			conn = DAOFactory.getInstance().getConnection();
			StatisticDAO statDAO = DAOFactory.getInstance().getStatisticDAO(
					conn);
			stat = statDAO.get(user.getId(), user.getId()).orNull();
			DbUtils.commitAndCloseQuietly(conn);
		} catch (SQLException e) {
			DbUtils.rollbackAndCloseQuietly(conn);
			throw e;
		}

		if (stat == null) {
			if (user.getRole().equals(User.Role.ROOT)) {
				stat = new Statistic();
				stat.setGroupId(user.getId());
				stat.setRootId(user.getId());
				stat.setQuota(214748364800L);
				stat.setBytes(0L);
			} else {
				return;
			}
		}

		com.banmayun.server.migration.to.core.Root root = new com.banmayun.server.migration.to.core.Root();
		root.setType(RootType.USER);
		root.setByteCount(stat.getBytes());
		root.setQuota(stat.getQuota());
		root.setDefaultPermission(this.config.getGroupDefaultPermission());

		TransactionManager tm = DAOManager.getInstance()
				.getTransactionManager();
		com.banmayun.server.migration.to.core.Root createdRoot = null;
		com.banmayun.server.migration.to.core.User createdUser = null;
		try {
			tm.start();
			createdRoot = this.rootDAO.createRoot(root);
			newUser.setRootId(createdRoot.getId());
			try {
				createdUser = this.userDAO.createUser(newUser);
			} catch (UniqueViolationException e) {
				tm.rollback();
				createdUser = this.userDAO
						.findUserByName(
								newUser.getName(),
								com.banmayun.server.migration.to.core.User.DEFAULT_SOURCE)
						.orNull();
				createdRoot = this.rootDAO.getRoot(createdUser.getRootId()).orNull();
			}
			System.out.println("U: " + user.getId() + " -> "
					+ createdUser.getId() + " rootId=" + createdRoot.getId());
			this.userIds.put(user.getId(), createdUser.getId());
			this.userRootIds.put(user.getId(), createdRoot.getId());
			tm.commit();
		} catch (Exception e) {
			e.printStackTrace();
			tm.rollback();
		} finally {
			tm.close();
		}
	}

	private UserRole migrateUserRole(User.Role role) {
		switch (role) {
		case ADMIN:
			return UserRole.ADMIN;
		case ROOT:
			return UserRole.ROOT;
		case USER:
			return UserRole.USER;
		case BLOCKED:
			return UserRole.USER;
		default:
			throw new RuntimeException();
		}
	}

	private void migrateGroups() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Group> groups = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				GroupDAO groupDAO = DAOFactory.getInstance().getGroupDAO(conn);
				groups = groupDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Groups: " + groups.size());
			for (Group group : groups) {
				this.migrateOneGroup(group);
			}

			offset += groups.size();
			if (groups.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private GroupType migrateGroupType(Group.Type type) {
		switch (type) {
		case SYSTEM_PUBLIC:
			return GroupType.SYSTEM_PUBLIC;
		case PUBLIC:
			return GroupType.PUBLIC;
		case PROTECTED:
			return GroupType.PROTECTED;
		case PRIVATE:
			return GroupType.PRIVATE;
		default:
			throw new RuntimeException();
		}
	}

	private void migrateOneGroup(Group group) throws Exception {
		com.banmayun.server.migration.to.core.Group newGroup = new com.banmayun.server.migration.to.core.Group();
		newGroup.setAnnounce(group.getAnnounce());
		newGroup.setCreatedAt(group.getCreated());
		newGroup.setCreatedBy(this.userIds.get(group.getCreatedBy()));
		newGroup.setIntro(group.getIntro());
		newGroup.setIsVisible(group.getIsVisible());
		newGroup.setName(group.getName());
		newGroup.setTags(group.getTags());
		newGroup.setType(this.migrateGroupType(group.getType()));
		newGroup.setIsActivated(true);
		newGroup.setIsBlocked(false);
		if (group.getStatus().equals(Group.Status.BLOCKED)) {
			newGroup.setIsBlocked(true);
		} else {
			newGroup.setIsBlocked(false);
		}

		if (group.getStatus().equals(Group.Status.NOT_ACTIVATED)) {
			newGroup.setIsActivated(false);
		} else {
			newGroup.setIsActivated(true);
		}
		newGroup.setUserCount(0);
		newGroup.setSource(group.getDomain().equalsIgnoreCase("local") ? com.banmayun.server.migration.to.core.Group.DEFAULT_SOURCE
				: group.getDomain());

		Connection conn = null;
		Statistic stat = null;
		try {
			conn = DAOFactory.getInstance().getConnection();
			StatisticDAO statDAO = DAOFactory.getInstance().getStatisticDAO(
					conn);
			stat = statDAO.get(group.getId(), group.getId()).orNull();
			DbUtils.commitAndCloseQuietly(conn);
		} catch (SQLException e) {
			DbUtils.rollbackAndCloseQuietly(conn);
			throw e;
		}

		if (stat == null) {
			System.out.println("Group without Statistic: group_id = "
					+ group.getId());
			return;
		}

		com.banmayun.server.migration.to.core.Root root = new com.banmayun.server.migration.to.core.Root();
		root.setType(RootType.GROUP);
		root.setByteCount(stat.getBytes());
		root.setQuota(stat.getQuota());
		root.setDefaultPermission(this.config.getGroupDefaultPermission());

		TransactionManager tm = DAOManager.getInstance()
				.getTransactionManager();
		try {
			tm.start();
			com.banmayun.server.migration.to.core.Root createdRoot = this.rootDAO
					.createRoot(root);
			newGroup.setRootId(createdRoot.getId());
			com.banmayun.server.migration.to.core.Group createdGroup = this.groupDAO
					.createGroup(newGroup);
			System.out.println("G: " + group.getId() + " -> "
					+ createdGroup.getId() + "  rootId = "
					+ createdRoot.getId());
			this.groupIds.put(group.getId(), createdGroup.getId());
			this.groupRootIds.put(group.getId(), createdRoot.getId());
			tm.commit();
		} catch (Exception e) {
			e.printStackTrace();
			tm.rollback();
		} finally {
			tm.close();
		}
	}

	private void migrateRelations() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Relation> relations = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				RelationDAO relationDAO = DAOFactory.getInstance()
						.getRelationDAO(conn);
				relations = relationDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Relations: " + relations.size());
			for (Relation relation : relations) {
				this.migrateOneRelation(relation);
			}

			offset += relations.size();
			if (relations.size() < LIST_LIMIT) {
				break;
			}
		}

	}

	private void migrateOneRelation(Relation relation) throws Exception {
		com.banmayun.server.migration.to.core.Relation newRelation = new com.banmayun.server.migration.to.core.Relation();
		newRelation.setCreatedAt(relation.getCreated());
		newRelation.setGroupId(this.groupIds.get(relation.getGroupId()));
		newRelation.setUserId(this.userIds.get(relation.getUserId()));
		newRelation.setRemarks(relation.getRemarks());
		newRelation.setIsActivated(true);
		newRelation.setIsBlocked(false);
		switch (relation.getRole()) {
		case BLOCKED:
			newRelation.setRole(RelationRole.MEMBER);
			newRelation.setIsActivated(false);
			break;
		case MEMBER:
			newRelation.setRole(RelationRole.MEMBER);
			break;
		case ADMIN:
			newRelation.setRole(RelationRole.ADMIN);
			break;
		case OWNER:
			newRelation.setRole(RelationRole.OWNER);
			break;
		default:
			throw new RuntimeException();
		}

		if (newRelation.getUserId() == null || newRelation.getGroupId() == null) {
			return;
		}

		TransactionManager tm = DAOManager.getInstance()
				.getTransactionManager();
		try {
			tm.start();

			com.banmayun.server.migration.to.core.Relation createdRelation = this.relationDAO
					.createRelation(newRelation);
			System.out.println("R: (" + relation.getGroupId() + ","
					+ relation.getUserId() + ")  -> ("
					+ createdRelation.getGroupId() + ","
					+ createdRelation.getUserId() + ")");

			tm.commit();
		} catch (Exception e) {
			e.printStackTrace();
			tm.rollback();
		} finally {
			tm.close();
		}

	}

	private void migrateDatas() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Data> datas = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				DataDAO dataDAO = DAOFactory.getInstance().getDataDAO(conn);
				datas = dataDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Datas: " + datas.size());
			for (Data data : datas) {
				com.banmayun.server.migration.to.core.Data newData = new com.banmayun.server.migration.to.core.Data();
				newData.setBytes(data.getBytes());
				newData.setLocation(data.getLocation());
				newData.setMD5(data.getMD5());
				newData.setRefCount(data.getRefs());

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.dataDAO.createData(newData);
					tm.commit();
				} catch (Exception e) {
					e.printStackTrace();
					tm.rollback();
				} finally {
					tm.close();
				}
			}
			offset += datas.size();
			if (datas.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateLinks() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Link> links = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				LinkDAO dataDAO = DAOFactory.getInstance().getLinkDAO(conn);
				links = dataDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Links: " + links.size());
			for (Link link : links) {
				com.banmayun.server.migration.to.core.Link newLink = new com.banmayun.server.migration.to.core.Link();
				newLink.setName(link.getName());
				newLink.setToken(link.getToken());
				newLink.setDevice(LinkDevice.valueOf(link.getDevice()
						.toString()));
				newLink.setCreatedAt(link.getCreated());
				newLink.setExpiresAt(link.getExpires());
				newLink.setUserId(this.userIds.get(link.getOwnerId()));
				switch (link.getCategory()) {
				case ACCESS:
					newLink.setCategory(LinkCategory.ACCESS);
					break;
				case ACTIVATION:
					newLink.setCategory(LinkCategory.EMAIL_VERIFICATION);
					break;
				case PASSWORD_RESET:
					newLink.setCategory(LinkCategory.PASSWORD_RESET);
					break;
				default:
					throw new RuntimeException();
				}

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.linkDAO.createLink(newLink);
					tm.commit();
				} catch (Exception e) {
					e.printStackTrace();
					tm.rollback();
				} finally {
					tm.close();
				}
			}
			offset += links.size();
			if (links.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateSpaces() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<User> users = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				UserDAO userDAO = DAOFactory.getInstance().getUserDAO(conn);
				users = userDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate User Spaces: " + users.size());
			for (User user : users) {
				System.out
						.println("Migrate User Space:  id = " + user.getId()
								+ ", newRootId = "
								+ this.userRootIds.get(user.getId()));
				this.migrateOneSpace(user.getId(),
						this.userRootIds.get(user.getId()));
			}

			offset += users.size();
			if (users.size() < LIST_LIMIT) {
				break;
			}
		}

		offset = 0;
		while (true) {
			Connection conn = null;
			List<Group> groups = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				GroupDAO groupDAO = DAOFactory.getInstance().getGroupDAO(conn);
				groups = groupDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Groups: " + groups.size());
			for (Group group : groups) {
				System.out.println("Migrate Group Space:  id = "
						+ group.getId() + ", newRootId = "
						+ this.groupRootIds.get(group.getId()));
				this.migrateOneSpace(group.getId(),
						this.groupRootIds.get(group.getId()));
			}

			offset += groups.size();
			if (groups.size() < LIST_LIMIT) {
				break;
			}
		}

		this.setSeqInitValue();
	}

	private void setSeqInitValue() throws Exception {
		TransactionManager tm = DAOManager.getInstance()
				.getTransactionManager();
		try {
			tm.start();
			this.cursorDAO.setIdInitValue();
			this.shareDAO.setIdInitValue();
			this.metaDAO.setVersionInitValue();
			tm.commit();
		} catch (Exception e) {
			tm.rollback();
			throw e;
		} finally {
			tm.close();
		}
	}

	private void migrateOneSpace(Long groupId, Long newRootId) throws Exception {
		this.metaIds.clear();
		this.migrateOneSapceMetas(groupId, newRootId);
		this.migrateOneSapceRevisions(groupId, newRootId);
		this.migrateOneSpaceTrashes(groupId, newRootId);
		this.mirgrateOneSpaceCursors(groupId, newRootId);
		this.mirgrateOneSpaceShares(groupId, newRootId);
	}

	private void mirgrateOneSpaceShares(Long groupId, Long newRootId)
			throws Exception {
		System.out.println("Migrate Shares:  id = " + groupId);
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Share> shares = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				ShareDAO shareDAO = DAOFactory.getInstance().getShareDAO(conn);
				shares = shareDAO.listByRoot(groupId, groupId, offset,
						LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Shares: " + shares.size());
			for (Share share : shares) {
				com.banmayun.server.migration.to.core.Share newShare = new com.banmayun.server.migration.to.core.Share();
				newShare.setCreatedAt(share.getCreated());
				newShare.setCreatedBy(this.userIds.get(share.getCreatedBy()));
				newShare.setExpiresAt(share.getExpires());
				newShare.setId(share.getId());
				newShare.setPasswordSha256(share.getPassword());
				newShare.setRootId(newRootId);

				conn = null;
				Meta meta = null;
				try {
					conn = DAOFactory.getInstance().getConnection();
					MetaDAO metaDAO = DAOFactory.getInstance().getMetaDAO(conn);
					meta = metaDAO.getByPath(groupId, groupId, share.getPath())
							.orNull();
					DbUtils.commitAndCloseQuietly(conn);
				} catch (SQLException e) {
					DbUtils.rollbackAndCloseQuietly(conn);
					throw e;
				}

				if (meta == null) {
					continue;
				}
				newShare.setMetaId(this.metaIds.get(meta.getFileId()));
				if (newShare.getMetaId() == null) {
					continue;
				}

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.shareDAO.createShare(newShare);
					tm.commit();
				} catch (Exception e) {
					tm.rollback();
					throw e;
				} finally {
					tm.close();
				}
			}

			offset += shares.size();
			if (shares.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void mirgrateOneSpaceCursors(Long groupId, Long newRootId)
			throws Exception {
		System.out.println("Migrate Cursors:  id = " + groupId);
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Cursor> cursors = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				CursorDAO cursorDAO = DAOFactory.getInstance().getCursorDAO(
						conn);
				cursors = cursorDAO.listByGroupId(groupId, offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Cursors: " + cursors.size());
			for (Cursor cursor : cursors) {
				com.banmayun.server.migration.to.core.Cursor newCursor = new com.banmayun.server.migration.to.core.Cursor();
				newCursor.setCreatedAt(cursor.getCreated());
				newCursor.setNextVersion(cursor.getNextVersion());
				newCursor.setId(cursor.getId());
				newCursor.setPos(cursor.getPos());
				newCursor.setPrev(cursor.getPrev());
				newCursor.setRootId(newRootId);
				newCursor.setVersion(cursor.getVersion());

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.cursorDAO.createCursor(newCursor);
					tm.commit();
				} catch (UniqueViolationException e) {
					tm.rollback();
				} catch (Exception e) {
					tm.rollback();
					throw e;
				} finally {
					tm.close();
				}
			}

			offset += cursors.size();
			if (cursors.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateOneSpaceTrashes(Long groupId, Long newRootId)
			throws Exception {
		System.out.println("Migrate Trashes:  id = " + groupId);
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Trash> trashes = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				TrashDAO trashDAO = DAOFactory.getInstance().getTrashDAO(conn);
				trashes = trashDAO.listByGroupId(groupId, offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Trashes: " + trashes.size());
			for (Trash trash : trashes) {
				com.banmayun.server.migration.to.core.Trash newTrash = new com.banmayun.server.migration.to.core.Trash();
				newTrash.setCreatedAt(trash.getCreated());
				newTrash.setCreatedBy(this.userIds.get(trash.getCreatedBy()));
				newTrash.setMetaId(this.metaIds.get(trash.getFileId()));
				newTrash.setRootId(newRootId);
				newTrash.setIsDeleted(trash.getIsDeleted());

				if (newTrash.getMetaId() == null) {
					continue;
				}

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.trashDAO.createTrash(newTrash);
					tm.commit();
				} catch (Exception e) {
					tm.rollback();
					throw e;
				} finally {
					tm.close();
				}
			}

			offset += trashes.size();
			if (trashes.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateOneSapceRevisions(Long groupId, Long newRootId)
			throws Exception {
		System.out.println("Migrate Revisons:  id = " + groupId);
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Revision> revisions = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				RevisionDAO revisionDAO = DAOFactory.getInstance()
						.getRevisionDAO(conn);
				revisions = revisionDAO.listByGroupId(groupId, offset,
						LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Revisions: " + revisions.size());
			for (Revision revision : revisions) {
				com.banmayun.server.migration.to.core.Revision newRevision = new com.banmayun.server.migration.to.core.Revision();
				newRevision.setBytes(revision.getBytes());
				newRevision.setClientModifiedAt(revision.getClientModified());
				newRevision.setMD5(revision.getMD5());
				newRevision.setMetaId(this.metaIds.get(revision.getFileId()));
				newRevision.setModifiedAt(revision.getModified());
				newRevision.setModifiedBy(this.userIds.get(revision
						.getModifiedBy()));
				newRevision.setRootId(newRootId);
				newRevision.setVersion(revision.getVersion());

				if (newRevision.getMetaId() == null) {
					continue;
				}

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.revisionDAO.createRevision(newRevision);
					tm.commit();
				} catch (Exception e) {
					tm.rollback();
					throw e;
				} finally {
					tm.close();
				}
			}

			offset += revisions.size();
			if (revisions.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateOneSapceMetas(Long groupId, Long newRootId)
			throws Exception {
		System.out.println("Migrate Metas:  id = " + groupId);
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<Meta> metas = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				MetaDAO metaDAO = DAOFactory.getInstance().getMetaDAO(conn);
				metas = metaDAO.listByGroup(groupId, groupId, offset,
						LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate Metas: " + metas.size());
			for (Meta meta : metas) {
				this.migrateOneMeta(newRootId, meta);
			}

			offset += metas.size();
			if (metas.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateOneMeta(Long newRootId, Meta meta) throws Exception {
		com.banmayun.server.migration.to.core.Meta newMeta = new com.banmayun.server.migration.to.core.Meta();

		newMeta.setRootId(newRootId);
		newMeta.setBytes(meta.getBytes());
		newMeta.setClientModifiedAt(meta.getClientModified());
		newMeta.setCreatedAt(meta.getCreated());
		newMeta.setCreatedBy(this.userIds.get(meta.getCreatedBy()));
		newMeta.setIsDir(meta.getIsDir());
		newMeta.setMD5(meta.getMD5());
		newMeta.setModifiedAt(meta.getModified());
		newMeta.setModifiedBy(this.userIds.get(meta.getModifiedBy()));
		newMeta.setName(meta.getName());
		newMeta.setNonce(meta.getNonce());
		newMeta.setParentPath(meta.getParentPath());
		newMeta.setPath(meta.getPath());
		newMeta.setVersion(meta.getVersion());

		if (!PathUtils.getTopLevelPath(meta.getPath()).equalsIgnoreCase(
				meta.getPath())) {
			newMeta.setPermission(null);
		} else {
			Connection conn = null;
			Permission perm = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				PermissionDAO permDAO = DAOFactory.getInstance()
						.getPermissionDAO(conn);
				perm = permDAO.getByPath(meta.getGroupId(), meta.getGroupId(),
						meta.getPath()).orNull();
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}
			if (perm == null) {
				newMeta.setPermission(null);
			} else {
				newMeta.setPermission(migratePermisson(perm));
			}
		}
		TransactionManager tm = DAOManager.getInstance()
				.getTransactionManager();
		try {
			tm.start();
			com.banmayun.server.migration.to.core.Meta createdMeta = this.metaDAO
					.createMeta(newMeta);
			this.metaIds.put(meta.getFileId(), createdMeta.getId());
			tm.commit();
		} catch (UniqueViolationException e) {
			tm.rollback();
		} catch (Exception e) {
			tm.rollback();
			throw e;
		} finally {
			tm.close();
		}
	}

	private String migratePermisson(Permission perm) {
		StringBuilder sb = new StringBuilder();
		Boolean[] fields = new Boolean[] { perm.getCanCreate(),
				perm.getCanOwnerRead(), perm.getCanOwnerWrite(),
				perm.getCanOwnerDelete(), perm.getCanCreate(),
				perm.getCanOthersRead(), perm.getCanOthersWrite(),
				perm.getCanOthersDelete() };
		for (int i = 0; i < fields.length; i++) {
			if (fields[i] != null && fields[i]) {
				sb.append(TRUE_CHARS[i]);
			} else {
				sb.append(FALSE_CHAR);
			}
		}
		return sb.toString();
	}

	public void migrateStatistic() throws Exception {
		migrateGroupStatistic();
		migrateSummaryStatistic();
	}

	private void migrateGroupStatistic() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<GroupStatistic> groupStatistics = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				GroupStatisticDAO groupStatisticDAO = DAOFactory.getInstance()
						.getGroupStatisticDAO(conn);
				groupStatistics = groupStatisticDAO.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate GroupStatistic: "
					+ groupStatistics.size());
			for (GroupStatistic gs : groupStatistics) {
				com.banmayun.server.migration.to.core.StatisticGroup newGs = new com.banmayun.server.migration.to.core.StatisticGroup();
				newGs.setGroupId(gs.getGroupId());
				newGs.setMetaCount(gs.getMetaCount());
				newGs.setBytes(gs.getBytes());
				newGs.setDate(gs.getDate());
				newGs.setFileCount(gs.getfileCount());
				newGs.setUserCount(gs.getUserCount());
				newGs.setIsPersonalSpace(gs.getIsPersonalSpace());

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.statisticGroupDAO.create(newGs);
					tm.commit();
				} catch (Exception e) {
					e.printStackTrace();
					tm.rollback();
				} finally {
					tm.close();
				}
			}
			offset += groupStatistics.size();
			if (groupStatistics.size() < LIST_LIMIT) {
				break;
			}
		}
	}

	private void migrateSummaryStatistic() throws Exception {
		int offset = 0;
		while (true) {
			Connection conn = null;
			List<SummaryStatistic> summaryStatistics = null;
			try {
				conn = DAOFactory.getInstance().getConnection();
				SummaryStatisticDAO summaryStatisticDAO = DAOFactory
						.getInstance().getSummaryStatisticDAO(conn);
				summaryStatistics = summaryStatisticDAO
						.list(offset, LIST_LIMIT);
				DbUtils.commitAndCloseQuietly(conn);
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				throw e;
			}

			System.out.println("Migrate SummaryStatistic: "
					+ summaryStatistics.size());
			for (SummaryStatistic ss : summaryStatistics) {
				com.banmayun.server.migration.to.core.StatisticSummary newSs = new com.banmayun.server.migration.to.core.StatisticSummary();
				newSs.setDate(ss.getDate());
				newSs.setBytes(ss.getBytes());
				newSs.setFileCount(ss.getfileCount());
				newSs.setExtensionCount(ss.getExtensionCount());
				newSs.setGroupCount(ss.getGroupCount());
				newSs.setMetaCount(ss.getMetaCount());
				newSs.setUserCount(ss.getUserCount());

				TransactionManager tm = DAOManager.getInstance()
						.getTransactionManager();
				try {
					tm.start();
					this.statisticSummaryDAO.create(newSs);
					tm.commit();
				} catch (Exception e) {
					e.printStackTrace();
					tm.rollback();
				} finally {
					tm.close();
				}
			}
			offset += summaryStatistics.size();
			if (summaryStatistics.size() < LIST_LIMIT) {
				break;
			}
		}
	}
}
