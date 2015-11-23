package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Group implements Cloneable {

    public static final String DEFAULT_SOURCE = "";

    public enum GroupType {
        SYSTEM_PUBLIC,
        PUBLIC,
        PROTECTED,
        PRIVATE
    }

    private Long id = null;
    private String name = null;
    private String source = null;
    private String intro = null;
    private String tags = null;
    private GroupType type = null;
    private Boolean isVisible = null;
    private Boolean isActivated = null;
    private Boolean isBlocked = null;
    private String announce = null;
    private Long rootId = null;
    private Timestamp createdAt = null;
    private Long createdBy = null;
    private Integer userCount = null;
    private Boolean isPromoted = null;
    private Boolean isDeleted = null;
    private Integer membersCanOwn = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSource() {
        return this.source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getIntro() {
        return this.intro;
    }

    public void setIntro(String intro) {
        this.intro = intro;
    }

    public String getTags() {
        return this.tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public GroupType getType() {
        return this.type;
    }

    public void setType(GroupType type) {
        this.type = type;
    }

    public Boolean getIsVisible() {
        return this.isVisible;
    }

    public void setIsVisible(Boolean isVisible) {
        this.isVisible = isVisible;
    }

    public Boolean getIsActivated() {
        return this.isActivated;
    }

    public void setIsActivated(Boolean isActivated) {
        this.isActivated = isActivated;
    }

    public Boolean getIsBlocked() {
        return this.isBlocked;
    }

    public void setIsBlocked(Boolean isBlocked) {
        this.isBlocked = isBlocked;
    }

    public String getAnnounce() {
        return this.announce;
    }

    public void setAnnounce(String announce) {
        this.announce = announce;
    }

    public Long getRootId() {
        return this.rootId;
    }

    public void setRootId(Long rootId) {
        this.rootId = rootId;
    }

    public Timestamp getCreatedAt() {
        return this.createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    public Long getCreatedBy() {
        return this.createdBy;
    }

    public void setCreatedBy(Long createdBy) {
        this.createdBy = createdBy;
    }

    public Integer getUserCount() {
        return this.userCount;
    }

    public void setUserCount(Integer userCount) {
        this.userCount = userCount;
    }
    
    public Boolean getIsPromoted() {
        return this.isPromoted;
    }

    public void setIsPromoted(Boolean isPromoted) {
        this.isPromoted = isPromoted;
    }
    
    public Boolean getIsDeleted() {
        return this.isDeleted;
    }

    public void setIsDeleted(Boolean isDeleted) {
        this.isDeleted = isDeleted;
    }
    
    public Integer getMembersCanOwn() {
    	return this.membersCanOwn;
    }
    
    public void setMembersCanOwn(Integer membersCanOwn) {
    	this.membersCanOwn = membersCanOwn;
    }

    @Override
    public Group clone() {
        Group group = null;
        try {
            group = (Group) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return group;
    }
}
