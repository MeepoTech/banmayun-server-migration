package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class User implements Cloneable {

    public static final String DEFAULT_SOURCE = "";

    public enum UserRole {
        USER,
        ADMIN,
        ROOT
    }

    private Long id = null;
    private String name = null;
    private String email = null;
    private String passwordSha256 = null;
    private String source = null;
    private UserRole role = null;
    private Boolean isActivated = null;
    private Boolean isBlocked = null;
    private Integer groupsCanOwn = null;
    private String displayName = null;
    private Long rootId = null;
    private Timestamp createdAt = null;
    private Integer groupCount = null;

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

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPasswordSha256() {
        return this.passwordSha256;
    }

    public void setPasswordSha256(String passwordSha256) {
        this.passwordSha256 = passwordSha256;
    }

    public String getSource() {
        return this.source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public UserRole getRole() {
        return this.role;
    }

    public void setRole(UserRole role) {
        this.role = role;
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

    public Integer getGroupsCanOwn() {
        return this.groupsCanOwn;
    }

    public void setGroupsCanOwn(Integer groupsCanOwn) {
        this.groupsCanOwn = groupsCanOwn;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
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

    public Integer getGroupCount() {
        return this.groupCount;
    }

    public void setGroupCount(Integer groupCount) {
        this.groupCount = groupCount;
    }

    @Override
    public User clone() {
        User user = null;
        try {
            user = (User) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return user;
    }
}
