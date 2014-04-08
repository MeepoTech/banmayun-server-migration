package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Relation implements Cloneable {

    public enum RelationRole {
        MEMBER,
        ADMIN,
        OWNER
    }

    private Long userId = null;
    private Long groupId = null;
    private RelationRole role = null;
    private Boolean isActivated = null;
    private Boolean isBlocked = null;
    private String remarks = null;
    private Timestamp createdAt = null;

    public Long getUserId() {
        return this.userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getGroupId() {
        return this.groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public RelationRole getRole() {
        return this.role;
    }

    public void setRole(RelationRole role) {
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

    public String getRemarks() {
        return this.remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public Timestamp getCreatedAt() {
        return this.createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public Relation clone() {
        Relation relation = null;
        try {
            relation = (Relation) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return relation;
    }
}
