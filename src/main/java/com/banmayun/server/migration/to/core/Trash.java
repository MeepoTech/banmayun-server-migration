package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Trash implements Cloneable {

    private Long id = null;
    private Long rootId = null;
    private Long metaId = null;
    private Boolean isDeleted = null;
    private Timestamp createdAt = null;
    private Long createdBy = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getRootId() {
        return rootId;
    }

    public void setRootId(Long rootId) {
        this.rootId = rootId;
    }

    public Long getMetaId() {
        return this.metaId;
    }

    public void setMetaId(Long metaId) {
        this.metaId = metaId;
    }

    public Boolean getIsDeleted() {
        return this.isDeleted;
    }

    public void setIsDeleted(Boolean isDeleted) {
        this.isDeleted = isDeleted;
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

    @Override
    public Trash clone() {
        Trash trash = null;
        try {
            trash = (Trash) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return trash;
    }
}
