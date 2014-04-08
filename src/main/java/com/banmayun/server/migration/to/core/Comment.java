package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Comment implements Cloneable {

    private Long id = null;
    private Long rootId = null;
    private Long metaId = null;
    private String contents = null;
    private Timestamp createdAt = null;
    private Long createdBy = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getRootId() {
        return this.rootId;
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

    public String getContents() {
        return this.contents;
    }

    public void setContents(String contents) {
        this.contents = contents;
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
    public Comment clone() {
        Comment comment = null;
        try {
            comment = (Comment) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return comment;
    }
}
