package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Cursor implements Cloneable {

    private Long id = null;
    private Long rootId = null;
    private Long version = null;
    private Integer pos = null;
    private Long nextVersion = null;
    private Long prev = null;
    private Timestamp createdAt = null;

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

    public Long getVersion() {
        return this.version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Integer getPos() {
        return this.pos;
    }

    public void setPos(Integer pos) {
        this.pos = pos;
    }

    public Long getNextVersion() {
        return this.nextVersion;
    }

    public void setNextVersion(Long nextVersion) {
        this.nextVersion = nextVersion;
    }

    public Long getPrev() {
        return this.prev;
    }

    public void setPrev(Long prev) {
        this.prev = prev;
    }

    public Timestamp getCreatedAt() {
        return this.createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public Cursor clone() {
        Cursor cursor = null;
        try {
            cursor = (Cursor) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return cursor;
    }
}
