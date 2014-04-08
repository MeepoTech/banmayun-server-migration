package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class ChunkedUpload implements Cloneable {

    private Long id = null;
    private String location = null;
    private Long pos = null;
    private Long bytes = null;
    private Timestamp expiresAt = null;
    private Timestamp createdAt = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Long getPos() {
        return this.pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }

    public Long getBytes() {
        return this.bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Timestamp getExpiresAt() {
        return this.expiresAt;
    }

    public void setExpiresAt(Timestamp expiresAt) {
        this.expiresAt = expiresAt;
    }

    public Timestamp getCreatedAt() {
        return this.createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public ChunkedUpload clone() {
        ChunkedUpload chunkedUpload = null;
        try {
            chunkedUpload = (ChunkedUpload) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return chunkedUpload;
    }
}
