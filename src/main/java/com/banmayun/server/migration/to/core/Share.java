package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Share implements Cloneable {

    private Long id = null;
    private Long rootId = null;
    private Long metaId = null;
    private String passwordSha256 = null;
    private Timestamp expiresAt = null;
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
        return metaId;
    }

    public void setMetaId(Long metaId) {
        this.metaId = metaId;
    }

    public String getPasswordSha256() {
        return this.passwordSha256;
    }

    public void setPasswordSha256(String passwordSha256) {
        this.passwordSha256 = passwordSha256;
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

    public Long getCreatedBy() {
        return this.createdBy;
    }

    public void setCreatedBy(Long createdBy) {
        this.createdBy = createdBy;
    }

    @Override
    public Share clone() {
        Share share = null;
        try {
            share = (Share) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return share;
    }
}
