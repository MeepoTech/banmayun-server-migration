package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Revision implements Cloneable {

    private Long metaId = null;
    private Long version = null;
    private Long rootId = null;
    private String md5 = null;
    private Long bytes = null;
    private Timestamp modifiedAt = null;
    private Long modifiedBy = null;
    private Timestamp clientModifiedAt = null;

    public Long getMetaId() {
        return this.metaId;
    }

    public void setMetaId(Long metaId) {
        this.metaId = metaId;
    }

    public Long getVersion() {
        return this.version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Long getRootId() {
        return this.rootId;
    }

    public void setRootId(Long rootId) {
        this.rootId = rootId;
    }

    public String getMD5() {
        return this.md5;
    }

    public void setMD5(String md5) {
        this.md5 = md5;
    }

    public Long getBytes() {
        return this.bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Timestamp getModifiedAt() {
        return this.modifiedAt;
    }

    public void setModifiedAt(Timestamp modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public Long getModifiedBy() {
        return this.modifiedBy;
    }

    public void setModifiedBy(Long modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public Timestamp getClientModifiedAt() {
        return this.clientModifiedAt;
    }

    public void setClientModifiedAt(Timestamp clientModifiedAt) {
        this.clientModifiedAt = clientModifiedAt;
    }

    @Override
    public Revision clone() {
        Revision revision = null;
        try {
            revision = (Revision) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return revision;
    }
}
