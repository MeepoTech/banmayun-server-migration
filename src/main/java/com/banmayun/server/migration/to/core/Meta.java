package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Meta implements Cloneable {

    public static final long DEFAULT_NONCE = 0L;
    public static final long INITIAL_VERSION = 0L;

    private Long id = null;
    private Long version = null;
    private Long rootId = null;
    private String path = null;
    private Long nonce = null;
    private String parentPath = null;
    private String name = null;
    private Boolean isDir = null;
    private String md5 = null;
    private Long bytes = null;
    private String permission = null;
    private Timestamp createdAt = null;
    private Long createdBy = null;
    private Timestamp modifiedAt = null;
    private Long modifiedBy = null;
    private Timestamp clientModifiedAt = null;
    private Integer commentCount = null;
    private Integer shareCount = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public String getPath() {
        return this.path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getNonce() {
        return this.nonce;
    }

    public void setNonce(Long nonce) {
        this.nonce = nonce;
    }

    public String getParentPath() {
        return this.parentPath;
    }

    public void setParentPath(String parentPath) {
        this.parentPath = parentPath;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getIsDir() {
        return this.isDir;
    }

    public void setIsDir(Boolean isDir) {
        this.isDir = isDir;
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

    public String getPermission() {
        return this.permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
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

    public Integer getCommentCount() {
        return this.commentCount;
    }

    public void setCommentCount(Integer commentCount) {
        this.commentCount = commentCount;
    }

    public Integer getShareCount() {
        return this.shareCount;
    }

    public void setShareCount(Integer shareCount) {
        this.shareCount = shareCount;
    }

    @Override
    public Meta clone() {
        Meta meta = null;
        try {
            meta = (Meta) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return meta;
    }
}
