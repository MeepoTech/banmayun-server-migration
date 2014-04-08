package com.banmayun.server.migration.to.core;

public class Root implements Cloneable {

    public enum RootType {
        USER,
        GROUP
    }

    private Long id = null;
    private RootType type = null;
    private Long quota = null;
    private String defaultPermission = null;
    private Integer fileCount = null;
    private Long byteCount = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public RootType getType() {
        return this.type;
    }

    public void setType(RootType type) {
        this.type = type;
    }

    public Long getQuota() {
        return this.quota;
    }

    public void setQuota(Long quota) {
        this.quota = quota;
    }

    public String getDefaultPermission() {
        return this.defaultPermission;
    }

    public void setDefaultPermission(String defaultPermission) {
        this.defaultPermission = defaultPermission;
    }

    public Integer getFileCount() {
        return this.fileCount;
    }

    public void setFileCount(Integer fileCount) {
        this.fileCount = fileCount;
    }

    public Long getByteCount() {
        return this.byteCount;
    }

    public void setByteCount(Long byteCount) {
        this.byteCount = byteCount;
    }

    @Override
    public Root clone() {
        Root root = null;
        try {
            root = (Root) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return root;
    }
}
