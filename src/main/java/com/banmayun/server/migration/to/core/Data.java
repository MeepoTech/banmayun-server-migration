package com.banmayun.server.migration.to.core;

public class Data implements Cloneable {

    private String md5 = null;
    private Long bytes = null;
    private String location = null;
    private Integer refCount = null;

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

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Integer getRefCount() {
        return this.refCount;
    }

    public void setRefCount(Integer refCount) {
        this.refCount = refCount;
    }

    @Override
    public Data clone() {
        Data data = null;
        try {
            data = (Data) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return data;
    }
}
