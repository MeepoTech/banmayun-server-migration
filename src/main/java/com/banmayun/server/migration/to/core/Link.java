package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Link implements Cloneable {

    public enum LinkCategory {
        ACCESS,
        EMAIL_VERIFICATION,
        PASSWORD_RESET
    }

    public enum LinkDevice {
        PC_WINDOWS,
        PC_MACOSX,
        PC_LINUX,
        PHONE_IOS,
        PHONE_ANDROID,
        PAD_IOS,
        PAD_ANDROID,
        WEB,
        UNKNOWN
    }

    private Long id = null;
    private Long userId = null;
    private String token = null;
    private LinkCategory category = null;
    private String name = null;
    private LinkDevice device = null;
    private Timestamp expiresAt = null;
    private Timestamp createdAt = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getUserId() {
        return this.userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getToken() {
        return this.token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public LinkCategory getCategory() {
        return this.category;
    }

    public void setCategory(LinkCategory category) {
        this.category = category;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LinkDevice getDevice() {
        return this.device;
    }

    public void setDevice(LinkDevice device) {
        this.device = device;
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
    public Link clone() {
        Link link = null;
        try {
            link = (Link) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return link;
    }
}
