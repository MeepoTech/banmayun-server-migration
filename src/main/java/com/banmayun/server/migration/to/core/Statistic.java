package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class Statistic implements Cloneable {

    public enum StatisticType {
        USER_COUNT,
        GROUP_COUNT,
        LINK_COUNT,
        FILE_COUNT,
        BYTE_COUNT
    }

    private StatisticType type = null;
    private Integer intValue = null;
    private Long longValue = null;
    private String stringValue = null;
    private Timestamp createdAt = null;

    public StatisticType getType() {
        return this.type;
    }

    public void setType(StatisticType type) {
        this.type = type;
    }

    public Integer getIntValue() {
        return this.intValue;
    }

    public void setIntValue(Integer intValue) {
        this.intValue = intValue;
    }

    public Long getLongValue() {
        return this.longValue;
    }

    public void setLongValue(Long longValue) {
        this.longValue = longValue;
    }

    public String getStringValue() {
        return this.stringValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public Timestamp getCreatedAt() {
        return this.createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public Statistic clone() {
        Statistic statistic = null;
        try {
            statistic = (Statistic) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return statistic;
    }
}
