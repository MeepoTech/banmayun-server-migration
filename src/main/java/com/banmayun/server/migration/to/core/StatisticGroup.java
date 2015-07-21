package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class StatisticGroup implements Cloneable {

    protected Long groupId = null;
    protected Timestamp date = null;
    protected Long userCount = null;
    protected Long metaCount = null;
    protected Long fileCount = null;
    protected Long bytes = null;
    protected Boolean isPersonalSpace = null;

    public Long getGroupId() {
      return this.groupId;
    }

    public void setGroupId(Long groupId) {
      this.groupId = groupId;
    }

    public Timestamp getDate() {
      return this.date;
    }

    public void setDate(Timestamp date) {
      this.date = date;
    }

    public Long getUserCount() {
      return this.userCount;
    }

    public void setUserCount(Long userCount) {
      this.userCount = userCount;
    }

    public Long getMetaCount() {
      return this.metaCount;
    }

    public void setMetaCount(Long metaCount) {
      this.metaCount = metaCount;
    }

    public Long getfileCount() {
      return this.fileCount;
    }

    public void setFileCount(Long fileCount) {
      this.fileCount = fileCount;
    }

    public Long getBytes() {
      return this.bytes;
    }

    public void setBytes(Long bytes) {
      this.bytes = bytes;
    }

    public Boolean getIsPersonalSpace() {
      return this.isPersonalSpace;
    }

    public void setIsPersonalSpace(Boolean isPersonalSpace) {
      this.isPersonalSpace = isPersonalSpace;
    }
    
    @Override
    public StatisticGroup clone() {
        StatisticGroup statistic = null;
        try {
            statistic = (StatisticGroup) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return statistic;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("groupID:" + groupId + ", ");
      sb.append("date:" + date + ", ");
      sb.append("userCount:" + userCount + ", ");
      sb.append("metaCount:" + metaCount + ", ");
      sb.append("fileCount:" + fileCount + ", ");
      sb.append("bytes:" + bytes + ", ");
      sb.append("isPersonalSpace:" + isPersonalSpace);
      return sb.toString();
    }
  }
