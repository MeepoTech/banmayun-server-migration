package com.banmayun.server.migration.to.core;

import java.sql.Timestamp;

public class StatisticSummary implements Cloneable {

    protected Timestamp date = null;
    protected Long groupCount = null;
    protected Long userCount = null;
    protected Long metaCount = null;
    protected Long fileCount = null;
    protected Long bytes = null;
    protected String extensionCount = null;

    public Timestamp getDate() {
      return this.date;
    }

    public void setDate(Timestamp date) {
      this.date = date;
    }

    public Long getGroupCount() {
      return this.groupCount;
    }

    public void setGroupCount(Long groupCount) {
      this.groupCount = groupCount;
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

    public String getExtensionCount() {
      return this.extensionCount;
    }

    public void setExtensionCount(String extensionCount) {
      this.extensionCount = extensionCount;
    }
    
    @Override
    public StatisticSummary clone() {
        StatisticSummary statistic = null;
        try {
            statistic = (StatisticSummary) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return statistic;
    }
  }