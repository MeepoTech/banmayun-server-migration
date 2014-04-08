/* ****************************************************************************
 * MEEPOTECH CONFIDENTIAL
 * ----------------------
 * [2013] - [2014] MeePo Technology Incorporated
 * All Rights Reserved.
 *
 * IMPORTANT NOTICE:
 * All information contained herein is, and remains the property of MeePo
 * Technology Incorporated and its suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to MeePo Technology
 * Incorporated and its suppliers and may be covered by Chinese and Foreign
 * Patents, patents in process, and are protected by trade secret or copyright
 * law. Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained from
 * MeePo Technology Incorporated.
 * ****************************************************************************
 */

package com.banmayun.server.migration.from.core;

import java.io.Serializable;
import java.sql.Timestamp;

public class GroupStatistic implements Serializable {

    private static final long serialVersionUID = 1L;

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
