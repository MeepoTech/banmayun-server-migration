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

public class SummaryStatistic implements Serializable {

    private static final long serialVersionUID = 1L;

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
}
