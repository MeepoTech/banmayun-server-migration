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

public class Cursor implements Serializable {

    private static final long serialVersionUID = 1L;

    protected Long id = null;
    protected Long groupId = null;
    protected Long rootId = null;
    protected Long version = null;
    protected Integer pos = null;
    protected Long nextVersion = null;
    protected Long prev = null;
    protected Timestamp created = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getGroupId() {
        return this.groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public Long getRootId() {
        return this.rootId;
    }

    public void setRootId(Long rootId) {
        this.rootId = rootId;
    }

    public Long getVersion() {
        return this.version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Integer getPos() {
        return this.pos;
    }

    public void setPos(Integer pos) {
        this.pos = pos;
    }

    public Long getNextVersion() {
        return this.nextVersion;
    }

    public void setNextVersion(Long nextVersion) {
        this.nextVersion = nextVersion;
    }

    public Long getPrev() {
        return this.prev;
    }

    public void setPrev(Long prev) {
        this.prev = prev;
    }

    public Timestamp getCreated() {
        return this.created;
    }

    public void setCreated(Timestamp created) {
        this.created = created;
    }
}
