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

public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Role {
        BLOCKED,
        USER,
        ADMIN,
        ROOT
    }

    protected Long id = null;
    protected String name = null;
    protected String email = null;
    protected String fullName = null;
    protected String password = null;
    protected Role role = null;
    protected Integer groupsCanOwn = null;
    protected Timestamp created = null;
    protected String domain = null;

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = (name == null ? null : name.toLowerCase());
    }

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        this.email = (email == null ? null : email.toLowerCase());
    }

    public String getFullName() {
        return this.fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Role getRole() {
        return this.role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public Integer getGroupsCanOwn() {
        return this.groupsCanOwn;
    }

    public void setGroupsCanOwn(Integer groupsCanOwn) {
        this.groupsCanOwn = groupsCanOwn;
    }

    public Timestamp getCreated() {
        return this.created;
    }

    public void setCreated(Timestamp created) {
        this.created = created;
    }

    public String getDomain() {
        return this.domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
}
