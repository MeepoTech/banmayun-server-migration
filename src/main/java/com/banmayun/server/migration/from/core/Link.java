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

public class Link implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Device {
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

    public enum Category {
        ACCESS,
        ACTIVATION,
        PASSWORD_RESET
    }

    protected Long id = null;
    protected String name = null;
    protected Device device = null;
    protected String token = null;
    protected Category category = null;
    protected Long ownerId = null;
    protected Timestamp expires = null;
    protected Timestamp created = null;

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
        this.name = name;
    }

    public Device getDevice() {
        return this.device;
    }

    public void setDevice(Device device) {
        this.device = device;
    }

    public String getToken() {
        return this.token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Category getCategory() {
        return this.category;
    }

    public void setCategory(Category category) {
        this.category = category;
    }

    public Long getOwnerId() {
        return this.ownerId;
    }

    public void setOwnerId(Long ownerId) {
        this.ownerId = ownerId;
    }

    public Timestamp getExpires() {
        return this.expires;
    }

    public void setExpires(Timestamp expires) {
        this.expires = expires;
    }

    public Timestamp getCreated() {
        return this.created;
    }

    public void setCreated(Timestamp created) {
        this.created = created;
    }
}
