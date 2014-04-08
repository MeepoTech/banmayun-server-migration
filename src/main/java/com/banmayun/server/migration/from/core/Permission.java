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

public class Permission implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final char[] TRUE_CHARS = new char[] { 'c', 'r', 'w', 'd', 'r', 'w', 'd' };
    public static final char FALSE_CHAR = '-';

    protected Long groupId = null;
    protected Long rootId = null;
    protected String path = null;
    protected Boolean canCreate = null;
    protected Boolean canOwnerRead = null;
    protected Boolean canOwnerWrite = null;
    protected Boolean canOwnerDelete = null;
    protected Boolean canOthersRead = null;
    protected Boolean canOthersWrite = null;
    protected Boolean canOthersDelete = null;

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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Boolean getCanCreate() {
        return this.canCreate;
    }

    public void setCanCreate(Boolean canCreate) {
        this.canCreate = canCreate;
    }

    public Boolean getCanOwnerRead() {
        return this.canOwnerRead;
    }

    public void setCanOwnerRead(Boolean canOwnerRead) {
        this.canOwnerRead = canOwnerRead;
    }

    public Boolean getCanOwnerWrite() {
        return this.canOwnerWrite;
    }

    public void setCanOwnerWrite(Boolean canOwnerWrite) {
        this.canOwnerWrite = canOwnerWrite;
    }

    public Boolean getCanOwnerDelete() {
        return this.canOwnerDelete;
    }

    public void setCanOwnerDelete(Boolean canOwnerDelete) {
        this.canOwnerDelete = canOwnerDelete;
    }

    public Boolean getCanOthersRead() {
        return this.canOthersRead;
    }

    public void setCanOthersRead(Boolean canOthersRead) {
        this.canOthersRead = canOthersRead;
    }

    public Boolean getCanOthersWrite() {
        return this.canOthersWrite;
    }

    public void setCanOthersWrite(Boolean canOthersWrite) {
        this.canOthersWrite = canOthersWrite;
    }

    public Boolean getCanOthersDelete() {
        return this.canOthersDelete;
    }

    public void setCanOthersDelete(Boolean canOthersDelete) {
        this.canOthersDelete = canOthersDelete;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Boolean[] fields = new Boolean[] { this.canCreate, this.canOwnerRead, this.canOwnerWrite, this.canOwnerDelete,
                this.canOthersRead, this.canOthersWrite, this.canOthersDelete };
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] != null && fields[i]) {
                sb.append(TRUE_CHARS[i]);
            } else {
                sb.append(FALSE_CHAR);
            }
        }
        return sb.toString();
    }

    @Override
    public Permission clone() {
        Permission that = new Permission();
        that.canCreate = this.canCreate;
        that.canOwnerRead = this.canOwnerRead;
        that.canOwnerWrite = this.canOwnerWrite;
        that.canOwnerDelete = this.canOwnerDelete;
        that.canOthersRead = this.canOthersRead;
        that.canOthersWrite = this.canOthersWrite;
        that.canOthersDelete = this.canOthersDelete;
        return that;
    }

    public static Permission parsePermission(String arg) {
        if (arg.length() != TRUE_CHARS.length) {
            throw new IllegalArgumentException("invalid arg, must be a string of length " + TRUE_CHARS.length);
        }

        Permission perm = new Permission();

        for (int i = 0; i < TRUE_CHARS.length; i++) {
            boolean val;
            if (arg.charAt(i) == TRUE_CHARS[i]) {
                val = true;
            } else if (arg.charAt(i) == FALSE_CHAR) {
                val = false;
            } else {
                throw new IllegalArgumentException("invalid arg, unknown char at pos " + i);
            }
            switch (i) {
            case 0:
                perm.canCreate = val;
                break;
            case 1:
                perm.canOwnerRead = val;
                break;
            case 2:
                perm.canOwnerWrite = val;
                break;
            case 3:
                perm.canOwnerDelete = val;
                break;
            case 4:
                perm.canOthersRead = val;
                break;
            case 5:
                perm.canOthersWrite = val;
                break;
            case 6:
                perm.canOthersDelete = val;
                break;
            }
        }

        return perm;
    }
}
