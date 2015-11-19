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

package com.banmayun.server.migration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.db.DatabaseConfiguration;

public class MigrationConfiguration extends Configuration {

    @Valid
    @NotNull
    @JsonProperty("fromDatabase")
    private DatabaseConfiguration fromDatabase = new DatabaseConfiguration();

    @Valid
    @NotNull
    @JsonProperty("toDatabase")
    private DatabaseConfiguration toDatabase = new DatabaseConfiguration();

    @JsonProperty("fromRoot")
    private String fromRoot = null;

    @JsonProperty("toRoot")
    private String toRoot = null;

    @JsonProperty("migrateAccount")
    private boolean migrateAccount = false;

    @JsonProperty("migrateData")
    private boolean migrateData = false;

    @JsonProperty("groupDefaultPermission")
    private String groupDefaultPermission = "irwdirw-";

    public DatabaseConfiguration getFromDatabaseConfiguration() {
        return this.fromDatabase;
    }

    public DatabaseConfiguration getToDatabaseConfiguration() {
        return this.toDatabase;
    }

    public String getFromRoot() {
        return this.fromRoot;
    }

    public String getToRoot() {
        return this.toRoot;
    }

    public boolean getMigrateAccount() {
        return this.migrateAccount;
    }

    public boolean getMigrateData() {
        return this.migrateData;
    }

    public String getGroupDefaultPermission() {
        return this.groupDefaultPermission;
    }
}
