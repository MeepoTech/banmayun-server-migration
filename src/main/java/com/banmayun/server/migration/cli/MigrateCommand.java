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

package com.banmayun.server.migration.cli;

import net.sourceforge.argparse4j.inf.Namespace;

import com.banmayun.server.migration.Migration;
import com.banmayun.server.migration.MigrationConfiguration;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;

public class MigrateCommand extends ConfiguredCommand<MigrationConfiguration> {

    public MigrateCommand(String name, String description) {
        super(name, description);
    }

    @Override
    protected void run(Bootstrap<MigrationConfiguration> arg0, Namespace arg1, MigrationConfiguration config)
            throws Exception {
        // in shell, we MUST NOT do the index job
        Migration.configure(config);

        new Migrator(config).migrate();
    }
}
