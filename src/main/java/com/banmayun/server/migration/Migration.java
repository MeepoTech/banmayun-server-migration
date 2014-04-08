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

import com.banmayun.server.migration.cli.MigrateCommand;
import com.banmayun.server.migration.from.db.DAOFactory;
import com.banmayun.server.migration.to.db.impl.DAOManager;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.db.DatabaseConfiguration;
import com.yammer.dropwizard.migrations.MigrationsBundle;

public class Migration extends Service<MigrationConfiguration> {

    public static void main(String[] args) throws Exception {
        new Migration().run(args);
    }

    private final MigrationsBundle<MigrationConfiguration> migration = new MigrationsBundle<MigrationConfiguration>() {
        @Override
        public DatabaseConfiguration getDatabaseConfiguration(MigrationConfiguration configuration) {
            return configuration.getToDatabaseConfiguration();
        }
    };

    public static void configure(MigrationConfiguration config) throws Exception {
        // configure database
        DatabaseConfiguration dbConfig = config.getFromDatabaseConfiguration();
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(dbConfig.getDriverClass());
        cpds.setJdbcUrl(dbConfig.getUrl());
        cpds.setUser(dbConfig.getUser());
        cpds.setPassword(dbConfig.getPassword());
        cpds.setMaxPoolSize(dbConfig.getMaxSize());
        cpds.setMinPoolSize(dbConfig.getMinSize());
        cpds.setMaxIdleTime((int) dbConfig.getCloseConnectionIfIdleFor().toSeconds());
        cpds.setCheckoutTimeout((int) dbConfig.getMaxWaitForConnection().toMilliseconds());

        DAOFactory.initializeInstance(cpds);

        dbConfig = config.getToDatabaseConfiguration();
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(dbConfig.getDriverClass());
        cpds.setJdbcUrl(dbConfig.getUrl());
        cpds.setUser(dbConfig.getUser());
        cpds.setPassword(dbConfig.getPassword());
        cpds.setMaxPoolSize(dbConfig.getMaxSize());
        cpds.setMinPoolSize(dbConfig.getMinSize());
        cpds.setMaxIdleTime((int) dbConfig.getCloseConnectionIfIdleFor().toSeconds());
        cpds.setCheckoutTimeout((int) dbConfig.getMaxWaitForConnection().toMilliseconds());

        DAOManager.initializeInstance(cpds);

    }

    @Override
    public void initialize(Bootstrap<MigrationConfiguration> bootstrap) {
        bootstrap.addBundle(this.migration);
        bootstrap.addCommand(new MigrateCommand("migrate", "migrate"));
    }

    @Override
    public void run(MigrationConfiguration config, Environment environment) throws Exception {
        configure(config);
    }
}
