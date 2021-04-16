package bio.terra.service.upgrade;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.upgrade.exception.MigrateException;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides methods for upgrading the data repository metadata and stairway databases.
 * See <a href="https://docs.google.com/document/d/1CY9bOSwaw0HjdZ9uuxwm1rh4LkcOqV65tjI77IhKcxE/edit#">Liquibase
 * Migration Notes</a></a>
 * <p>
 * -- perform the migrate according to the configuration settings
 * We rely on JobService to call back in to:
 * -- update the deployment row, unlocking the row and releasing any waiting DRmanagers
 * That is because we need to hold the migration lock during Stairway migration.
 * <p>
 * This is vulnerable to failure: if this pod crashes still holding the deployment lock, we will be stuck and
 * have to clear it by hand.
 * <p>
 */
@Component
public class Migrate {
    private static final Logger logger = LoggerFactory.getLogger("bio.terra.service.upgrade");
    private final DataRepoJdbcConfiguration dataRepoJdbcConfiguration;
    private final MigrateConfiguration migrateConfiguration;
    private final GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    private Environment env;

    @Autowired
    public Migrate(DataRepoJdbcConfiguration dataRepoJdbcConfiguration,
                   MigrateConfiguration migrateConfiguration,
                   GoogleResourceConfiguration resourceConfiguration) {
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
        this.migrateConfiguration = migrateConfiguration;
        this.resourceConfiguration = resourceConfiguration;
    }

    /**
     * Run liquibase migration
     *
     */
    public void migrateDatabase(boolean dropAllOnStart) {
        String changesetFile = dataRepoJdbcConfiguration.getChangesetFile();
        try (Connection connection = dataRepoJdbcConfiguration.getDataSource().getConnection()) {
            Liquibase liquibase = new Liquibase(changesetFile,
                new ClassLoaderResourceAccessor(),
                new JdbcConnection(connection));

            logger.info(String.format("dropAllOnStart is set to %s", dropAllOnStart));
            if (dropAllOnStart) {
                logger.info("Dropping all db objects in the default schema");
                liquibase.dropAll(); // drops everything in the default schema. The migrate schema should be OK
            }
            logger.info(String.format("updateAllOnStart is set to %s", migrateConfiguration.getUpdateAllOnStart()));
            if (migrateConfiguration.getUpdateAllOnStart()) {
                liquibase.update(new Contexts()); // Run all migrations - no context filtering
            }
        } catch (LiquibaseException | SQLException ex) {
            throw new MigrateException("Failed to migrate database from " + changesetFile, ex);
        }
    }

    /**
     * Helper function to block "drop all on start" from happening on undesired databases
     */
    public boolean allowDropAllOnStart() {
        boolean allowedProfile = Arrays.stream(env.getActiveProfiles()).anyMatch(env -> env.contains("dev")
            || env.contains("test") || env.contains("int"));

        boolean noDeleteDataProject = migrateConfiguration.getDataProjectNoDropAll().contains(
            resourceConfiguration.getSingleDataProjectId());
        return allowedProfile && !noDeleteDataProject;
    }

}
