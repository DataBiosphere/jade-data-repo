package bio.terra.service.upgrade;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import bio.terra.common.migrate.LiquibaseMigrator;
import bio.terra.service.upgrade.exception.MigrateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Arrays;

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
    private final LiquibaseMigrator liquibaseMigrator;
    private final Environment env;

    @Autowired
    public Migrate(DataRepoJdbcConfiguration dataRepoJdbcConfiguration,
                   MigrateConfiguration migrateConfiguration,
                   LiquibaseMigrator liquibaseMigrator,
                   Environment env) {
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
        this.migrateConfiguration = migrateConfiguration;
        this.liquibaseMigrator = liquibaseMigrator;
        this.env = env;
    }

    /**
     * Run liquibase migration based on the configuration
     */
    public void migrateDatabase() {

        String changesetFile = dataRepoJdbcConfiguration.getChangesetFile();
        DataSource dataSource = dataRepoJdbcConfiguration.getDataSource();

        boolean allowDropAllOnStart = Arrays.stream(env.getActiveProfiles()).anyMatch(env -> env.contains("dev")
            || env.contains("test") || env.contains("int"));

        logger.info("dropAllOnStart={}; allowDropAllOnstart={}; updateAllOnStart={}",
            migrateConfiguration.getDropAllOnStart(), allowDropAllOnStart, migrateConfiguration.getUpdateAllOnStart());

        // Two booleans mean 4 cases
        // - dropAll=false; updateAll=false - do nothing
        // - dropAll=false; updateAll=true - update
        // - dropAll=true; updateAll=true OR =false - drop and update
        // The case of dropAll=true and updateAll=false makes no sense, since it would leave you
        // with an empty database, so we collapse the cases.
        try {
            if (allowDropAllOnStart && migrateConfiguration.getDropAllOnStart()) {
                liquibaseMigrator.initialize(changesetFile, dataSource);
            } else {
                if (migrateConfiguration.getUpdateAllOnStart()) {
                    liquibaseMigrator.upgrade(changesetFile, dataSource);
                }
            }
        } catch (bio.terra.common.migrate.MigrateException ex) {
            throw new MigrateException("Failed to migrate database from " + changesetFile, ex);
        }
    }

}
