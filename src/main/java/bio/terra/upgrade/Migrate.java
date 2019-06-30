package bio.terra.upgrade;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.configuration.StairwayJdbcConfiguration;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Provides methods for upgrading the data repository metadata and stairway databases.
 * See <a href="https://docs.google.com/document/d/1CY9bOSwaw0HjdZ9uuxwm1rh4LkcOqV65tjI77IhKcxE/edit#">Liquibase
 * Migration Notes</a></a>
 */
@Component
public class Migrate {
    private Logger logger = LoggerFactory.getLogger("bio.terra.upgrade");
    private DataRepoJdbcConfiguration dataRepoJdbcConfiguration;
    private StairwayJdbcConfiguration stairwayJdbcConfiguration;
    private MigrateConfiguration migrateConfiguration;

    @Autowired
    public Migrate(DataRepoJdbcConfiguration dataRepoJdbcConfiguration,
                   StairwayJdbcConfiguration stairwayJdbcConfiguration,
                   MigrateConfiguration migrateConfiguration) {
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
        this.stairwayJdbcConfiguration = stairwayJdbcConfiguration;
        this.migrateConfiguration = migrateConfiguration;
    }

    public void migrateAllDatabases() {
        migrateDatabase(dataRepoJdbcConfiguration.getChangesetFile(), dataRepoJdbcConfiguration.getDataSource());
        migrateDatabase(stairwayJdbcConfiguration.getChangesetFile(), stairwayJdbcConfiguration.getDataSource());
    }

    private void migrateDatabase(String changesetFile, DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(changesetFile,
                    new ClassLoaderResourceAccessor(),
                    new JdbcConnection(connection));
            DatabaseChangeLogLock[] locks = liquibase.listLocks();
            for (DatabaseChangeLogLock lock : locks) {
                logger.info(String.format("dbChangeLogLock changeSet: %s, id: %s, lockedBy: %s, granted: %s",
                    changesetFile, lock.getId(), lock.getLockedBy(), lock.getLockGranted()));

                /**
                 * We can get into this state where one of the APIs is running migrations and gets shut down so that
                 * another API container can run. It will result in a lock that doesn't get released. This is similar
                 * to the problems we will have from deploying multiple containers at once that try to run migrations.
                 * If a lock has been held for more than a few minutes we should be able to assume that the container
                 * running the migration has failed.
                 */
                LocalDateTime now = LocalDateTime.now();
                LocalDateTime then = LocalDateTime.ofInstant(lock.getLockGranted().toInstant(), ZoneId.systemDefault());
                long durationMinutes = Duration.between(then, now).toMinutes();
                if (durationMinutes >= migrateConfiguration.getLockTimeoutMins()) {
                    logger.warn(String.format("forcing lock release (%s minutes since lock grant)", durationMinutes));
                    liquibase.forceReleaseLocks();
                }
            }
            logger.info(String.format("dropAllOnStart is set to %s", migrateConfiguration.getDropAllOnStart()));
            if (migrateConfiguration.getDropAllOnStart()) {
                liquibase.dropAll();
            }
            logger.info(String.format("updateAllOnStart is set to %s", migrateConfiguration.getUpdateAllOnStart()));
            if (migrateConfiguration.getUpdateAllOnStart()) {
                liquibase.update(new Contexts()); // Run all migrations - no context filtering
            }
        } catch (LiquibaseException | SQLException ex) {
            throw new MigrateException("Failed to migrate database from " + changesetFile, ex);
        }
    }
}
