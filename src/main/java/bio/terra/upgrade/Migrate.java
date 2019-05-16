package bio.terra.upgrade;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.configuration.StairwayJdbcConfiguration;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

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
