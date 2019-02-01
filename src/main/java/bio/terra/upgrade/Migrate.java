package bio.terra.upgrade;

import bio.terra.configuration.DataRepoJdbcConfiguration;
import bio.terra.configuration.StairwayJdbcConfiguration;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
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
    private DataRepoJdbcConfiguration dataRepoJdbcConfiguration;
    private StairwayJdbcConfiguration stairwayJdbcConfiguration;

    @Autowired
    public Migrate(DataRepoJdbcConfiguration dataRepoJdbcConfiguration,
                   StairwayJdbcConfiguration stairwayJdbcConfiguration) {
        this.dataRepoJdbcConfiguration = dataRepoJdbcConfiguration;
        this.stairwayJdbcConfiguration = stairwayJdbcConfiguration;
    }

    @PostConstruct
    public void migrateAllDatabases() {
        migrateDatabase(dataRepoJdbcConfiguration.getChangesetFile(), dataRepoJdbcConfiguration.getDataSource());
        migrateDatabase(stairwayJdbcConfiguration.getChangesetFile(), stairwayJdbcConfiguration.getDataSource());
    }

    private void migrateDatabase(String changesetFile, DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(changesetFile,
                    new FileSystemResourceAccessor(),
                    new JdbcConnection(connection));
            liquibase.update(new Contexts()); // Run all migrations - no context filtering
        } catch (LiquibaseException | SQLException ex) {
            throw new MigrateException("Failed to migrate database from " + changesetFile, ex);
        }
    }

    // Some modules require db migrations to run before they access the database. By having those modules receive their
    // JDBC configurations from this component, it will guarantee that migrations are run before they attempt to use
    // the configuration to connect to the database and start performing operations. This probably won't be a permanent
    // fix but it will help us along. See DR-127
    public StairwayJdbcConfiguration getStairwayJdbcConfiguration() {
        return stairwayJdbcConfiguration;
    }

    public DataRepoJdbcConfiguration getDataRepoJdbcConfiguration() {
        return dataRepoJdbcConfiguration;
    }
}
