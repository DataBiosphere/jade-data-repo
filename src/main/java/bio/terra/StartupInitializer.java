package bio.terra;

import bio.terra.configuration.StairwayJdbcConfiguration;
import bio.terra.stairway.Stairway;
import bio.terra.upgrade.Migrate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.sql.DataSource;

public final class StartupInitializer {
    private static final Logger logger = LoggerFactory.getLogger("bio.terra.configuration.StartupInitializer");

    private StartupInitializer() {

    }

    public static void initialize(ApplicationContext applicationContext) {
        Migrate migrate = (Migrate)applicationContext.getBean("migrate");
        Stairway stairway = (Stairway)applicationContext.getBean("stairway");
        StairwayJdbcConfiguration stairwayJdbcConfiguration =
            (StairwayJdbcConfiguration)applicationContext.getBean("stairwayJdbcConfiguration");

        logger.info("Migrating all databases");
        migrate.migrateAllDatabases();

        logger.info("Initializing all databases");
        DataSource dataSource = stairwayJdbcConfiguration.getDataSource();
        stairway.initialize(dataSource, stairwayJdbcConfiguration.isForceClean());
    }
}
