package bio.terra.app.utils.startup;

import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.stairway.FlightDao;
import bio.terra.stairway.Stairway;
import bio.terra.service.upgrade.Migrate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

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

        stairway.initialize(new FlightDao(stairwayJdbcConfiguration), stairwayJdbcConfiguration.isForceClean());
    }
}
