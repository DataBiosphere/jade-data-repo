package bio.terra.app.utils.startup;

import bio.terra.service.job.JobService;
import bio.terra.service.upgrade.Migrate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public final class StartupInitializer {
    private static final Logger logger = LoggerFactory.getLogger(StartupInitializer.class);

    private StartupInitializer() {

    }

    public static void initialize(ApplicationContext applicationContext) {
        Migrate migrate = (Migrate)applicationContext.getBean("migrate");
        JobService jobService = (JobService)applicationContext.getBean("jobService");

        logger.info("Migrating all databases");
        migrate.migrateAllDatabases();

        // Initialize jobService, and by extension perform stairway initialization and recovery
        jobService.initialize();
    }
}
