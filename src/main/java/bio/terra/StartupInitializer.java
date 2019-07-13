package bio.terra;

import bio.terra.configuration.StairwayJdbcConfiguration;
import bio.terra.stairway.Stairway;
import bio.terra.upgrade.Migrate;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.sql.DataSource;
import java.io.IOException;

public final class StartupInitializer {
    private static final Logger logger = LoggerFactory.getLogger("bio.terra.configuration.StartupInitializer");

    private StartupInitializer() {

    }

    public static void initialize(ApplicationContext applicationContext) {
        Migrate migrate = (Migrate)applicationContext.getBean("migrate");
        Stairway stairway = (Stairway)applicationContext.getBean("stairway");
        StairwayJdbcConfiguration stairwayJdbcConfiguration =
            (StairwayJdbcConfiguration)applicationContext.getBean("stairwayJdbcConfiguration");

        try {
            String serviceAccountUser = GoogleCredential.getApplicationDefault().getServiceAccountUser();
            if (serviceAccountUser != null) {
                logger.info("Running under: {}", serviceAccountUser);
            } else {
                logger.info("Likely running under default user account");
            }
        } catch (IOException e) {
            logger.warn("Could not get Google credentials: {}", e.getMessage());
        }

        logger.info("Migrating all databases");
        migrate.migrateAllDatabases();

        logger.info("Initializing all databases");
        DataSource dataSource = stairwayJdbcConfiguration.getDataSource();
        stairway.initialize(dataSource, stairwayJdbcConfiguration.isForceClean());
    }
}
