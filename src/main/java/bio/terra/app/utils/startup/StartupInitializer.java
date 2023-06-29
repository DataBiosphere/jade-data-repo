package bio.terra.app.utils.startup;

import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration.Synapse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public final class StartupInitializer {
  private static final Logger logger = LoggerFactory.getLogger(StartupInitializer.class);

  private StartupInitializer() {}

  public static void initialize(ApplicationContext applicationContext) {
    // Initialize jobService, stairway, migrate databases, perform recovery
    applicationContext.getBean(JobService.class);

    // Initialize the Synapse DB if needed
    Synapse config = applicationContext.getBean(AzureResourceConfiguration.class).getSynapse();
    if (config != null && config.isInitialize()) {
      logger.info("Initializing Synapse DB");
      applicationContext
          .getBean(AzureSynapsePdao.class)
          .initializeDb(
              config.getDatabaseName(),
              config.getEncryptionKey(),
              config.getParquetFileFormatName());
    } else {
      logger.info("Skipping Synapse DB initialization");
    }
  }
}
