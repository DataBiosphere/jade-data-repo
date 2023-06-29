package bio.terra.app.utils.startup;

import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public final class StartupInitializer {
  private static final Logger logger = LoggerFactory.getLogger(StartupInitializer.class);

  private StartupInitializer() {}

  public static void initialize(ApplicationContext applicationContext) {
    // Initialize jobService, stairway, migrate databases, perform recovery
    applicationContext.getBean(JobService.class);

    AzureResourceConfiguration config =
        applicationContext.getBean(AzureResourceConfiguration.class);
    // Initialize the Synapse DB if needed
    applicationContext
        .getBean(AzureSynapsePdao.class)
        .initializeDb(
            config.getSynapse().getDatabaseName(),
            config.getSynapse().getEncryptionKey(),
            config.getSynapse().getParquetFileFormatName());
  }
}
