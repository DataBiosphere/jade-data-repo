package bio.terra.service.resourcemanagement.flight;

import bio.terra.common.ErrorCollector;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.stairway.*;
import org.springframework.context.ApplicationContext;
import java.util.UUID;

public class AzureStorageAccountCleanupFlight extends Flight {

  public AzureStorageAccountCleanupFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    AzureMonitoringService monitoringService = appContext.getBean(AzureMonitoringService.class);
    AzureStorageAccountService azureStorageAccountService =
        appContext.getBean(AzureStorageAccountService.class);

    UUID subscriptionId =
        inputParameters.get(JobMapKeys.SUBSCRIPTION_ID.getKeyName(), UUID.class);
    String resourceGroupName =
        inputParameters.get(JobMapKeys.RESOURCE_GROUP_NAME.getKeyName(), String.class);
    String storageAccountName =
        inputParameters.get(JobMapKeys.STORAGE_ACCOUNT_NAME.getKeyName(), String.class);
    ErrorCollector errorCollector = new ErrorCollector(3, "StorageAccountCleanupFlight");

    //TODO - auth check - User must be admin in order to complete this request

    AzureStorageMonitoringStepProvider azureStorageMonitoringStepProvider =
        new AzureStorageMonitoringStepProvider(monitoringService);

    if (!azureStorageAccountService.storageAccountOrphaned(storageAccountName, resourceGroupName)) {
      errorCollector.record(
          String.format(
              "Attempting to delete storage account %s that is still linked to a TDR resource.",
              storageAccountName));
    } else {

      azureStorageMonitoringStepProvider
          .configureUndoSteps(subscriptionId, resourceGroupName, storageAccountName, errorCollector)
          .forEach(s -> this.addStep(s.step()));

      addStep(new DeleteCloudStorageAccountStep(azureStorageAccountService, subscriptionId, resourceGroupName, storageAccountName, errorCollector));
    }
  }

}
