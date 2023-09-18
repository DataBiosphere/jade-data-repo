package bio.terra.service.resourcemanagement.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSentinelStep extends DefaultUndoStep {
  private static final Logger logger = LoggerFactory.getLogger(DeleteSentinelStep.class);
  private AzureStorageAccountService azureStorageAccountService;
  private AzureMonitoringService monitoringService;

  public DeleteSentinelStep(
      AzureMonitoringService monitoringService,
      AzureStorageAccountService azureStorageAccountService) {
    this.monitoringService = monitoringService;
    this.azureStorageAccountService = azureStorageAccountService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> appIdList =
        workingMap.get(ProfileMapKeys.PROFILE_APPLICATION_DEPLOYMENT_ID_LIST, List.class);
    List<AzureStorageAccountResource> storageAccounts =
        azureStorageAccountService.listStorageAccountIdsPerAppDeployment(appIdList, true);
    workingMap.put(ProfileMapKeys.PROFILE_STORAGE_ACCOUNT_RESOURCE_LIST, storageAccounts);

    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    for (AzureStorageAccountResource storageAccountResource : storageAccounts) {
      var sentinel = monitoringService.getSentinel(profileModel, storageAccountResource);
      if (sentinel != null) {
        logger.info(
            "Sentinel instance found for storage account {}; Attempting delete.",
            storageAccountResource.getName());
        // TODO - how should we do error handling?
        monitoringService.deleteSentinelByStorageAccount(profileModel, storageAccountResource);
      } else {
        logger.warn(
            "Sentinel instance NOT FOUND for storage account {}; Skipping delete.",
            storageAccountResource.getName());
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
