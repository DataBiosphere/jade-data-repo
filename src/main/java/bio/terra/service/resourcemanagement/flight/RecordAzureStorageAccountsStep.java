package bio.terra.service.resourcemanagement.flight;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
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

public class RecordAzureStorageAccountsStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(RecordAzureStorageAccountsStep.class);
  AzureStorageAccountService azureStorageAccountService;

  public RecordAzureStorageAccountsStep(AzureStorageAccountService azureStorageAccountService) {
    this.azureStorageAccountService = azureStorageAccountService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> appIdList =
        workingMap.get(ProfileMapKeys.PROFILE_APPLICATION_DEPLOYMENT_ID_LIST, List.class);

    // Note: This will list a storage account for each top level container
    // So, there may be duplicates of the cloud storage account resource
    // Top level container and storage accounts have a many-to-one relationship
    List<AzureStorageAccountResource> storageAccounts =
        azureStorageAccountService.listStorageAccountPerAppDeployment(appIdList, true);
    workingMap.put(ProfileMapKeys.PROFILE_STORAGE_ACCOUNT_RESOURCE_LIST, storageAccounts);
    if (storageAccounts.isEmpty()) {
      logger.warn("No storage accounts found to be deleted for this billing profile.");
    } else {
      logger.info(
          "Found {} storage accounts to be deleted for this billing profile.",
          storageAccounts.size());
    }
    return StepResult.getStepResultSuccess();
  }
}
