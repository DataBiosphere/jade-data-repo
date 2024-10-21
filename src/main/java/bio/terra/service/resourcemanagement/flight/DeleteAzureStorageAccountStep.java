package bio.terra.service.resourcemanagement.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteAzureStorageAccountStep extends DefaultUndoStep {
  private static final Logger logger = LoggerFactory.getLogger(DeleteAzureStorageAccountStep.class);
  private AzureStorageAccountService azureStorageAccountService;

  public DeleteAzureStorageAccountStep(AzureStorageAccountService azureStorageAccountService) {
    this.azureStorageAccountService = azureStorageAccountService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    List<AzureStorageAccountResource> storageAccounts =
        workingMap.get(
            ProfileMapKeys.PROFILE_UNIQUE_STORAGE_ACCOUNT_RESOURCE_LIST, new TypeReference<>() {});

    for (AzureStorageAccountResource storageAccountResource : storageAccounts) {
      if (azureStorageAccountService.getCloudStorageAccount(profileModel, storageAccountResource)
          != null) {
        logger.info(
            "Azure storage account {} found; Attempting delete.", storageAccountResource.getName());
        azureStorageAccountService.deleteCloudStorageAccount(profileModel, storageAccountResource);
      } else {
        logger.warn(
            "Azure Storage Account NOT FOUND for storage account {}; Skipping delete.",
            storageAccountResource.getName());
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
