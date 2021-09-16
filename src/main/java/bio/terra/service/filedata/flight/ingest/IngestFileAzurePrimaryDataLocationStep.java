package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFileAzurePrimaryDataLocationStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(IngestFileAzurePrimaryDataLocationStep.class);
  private final ResourceService resourceService;
  private final Dataset dataset;

  public IngestFileAzurePrimaryDataLocationStep(ResourceService resourceService, Dataset dataset) {
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      // Retrieve the already authorized billing profile from the working map and retrieve
      // or create a storage account in the context of that profile and the dataset.
      BillingProfileModel billingProfile =
          workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

      try {
        AzureStorageAccountResource storageAccountResource =
            resourceService.getOrCreateStorageAccount(
                dataset, billingProfile, context.getFlightId());
        workingMap.put(FileMapKeys.STORAGE_ACCOUNT_INFO, storageAccountResource);
        AzureStorageAuthInfo storageAuthInfo =
            new AzureStorageAuthInfo()
                .subscriptionId(billingProfile.getSubscriptionId())
                .resourceGroupName(
                    storageAccountResource.getApplicationResource().getAzureResourceGroupName())
                .storageAccountResourceName(storageAccountResource.getName());
        workingMap.put(FileMapKeys.STORAGE_AUTH_INFO, storageAuthInfo);

      } catch (BucketLockException blEx) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // There is not much to undo here. It is possible that a storage account was created in the last
    // step. We could look to see if there are no other files in the storage account and delete it
    // here, but I think it is likely the storage account will be used again.
    return StepResult.getStepResultSuccess();
  }
}
