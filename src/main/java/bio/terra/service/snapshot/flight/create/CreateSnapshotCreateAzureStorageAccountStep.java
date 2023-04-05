package bio.terra.service.snapshot.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.common.CreateAzureStorageAccountStep;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class CreateSnapshotCreateAzureStorageAccountStep extends CreateAzureStorageAccountStep {
  private final ResourceService resourceService;
  private final Dataset dataset;

  public CreateSnapshotCreateAzureStorageAccountStep(
      ResourceService resourceService, Dataset dataset) {
    super(resourceService, dataset);
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    getOrCreateDatasetStorageAccount(context);
    createSnapshotStorageAccount(context);
    return StepResult.getStepResultSuccess();
  }

  private void createSnapshotStorageAccount(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String flightId = context.getFlightId();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    AzureStorageAccountResource storageAccountResource =
        resourceService.createSnapshotStorageAccount(
            snapshotId, dataset.getStorageAccountRegion(), billingProfile, flightId);
    workingMap.put(CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_RESOURCE, storageAccountResource);

    AzureStorageAuthInfo storageAuthInfo =
        AzureStorageAuthInfo.azureStorageAuthInfoBuilder(billingProfile, storageAccountResource);
    workingMap.put(CommonMapKeys.SNAPSHOT_STORAGE_AUTH_INFO, storageAuthInfo);
  }
}
