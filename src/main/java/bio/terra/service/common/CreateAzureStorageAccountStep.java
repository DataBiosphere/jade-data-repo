package bio.terra.service.common;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public abstract class CreateAzureStorageAccountStep implements Step {

  private final DatasetService datasetService;
  private final ResourceService resourceService;

  public CreateAzureStorageAccountStep(
      DatasetService datasetService, ResourceService resourceService) {
    this.datasetService = datasetService;
    this.resourceService = resourceService;
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }

  protected void getOrCreateDatasetStorageAccount(FlightContext context)
      throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String flightId = context.getFlightId();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    AzureStorageAccountResource storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfile, flightId);
    workingMap.put(CommonMapKeys.DATASET_STORAGE_ACCOUNT_INFO, storageAccountResource);
  }

  protected void getOrCreateSnapshotStorageAccount(FlightContext context)
      throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String flightId = context.getFlightId();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    // TODO - replace w/ snapshot code
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    AzureStorageAccountResource storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfile, flightId);
    workingMap.put(CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_INFO, storageAccountResource);
  }
}
