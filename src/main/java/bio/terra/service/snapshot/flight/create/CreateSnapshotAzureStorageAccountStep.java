package bio.terra.service.snapshot.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
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
import bio.terra.stairway.exception.RetryException;

public class CreateSnapshotAzureStorageAccountStep implements Step {

  private final DatasetService datasetService;
  private final ResourceService resourceService;

  public CreateSnapshotAzureStorageAccountStep(
      DatasetService datasetService, ResourceService resourceService) {
    this.datasetService = datasetService;
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    String flightId = context.getFlightId();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);

    // TODO - switch this to use new method provided in DR-2155
    AzureStorageAccountResource storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfile, flightId);
    workingMap.put(CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_INFO, storageAccountResource);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
