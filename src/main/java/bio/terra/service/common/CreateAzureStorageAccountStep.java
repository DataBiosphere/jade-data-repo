package bio.terra.service.common;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.function.Predicate;

public abstract class CreateAzureStorageAccountStep extends OptionalStep {

  private final ResourceService resourceService;
  private final Dataset dataset;

  public CreateAzureStorageAccountStep(
      ResourceService resourceService, Dataset dataset, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  public CreateAzureStorageAccountStep(ResourceService resourceService, Dataset dataset) {
    this(resourceService, dataset, OptionalStep::alwaysDo);
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

    AzureStorageAccountResource storageAccountResource =
        resourceService.getOrCreateDatasetStorageAccount(dataset, billingProfile, flightId);
    workingMap.put(CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, storageAccountResource);

    AzureStorageAuthInfo storageAuthInfo =
        AzureStorageAuthInfo.azureStorageAuthInfoBuilder(billingProfile, storageAccountResource);
    workingMap.put(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, storageAuthInfo);
  }
}
