package bio.terra.service.resourcemanagement.flight;

import bio.terra.app.model.AzureRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateSentinelStep extends AbstractCreateMonitoringResourceStep {

  public CreateSentinelStep(AzureMonitoringService monitoringService, AzureRegion region) {
    super(monitoringService, region);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount = getStorageAccount(context);
    if (monitoringService.getSentinel(profileModel, storageAccount) != null) {
      return StepResult.getStepResultSuccess();
    }
    String sentinelId = monitoringService.createSentinel(profileModel, storageAccount);
    // Only record if we created the Sentinel deployment as an indicator that we need to remove
    // it on failure
    workingMap.put(AzureMonitoringMapKeys.SENTINEL_ID, sentinelId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String sentinelId = workingMap.get(AzureMonitoringMapKeys.SENTINEL_ID, String.class);
    if (sentinelId != null) {
      monitoringService.deleteSentinelById(profileModel, sentinelId);
    }
    return StepResult.getStepResultSuccess();
  }
}
