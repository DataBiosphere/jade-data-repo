package bio.terra.service.resourcemanagement.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateLogAnalyticsWorkspaceStep extends AbstractCreateMonitoringResourceStep {

  private final AzureMonitoringService monitoringService;

  public CreateLogAnalyticsWorkspaceStep(AzureMonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount = getStorageAccount(context);
    if (monitoringService.getLogAnalyticsWorkspace(profileModel, storageAccount) != null) {
      return StepResult.getStepResultSuccess();
    }
    String logAnalyticsWorkspaceId =
        monitoringService.createLogAnalyticsWorkspace(profileModel, storageAccount);
    // Only record if we created the Log Analytics Workspace as an indicator that we need to remove
    // it on failure
    workingMap.put(AzureMonitoringMapKeys.LOG_ANALYTICS_WORKSPACE_ID, logAnalyticsWorkspaceId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String logAnalyticsWorkspaceId =
        workingMap.get(AzureMonitoringMapKeys.LOG_ANALYTICS_WORKSPACE_ID, String.class);
    if (logAnalyticsWorkspaceId != null) {
      monitoringService.deleteLogAnalyticsWorkspace(profileModel, logAnalyticsWorkspaceId);
    }
    return StepResult.getStepResultSuccess();
  }
}
