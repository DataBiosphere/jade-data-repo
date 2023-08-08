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

public class CreateDiagnosticSettingStep extends AbstractCreateMonitoringResourceStep {

  public CreateDiagnosticSettingStep(AzureMonitoringService monitoringService, AzureRegion region) {
    super(monitoringService, region);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount = getStorageAccount(context);
    if (monitoringService.getDiagnosticSetting(profileModel, storageAccount) != null) {
      return StepResult.getStepResultSuccess();
    }
    String logAnalyticsWorkspaceId =
        workingMap.get(AzureMonitoringMapKeys.LOG_ANALYTICS_WORKSPACE_ID, String.class);
    String diagnosticSettingId =
        monitoringService.createDiagnosticSetting(
            profileModel, storageAccount, logAnalyticsWorkspaceId);
    // Only record if we created the Diagnostic Setting as an indicator that we need to remove
    // it on failure
    workingMap.put(AzureMonitoringMapKeys.DIAGNOSTIC_SETTING_ID, diagnosticSettingId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String diagnosticSettingId =
        workingMap.get(AzureMonitoringMapKeys.DIAGNOSTIC_SETTING_ID, String.class);
    if (diagnosticSettingId != null) {
      monitoringService.deleteDiagnosticSetting(profileModel, diagnosticSettingId);
    }
    return StepResult.getStepResultSuccess();
  }
}
