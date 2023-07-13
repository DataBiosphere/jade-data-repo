package bio.terra.service.resourcemanagement.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateExportRuleStep extends AbstractCreateMonitoringResourceStep {

  private final AzureMonitoringService monitoringService;

  public CreateExportRuleStep(AzureMonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount = getStorageAccount(context);
    if (monitoringService.getDataExportRule(profileModel, storageAccount) != null) {
      return StepResult.getStepResultSuccess();
    }
    String exportRuleId = monitoringService.createDataExportRule(profileModel, storageAccount);
    // Only record if we created the data export rule as an indicator that we need to remove
    // it on failure
    workingMap.put(AzureMonitoringMapKeys.EXPORT_RULE_ID, exportRuleId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String exportRuleId = workingMap.get(AzureMonitoringMapKeys.EXPORT_RULE_ID, String.class);
    if (exportRuleId != null) {
      monitoringService.deleteDataExportRule(profileModel, exportRuleId);
    }
    return StepResult.getStepResultSuccess();
  }
}
